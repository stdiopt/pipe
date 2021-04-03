package pipe_test

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stdiopt/pipe"
)

func TestSimple(t *testing.T) {
	origin := pipe.NewProc(
		pipe.WithFunc(func(ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				if err := ints.Send(i); err != nil {
					return err
				}
			}
			return nil
		}),
	)

	res := []int{}
	pipe.NewProc(
		pipe.WithSource(0, origin),
		pipe.WithFunc(func(c pipe.Consumer) error {
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				res = append(res, v)
				return nil
			})
		}),
	)

	err := origin.Run()

	if want := error(nil); err != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, err)
	}

	if want := 10; len(res) != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, len(res))
	}

	t.Log(res)
}

func TestNamedOutput(t *testing.T) {
	res := []int{}

	origin := pipe.NewProc(
		pipe.WithOutputs("ints"),
		pipe.WithFunc(func(ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				if err := ints.Send(i); err != nil {
					return err
				}
			}
			return nil
		}),
	)

	pipe.NewProc(
		pipe.WithNamedSource("ints", origin),
		pipe.WithFunc(func(c pipe.Consumer) error {
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				res = append(res, v)
				return nil
			})
		}),
	)

	err := origin.Run()

	if want := error(nil); err != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, err)
	}

	if want := 10; len(res) != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, len(res))
	}
	t.Log(res)
}

func TestCancelation(t *testing.T) {
	origin := pipe.NewProc(
		pipe.WithFunc(func(ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				if err := ints.Send(i); err != nil {
					return err
				}
			}
			return nil
		}),
	)
	pass := pipe.NewProc(
		pipe.WithSource(0, origin),
		pipe.WithFunc(func(c pipe.Consumer, ints pipe.Sender) error {
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				if v == 5 {
					return errors.New("intentional error")
				}
				return ints.Send(v)
			})
		}),
	)

	res := []int{}
	pipe.NewProc(
		pipe.WithSource(0, pass),
		pipe.WithFunc(func(c pipe.Consumer) error {
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				res = append(res, v)
				return nil
			})
		}),
	)

	err := origin.Run()

	if err == nil {
		t.Fatalf("\nwant: %v\n got: %v\n", "error", nil)
	}

	if want := "intentional error"; err.Error() != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, err)
	}

	if want := 5; len(res) != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, len(res))
	}
	t.Log(res)
}

func TestSplit(t *testing.T) {
	odd := []int{}
	even := []int{}

	origin := pipe.NewProc(
		pipe.WithFunc(func(ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				if err := ints.Send(i); err != nil {
					return err
				}
			}
			return nil
		}),
	)

	split := pipe.NewProc(
		pipe.WithSource(0, origin),
		pipe.WithFunc(func(c pipe.Consumer, odds, evens pipe.Sender) error {
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				var err error
				if v&1 == 0 {
					err = evens.Send(v)
				} else {
					err = odds.Send(v)
				}
				return err
			})
		}),
	)

	pipe.NewProc(
		pipe.WithSource(0, split),
		pipe.WithFunc(func(c pipe.Consumer) error {
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				odd = append(odd, v)
				return nil
			})
		}),
	)

	pipe.NewProc(
		pipe.WithSource(1, split),
		pipe.WithFunc(func(c pipe.Consumer) error {
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				even = append(even, v)
				return nil
			})
		}),
	)

	err := origin.Run()
	if want := error(nil); err != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, err)
	}
	for _, v := range odd {
		if v&1 == 0 {
			t.Errorf("wrong value on odd: %v", v)
		}
	}
	for _, v := range even {
		if v&1 == 1 {
			t.Errorf("wrong value on even: %v", v)
		}
	}

	t.Log(even, odd)
}

func TestWorkers(t *testing.T) {
	origin := pipe.NewProc(
		pipe.WithFunc(func(ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				if err := ints.Send(i); err != nil {
					return err
				}
			}
			return nil
		}),
	)

	workerCount := 0
	// each work takes 1 second but having multiple workers they shouldn't take
	// 10(input number) seconds
	workers := pipe.NewProc(
		pipe.WithWorkers(10),
		pipe.WithSource(0, origin),
		pipe.WithFunc(func(c pipe.Consumer, out pipe.Sender) error {
			workerCount++
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				time.Sleep(1 * time.Second)
				return out.Send(v)
			})
		}),
	)
	res := []int{}
	pipe.NewProc(
		pipe.WithSource(0, workers),
		pipe.WithFunc(func(c pipe.Consumer) error {
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				res = append(res, v)
				return nil
			})
		}),
	)

	mark := time.Now()
	err := origin.Run()
	dur := time.Since(mark)

	if want := 10; workerCount != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, workerCount)
	}

	if want := error(nil); err != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, err)
	}

	if dur.Seconds() > 2 {
		t.Errorf("took long enough, workers might be broken")
	}

	if want := 10; len(res) != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, len(res))
	}

	// for better verification
	sort.Ints(res)
	if want := 10; len(res) != want {
		t.Fatalf("\nwant: %v\n got: %v\n", want, len(res))
	}
	for i := 0; i < 10; i++ {
		if want := i; res[i] != want {
			t.Errorf("\nwant: %v\n got: %v\n", res[i], want)
		}
	}

	t.Log(res)
}

// TestMultipleIO will test a proc sending to several and receiving from
// several procs
// - origin will stream to neg and writer
// - writer will receive from origin and neg
func TestMultipleIO(t *testing.T) {
	origin := pipe.NewProc(
		pipe.WithFunc(func(ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				if err := ints.Send(i); err != nil {
					return err
				}
			}
			return nil
		}),
	)

	sum10 := pipe.NewProc(
		pipe.WithSource(0, origin),
		pipe.WithFunc(func(c pipe.Consumer, ints pipe.Sender) error {
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				if err := ints.Send(v + 10); err != nil {
					return err
				}
				return nil
			})
		}),
	)

	res := []int{}
	pipe.NewProc(
		pipe.WithSource(0, origin),
		pipe.WithSource(0, sum10),
		pipe.WithFunc(func(c pipe.Consumer) error {
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				res = append(res, v)
				return nil
			})
		}),
	)

	err := origin.Run()

	if want := error(nil); err != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, err)
	}

	if want := 20; len(res) != want {
		t.Errorf("\nwant: %v\n got: %v\n", want, 20)
	}
	// for better verification
	sort.Ints(res)
	for i := 0; i < 20; i++ {
		if want := i; res[i] != want {
			t.Errorf("\nwant: %v\n got: %v\n", res[i], want)
		}
	}

	t.Log(res)
}

func TestFunc(t *testing.T) {
	tests := []struct {
		name      string
		fn        interface{}
		wantPanic string
	}{
		{
			"1 sender",
			func(s pipe.Sender) error { return nil },
			"",
		},
		{
			"1 consumer",
			func(c pipe.Consumer) error { return nil },
			"",
		},
		{
			"multiple senders",
			func(s1, s2, s3 pipe.Sender) error { return nil },
			"",
		},
		{
			"consumer+multiple senders",
			func(c pipe.Consumer, s1, s2, s3 pipe.Sender) error { return nil },
			"",
		},
		{
			"no params",
			func() error { return nil },
			"func must have at least 1 param",
		},
		{
			"no return",
			func(c pipe.Consumer) {},
			"func should have an error return",
		},
		{
			"consumer misplaced",
			func(p pipe.Sender, c pipe.Consumer) error { return nil },
			"func can only have 1 pipe.Consumer and must be the first argument",
		},
	}

	for _, tt := range tests {
		var rec string
		func() {
			defer func() {
				v := recover()
				if v == nil {
					return
				}
				err, ok := v.(error)
				if !ok {
					t.Fatalf("expected an error panic")
				}
				rec = err.Error()
			}()
			pipe.NewProc(pipe.WithFunc(tt.fn))
		}()

		if want := tt.wantPanic; rec != want {
			t.Errorf("\nwant: %v\n got: %v\n", want, rec)
		}
	}
}
