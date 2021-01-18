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
		pipe.Func(func(_ pipe.Consumer, ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				ints.Send(i)
			}
			return nil
		}),
	)

	res := []int{}
	pipe.NewProc(
		pipe.Source(0, origin),
		pipe.Func(func(c pipe.Consumer) error {
			for c.Next() {
				v := c.Value().(int)
				res = append(res, v)
			}
			return nil
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
		pipe.Outputs("ints"),
		pipe.Func(func(_ pipe.Consumer, ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				ints.Send(i)
			}
			return nil
		}),
	)

	pipe.NewProc(
		pipe.Source("ints", origin),
		pipe.Func(func(c pipe.Consumer) error {
			for c.Next() {
				v := c.Value().(int)
				res = append(res, v)
			}
			return nil
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
		pipe.Func(func(_ pipe.Consumer, ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				if err := ints.Send(i); err != nil {
					return err
				}
			}
			return nil
		}),
	)
	pass := pipe.NewProc(
		pipe.Source(0, origin),
		pipe.Func(func(c pipe.Consumer, ints pipe.Sender) error {
			for c.Next() {
				v := c.Value().(int)
				if v == 5 {
					return errors.New("intentional error")
				}
				if err := ints.Send(v); err != nil {
					return err
				}
			}
			return nil
		}),
	)

	res := []int{}
	pipe.NewProc(
		pipe.Source(0, pass),
		pipe.Func(func(c pipe.Consumer) error {
			for c.Next() {
				v := c.Value().(int)
				res = append(res, v)
			}
			return nil
		}),
	)

	err := origin.Run()

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
		pipe.Func(func(_ pipe.Consumer, ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				if err := ints.Send(i); err != nil {
					return err
				}
			}
			return nil
		}),
	)

	split := pipe.NewProc(
		pipe.Source(0, origin),
		pipe.Func(func(c pipe.Consumer, odds, evens pipe.Sender) error {
			for c.Next() {
				v := c.Value().(int)
				var err error
				if v&1 == 0 {
					err = evens.Send(v)
				} else {
					err = odds.Send(v)
				}
				if err != nil {
					return err
				}
			}
			return nil
		}),
	)

	pipe.NewProc(
		pipe.Source(0, split),
		pipe.Func(func(c pipe.Consumer) error {
			for c.Next() {
				v := c.Value().(int)
				odd = append(odd, v)
			}
			return nil
		}),
	)

	pipe.NewProc(
		pipe.Source(1, split),
		pipe.Func(func(c pipe.Consumer) error {
			for c.Next() {
				v := c.Value().(int)
				even = append(even, v)
			}
			return nil
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
		pipe.Func(func(_ pipe.Consumer, ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				ints.Send(i)
			}
			return nil
		}),
	)
	// each work takes 1 second but having multiple workers they shouldn't take
	// 10(input number) seconds
	workers := pipe.NewProc(
		pipe.Workers(10),
		pipe.Source(0, origin),
		pipe.Func(func(c pipe.Consumer, out pipe.Sender) error {
			for c.Next() {
				v := c.Value().(int)
				time.Sleep(1 * time.Second)
				if err := out.Send(v); err != nil {
					return err
				}
			}
			return nil
		}),
	)
	res := []int{}
	pipe.NewProc(
		pipe.Source(0, workers),
		pipe.Func(func(c pipe.Consumer) error {
			for c.Next() {
				v := c.Value().(int)
				res = append(res, v)
			}
			return nil
		}),
	)

	mark := time.Now()
	err := origin.Run()
	dur := time.Since(mark)

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
		pipe.Func(func(_ pipe.Consumer, ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				ints.Send(i)
			}
			return nil
		}),
	)

	sum10 := pipe.NewProc(
		pipe.Source(0, origin),
		pipe.Func(func(c pipe.Consumer, ints pipe.Sender) error {
			for c.Next() {
				v := c.Value().(int)
				if err := ints.Send(v + 10); err != nil {
					return err
				}
			}
			return nil
		}),
	)

	res := []int{}
	pipe.NewProc(
		pipe.Source(0, origin),
		pipe.Source(0, sum10),
		pipe.Func(func(c pipe.Consumer) error {
			for c.Next() {
				v := c.Value().(int)
				res = append(res, v)
			}
			return nil
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
