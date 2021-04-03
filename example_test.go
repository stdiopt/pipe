package pipe_test

import (
	"fmt"
	"log"
	"sort"

	"github.com/stdiopt/pipe"
)

func Example() {
	origin := pipe.NewProc(
		pipe.WithFunc(func(_ pipe.Consumer, ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				if err := ints.Send(i); err != nil {
					return err
				}
			}
			return nil
		}),
	)

	evenodd := pipe.NewProc(
		pipe.WithWorkers(4),
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

	res := []int{}
	pipe.NewProc(
		pipe.WithBuffer(10),
		pipe.WithSource(0, evenodd),
		pipe.WithSource(1, evenodd),
		pipe.WithFunc(func(c pipe.Consumer) error {
			return c.Consume(func(vv interface{}) error {
				v := vv.(int)
				res = append(res, v)
				return nil
			})
		}),
	)
	if err := origin.Run(); err != nil {
		log.Fatal(err)
	}
	sort.Ints(res)
	fmt.Println(res)

	// Output:
	// [0 1 2 3 4 5 6 7 8 9]
}
