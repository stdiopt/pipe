package pipe_test

import (
	"fmt"
	"log"
	"sort"

	"github.com/stdiopt/pipe"
)

func Example() {
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

	evenodd := pipe.NewProc(
		pipe.Workers(4),
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

	res := []int{}
	pipe.NewProc(
		pipe.Buffer(10),
		pipe.Source(0, evenodd),
		pipe.Source(1, evenodd),
		pipe.Func(func(c pipe.Consumer) error {
			for c.Next() {
				v := c.Value().(int) // we expect strings
				res = append(res, v)
			}
			return nil
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
