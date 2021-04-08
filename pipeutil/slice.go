package pipeutil

import (
	"fmt"
	"reflect"

	"github.com/stdiopt/pipe"
)

func UnsliceFunc() pipe.ProcFunc {
	return pipe.WithFunc(func(c pipe.Consumer, s pipe.Sender) error {
		return c.Consume(func(v interface{}) error {
			val := reflect.ValueOf(v)
			if val.Type().Kind() != reflect.Slice {
				return fmt.Errorf("wrong type: %T, should be a slice", v)
			}
			for i := 0; i < val.Len(); i++ {
				if err := s.Send(val.Index(i).Interface()); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// NewUnsliceProc returns a proc that iterates a received slice and sends each element.
func NewUnsliceProc(opts ...pipe.ProcFunc) *pipe.Proc {
	return pipe.NewProc(
		pipe.Group(opts...),
		UnsliceFunc(),
	)
}
