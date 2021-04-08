package pipeutil

import (
	"reflect"

	"github.com/stdiopt/pipe"
)

func Transform(fn interface{}, opts ...pipe.ProcFunc) *pipe.Proc {
	return pipe.NewProc(
		pipe.Group(opts...),
		TransformFunc(fn),
	)
}

// TransformFunc returns a procfunc that executes fn to transform a single
// input to a single output.
func TransformFunc(fn interface{}) pipe.ProcFunc {
	fnVal := reflect.ValueOf(fn)
	fnTyp := fnVal.Type()
	if fnTyp.NumIn() != 1 ||
		fnTyp.NumOut() != 2 ||
		!fnTyp.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		panic("consume param should be 'func(t T1) (T2, error)'")
	}
	args := make([]reflect.Value, 1)
	return pipe.WithFunc(func(c pipe.Consumer, s pipe.Sender) error {
		return c.Consume(func(vv interface{}) error {
			args[0] = reflect.ValueOf(vv)
			ret := fnVal.Call(args)
			if err, ok := ret[1].Interface().(error); ok && err != nil {
				return err
			}
			return s.Send(ret[0].Interface())
		})
	})
}
