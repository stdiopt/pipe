package pipe

import (
	"context"
	"reflect"
)

// Consumer provides methods to consume a stream
type Consumer interface {
	// Next will block until we received a context cancellation or an input
	// if the input is closed or a cancellation received it returns false returns
	// true if a value was fetched
	//
	//		for c.Next() {
	//			v := c.Value()
	//		}
	//
	Next() bool

	// Value returns the value fetched by the last Next() iteration
	Value() interface{}

	// Consume iterate through the consumer using a func with a typed argument
	// if error is not nil, it will break the iteration
	Consume(fn interface{}) error
}

type consumer struct {
	ctx   context.Context
	input chan interface{}
	value interface{}
}

func (c *consumer) Consume(fn interface{}) error {
	fnVal := reflect.ValueOf(fn)
	fnTyp := fnVal.Type()
	if fnTyp.NumIn() != 1 ||
		fnTyp.NumOut() != 1 ||
		!fnTyp.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		panic("consume param should be 'func(t T) error'")
	}
	args := make([]reflect.Value, 1)
	for c.Next() {
		args[0] = reflect.ValueOf(c.Value())
		ret := fnVal.Call(args)
		if err, ok := ret[0].Interface().(error); ok && err != nil {
			return err
		}
	}
	return nil
}

func (c *consumer) Next() bool {
	select {
	case <-c.ctx.Done():
		return false
	case v, ok := <-c.input:
		if !ok {
			return false
		}
		c.value = v
		return true
	}
}

func (c *consumer) Value() interface{} {
	return c.value
}
