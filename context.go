package pipe

import (
	"context"
	"errors"
	"reflect"
)

// Sender a channel writer wrapper
type Sender interface {
	Send(v interface{}) error
}

// Consumer provides methods to consume a stream
type Consumer interface {
	Next() bool
	Value() interface{}
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

// Next will block until we received a context cancelation or an input
// if the input is closed or a cancelation received it returns false returns
// true if a value was fetched
//
//		for c.Next() {
//			v := c.Value()
//		}
//
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

// Value returns the value fetched by the last Next() iteration
func (c *consumer) Value() interface{} {
	return c.value
}

type sender struct {
	ctx     context.Context
	outputs []chan interface{}
}

func (p sender) Send(v interface{}) error {
	for _, ch := range p.outputs {
		select {
		case <-p.ctx.Done():
			return errors.New("canceled")
		case ch <- v:
		}
	}
	return nil
}
