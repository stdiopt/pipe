package pipe

import (
	"context"
	"fmt"
	"reflect"
)

type ConsumerFunc func(v Message) error

// Consumer provides methods to consume a stream
type Consumer interface {
	// Context returns the current consumer context
	Context() context.Context

	// Consume will call the fn for every value received,
	// fn must be a func with a signature like `func(T)error` where T is any type
	Consume(fn interface{}) error
}

type consumer struct {
	ctx   context.Context
	input chan Message
	// middleware wraps middleware func before consuming
	middleware func(fn ConsumerFunc) ConsumerFunc
}

func (c *consumer) Context() context.Context { return c.ctx }

// Consume will pass the consumer function through the middleware stack and
// call the fn for every value received, returning an error will pass error
// through and break the reader loop.
func (c *consumer) Consume(ifn interface{}) error {
	fn := makeConsumerFunc(ifn)
	if c.middleware != nil {
		fn = c.middleware(fn)
	}
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case v, ok := <-c.input:
			if !ok {
				return nil
			}
			if err := fn(v); err != nil {
				return fmt.Errorf("%w, origin: %v", err, v.Origin())
			}
		}
	}
}

func makeConsumerFunc(fn interface{}) ConsumerFunc {
	switch fn := fn.(type) {
	case func(m Message) error:
		return fn
	case func(v interface{}) error:
		return func(m Message) error { return fn(m.Value()) }
	}

	fnVal := reflect.ValueOf(fn)
	fnTyp := fnVal.Type()
	if fnTyp.NumIn() != 1 ||
		fnTyp.NumOut() != 1 ||
		!fnTyp.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		panic("consume param should be 'func(t T) error'")
	}
	args := make([]reflect.Value, 1)
	return func(m Message) error {
		args[0] = reflect.ValueOf(m.Value())
		ret := fnVal.Call(args)
		if err, ok := ret[0].Interface().(error); ok && err != nil {
			return err
		}
		return nil
	}
}
