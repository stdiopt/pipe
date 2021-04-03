package pipe

import (
	"context"
	"fmt"
)

type (
	ConsumerFunc       func(v interface{}) error
	ConsumerMiddleware func(fn ConsumerFunc) ConsumerFunc
)

func mergeMiddlewares(mws ...ConsumerMiddleware) ConsumerMiddleware {
	return func(fn ConsumerFunc) ConsumerFunc {
		for i := len(mws) - 1; i >= 0; i-- {
			fn = mws[i](fn)
		}
		return fn
	}
}

// Consumer provides methods to consume a stream
type Consumer interface {
	// Context returns the current consumer context
	Context() context.Context

	Consume(ConsumerFunc) error
}

type consumer struct {
	ctx        context.Context
	input      chan Message
	middleware func(fn ConsumerFunc) ConsumerFunc
}

func (c *consumer) Context() context.Context { return c.ctx }

func (c *consumer) Consume(fn ConsumerFunc) error {
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
			if err := fn(v.Value()); err != nil {
				return fmt.Errorf("error: %w, origin: %v", err, v.Origin())
			}
		}
	}
}
