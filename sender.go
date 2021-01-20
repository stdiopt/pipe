package pipe

import (
	"context"
	"errors"
)

// Sender a channel writer wrapper
type Sender interface {
	// Send a value
	Send(v interface{}) error
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
