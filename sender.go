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
	origin  *Proc
	outputs []chan Message
}

func (p sender) Send(v interface{}) error {
	for _, ch := range p.outputs {
		select {
		case <-p.ctx.Done():
			return errors.New("canceled")
		case ch <- message{p.origin, v}:
		}
	}
	return nil
}
