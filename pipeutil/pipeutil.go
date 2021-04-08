// Package pipeutil contains utils for pipe.
package pipeutil

import "github.com/stdiopt/pipe"

func NewValueProc(v interface{}, opts ...pipe.ProcFunc) *pipe.Proc {
	return pipe.NewProc(
		pipe.WithFunc(func(s pipe.Sender) error {
			return s.Send(v)
		}),
	)
}
