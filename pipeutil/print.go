package pipeutil

import (
	"fmt"
	"io"

	"github.com/stdiopt/pipe"
)

func NewFMTWriter(w io.Writer) *pipe.Proc {
	return pipe.NewProc(
		pipe.WithFunc(func(c pipe.Consumer) error {
			return c.Consume(func(vv interface{}) error {
				_, err := fmt.Fprint(w, vv, "\n")
				return err
			})
		}),
	)
}
