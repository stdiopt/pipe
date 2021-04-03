package pipeutil

import (
	"fmt"
	"io"

	"github.com/stdiopt/pipe"
)

type ErrInvalidData string

func (e ErrInvalidData) Error() string {
	return fmt.Sprintf("invalid data: %q", string(e))
}

type ReadErrorCloser interface {
	Read(b []byte) (int, error)
	CloseWithError(error) error
}

// AsReader returns the consumer as reader, consumer data should be bytes
func AsReader(c pipe.Consumer) ReadErrorCloser {
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		c.Consume(func(vv interface{}) error {
			b, ok := vv.([]byte)
			if !ok {
				pw.CloseWithError(ErrInvalidData(fmt.Sprintf("%T", vv)))
				return nil
			}
			if _, err := pw.Write(b); err != nil {
				pw.CloseWithError(err)
				return nil
			}
			return nil
		})
	}()
	return pr
}

// NewIOReader returns a proc that reads a reader and sends []byte on channel 0.
func NewIOReader(r io.Reader, opts ...pipe.ProcFunc) *pipe.Proc {
	return pipe.NewProc(
		pipe.Group(opts...),
		pipe.WithFunc(func(s pipe.Sender) error {
			buf := make([]byte, 4096)
			for {
				n, err := r.Read(buf)
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}

				b := make([]byte, n)
				copy(b, buf)
				if err := s.Send(b); err != nil {
					return err
				}
			}
			return nil
		}),
	)
}

// NewIOWriter returns a proc that writes bytes received from consumer.
func NewIOWriter(w io.Writer, opts ...pipe.ProcFunc) *pipe.Proc {
	return pipe.NewProc(
		pipe.Group(opts...),
		pipe.WithFunc(func(c pipe.Consumer) error {
			return c.Consume(func(vv interface{}) error {
				b, ok := vv.([]byte)
				if !ok {
					return fmt.Errorf("invalid type: %T", vv)
				}
				_, err := w.Write(b)
				return err
			})
		}),
	)
}
