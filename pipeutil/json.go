package pipeutil

import (
	"encoding/json"
	"reflect"

	"github.com/stdiopt/pipe"
)

// NewStreamerJSON sample object to decode a json []byte stream and send the object
func NewStreamerJSON(v interface{}, opts ...pipe.ProcFunc) *pipe.Proc {
	typ := reflect.Indirect(reflect.ValueOf(v)).Type()
	return pipe.NewProc(
		pipe.Group(opts...),
		pipe.WithWorkers(1),
		pipe.WithFunc(func(c pipe.Consumer, s pipe.Sender) error {
			rd := AsReader(c)
			dec := json.NewDecoder(rd)
			for {
				v := reflect.New(typ).Interface()
				if err := dec.Decode(&v); err != nil {
					rd.CloseWithError(err)
					return err
				}
				if err := s.Send(v); err != nil {
					rd.CloseWithError(err)
					return err
				}
			}
		}),
	)
}
