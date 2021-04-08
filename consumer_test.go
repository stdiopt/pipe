package pipe

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestConsumer(t *testing.T) {
	tests := []struct {
		name       string
		sender     func(context.Context, chan Message)
		middleware ConsumerMiddleware
		consumer   func(v interface{}) error
		wantErr    string
		wantRes    []interface{}
	}{
		{
			name: "sends",
			sender: func(_ context.Context, ch chan Message) {
				for i := 0; i < 10; i++ {
					ch <- message{value: i}
				}
			},
			wantRes: []interface{}{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name: "error",
			consumer: func(v interface{}) error {
				return errors.New("test")
			},
			wantErr: "test, origin: <nil>",
		},
		{
			name: "partial error",
			sender: func(_ context.Context, ch chan Message) {
				for i := 0; i < 10; i++ {
					ch <- message{value: i}
				}
			},
			consumer: func(v interface{}) error {
				if v.(int) > 5 {
					return errors.New("test")
				}
				return nil
			},
			wantErr: "test, origin: <nil>",
			wantRes: []interface{}{0, 1, 2, 3, 4, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ch := make(chan Message)
			go func() {
				defer close(ch)
				if tt.sender != nil {
					tt.sender(ctx, ch)
				} else {
					ch <- message{value: 0}
				}
			}()

			c := &consumer{ctx: ctx, input: ch, middleware: tt.middleware}

			var res []interface{}

			err := c.Consume(func(v interface{}) error {
				if tt.consumer != nil {
					if err := tt.consumer(v); err != nil {
						return err
					}
				}
				res = append(res, v)
				return nil
			})

			if want := tt.wantErr; (err == nil && want != "") || (err != nil && err.Error() != want) {
				t.Errorf("\nwant: %v\n got: %v\n", want, err)
			}
			if want := tt.wantRes; !reflect.DeepEqual(res, want) {
				t.Errorf("\nwant: %v\n got: %v\n", want, res)
			}
		})
	}
}
