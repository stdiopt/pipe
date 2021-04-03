package pipe

import (
	"fmt"
	"log"
	"reflect"
)

type errFatal struct{ err error }

func (e errFatal) Error() string { return e.err.Error() }
func (e errFatal) Unwrap() error { return e.err }

func TypedConsumer(fn interface{}) ConsumerFunc {
	fnVal := reflect.ValueOf(fn)
	fnTyp := fnVal.Type()
	if fnTyp.NumIn() != 1 ||
		fnTyp.NumOut() != 1 ||
		!fnTyp.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		panic("consume param should be 'func(T) error'")
	}
	args := make([]reflect.Value, 1)

	return func(v interface{}) error {
		args[0] = reflect.ValueOf(v)
		if args[0].Type() != fnTyp.In(0) {
			return errFatal{fmt.Errorf("invalid value type: %q for consumer type: %q", args[0].Type(), fnTyp.In(0))}
		}

		ret := fnVal.Call(args)
		if err, ok := ret[0].Interface().(error); ok && err != nil {
			return err
		}
		return nil
	}
}

func TypedConsume(c Consumer, fn interface{}) error {
	return c.Consume(TypedConsumer(fn))
}

func RetryConsumer(tries int) func(fn ConsumerFunc) ConsumerFunc {
	return func(fn ConsumerFunc) ConsumerFunc {
		return func(v interface{}) error {
			retry := 0
			var err error
			for ; retry <= tries; retry++ {
				if retry > 0 {
					log.Printf("retrying: %v", v)
				}
				err = fn(v)
				if err == nil {
					break
				}
				if err, ok := err.(errFatal); ok {
					return err
				}
			}
			if err != nil {
				return fmt.Errorf("%w (retries: %d) %T", err, retry, v)
			}
			return nil
		}
	}
}

func LogConsumer(prefix string) ConsumerMiddleware {
	return func(fn ConsumerFunc) ConsumerFunc {
		log := log.New(log.Writer(), fmt.Sprintf("[%s] ", prefix), 0)
		return func(v interface{}) error {
			log.Println("Received:", v)
			return fn(v)
		}
	}
}

// RetryConsumer that retries on failure
/*type RetryConsumer struct {
	Consumer
	Tries int
}

func NewRetryConsumer(c Consumer, tries int) RetryConsumer {
	return RetryConsumer{c, tries}
}

func (r RetryConsumer) Consume(fn ConsumerFunc) error {
	return r.Consumer.Consume(func(v interface{}) error {
		var err error
		retry := 0
		for ; retry <= r.Tries; retry++ {
			err = fn(v)
			if err == nil {
				break
			}
			if err, ok := err.(errFatal); ok {
				return err
			}
		}
		if err != nil {
			return fmt.Errorf("%w (retries: %d)", err, retry)
		}
		return nil
	})
}*/

type BackoffConsumer struct {
	Consumer Consumer
}
