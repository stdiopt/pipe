package pipe

import (
	"fmt"
	"log"
)

type ConsumerMiddleware func(fn ConsumerFunc) ConsumerFunc

func mergeMiddlewares(mws ...ConsumerMiddleware) ConsumerMiddleware {
	return func(fn ConsumerFunc) ConsumerFunc {
		for i := len(mws) - 1; i >= 0; i-- {
			fn = mws[i](fn)
		}
		return fn
	}
}

type errFatal struct{ err error }

func (e errFatal) Error() string { return e.err.Error() }
func (e errFatal) Unwrap() error { return e.err }

func RetryConsumer(tries int) ConsumerMiddleware {
	return func(fn ConsumerFunc) ConsumerFunc {
		return func(m Message) error {
			retry := 0
			var err error
			for ; retry <= tries; retry++ {
				if retry > 0 {
					log.Printf("retrying: %v", m.Value())
				}
				err = fn(m)
				if err == nil {
					break
				}
				if err, ok := err.(errFatal); ok {
					return err
				}
			}
			if err != nil {
				return fmt.Errorf("%w (retries: %d) %T", err, retry, m.Value())
			}
			return nil
		}
	}
}

func LogConsumer(prefix string) ConsumerMiddleware {
	return func(fn ConsumerFunc) ConsumerFunc {
		log := log.New(log.Writer(), fmt.Sprintf("[%s] ", prefix), 0)
		return func(m Message) error {
			log.Println("Received:", m)
			return fn(m)
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
