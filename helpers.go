package pipe

import (
	"fmt"
	"log"
	"math"
	"time"
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

func BackoffConsumer(max time.Duration, factor float64) ConsumerMiddleware {
	b := &backoff{max: max, factor: factor}
	return func(fn ConsumerFunc) ConsumerFunc {
		return func(m Message) error {
			err := fn(m)
			if err == nil {
				return nil
			}
			t := b.forAttempt(1)
			for tries := 1; t < b.max; tries++ {
				<-time.After(t)
				err = fn(m)
				if err == nil {
					return nil
				}
				if err, ok := err.(errFatal); ok {
					return err
				}
				t = b.forAttempt(tries)
			}
			return err
		}
	}
}

type backoff struct {
	max    time.Duration
	factor float64
}

func (c backoff) forAttempt(n int) time.Duration {
	attempt := float64(n)
	min := 100 * time.Millisecond
	max := c.max
	if max <= 0 {
		max = 10 * time.Second
	}
	if min >= max {
		return max
	}
	factor := c.factor
	if factor <= 0 {
		factor = 2
	}
	minf := float64(min)
	durf := minf * math.Pow(factor, attempt)
	// durf = rand.Float64()*(durf-minf) + minf

	if durf > math.MaxInt64 {
		return max
	}
	dur := time.Duration(durf)
	// keep within bounds
	if dur < min {
		return min
	}
	if dur > max {
		return max
	}
	return dur
}
