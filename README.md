# Pipes - an utility to create streamable workers

As sometimes we are bound to IO blocks this will help to create workers to
stream data

A Proc:

- only have one consumer and can consume from several sources
- can have more than one sender and each sender can send to several consumers
- should not have a cyclic links

## Example:

```go
func main() {
	// Create an origin that produces data, consumes from an DB/API or etc
	origin := pipe.NewProc(
		pipe.WithFunc(func(_ pipe.Consumer, ints pipe.Sender) error {
			for i := 0; i < 10; i++ {
				if err := ints.Send(i); err !=nil {
					return err
				}
			}
			return nil
		}),
	)

	evenodd := pipe.NewProc(
		pipe.WithWorkers(4),        // use 4 go routines
		pipe.WithSource(0, origin), // consumes output 0 from origin
		pipe.WithFunc(func(c pipe.Consumer, odds, evens pipe.Sender) error {
			return c.Consume(func(vv interface{}) error {
				v := c.Value().(int)
				target := odds
				if v&1 == 0 {
					target = evens
				}
				return target.Send(v)
			}
			return nil
		}),
	)

	res := []int{}
	// consumes data produced by evenodd and write it to result slice
	// could be an API endpoint/file/db
	pipe.NewProc(
		pipe.WithBuffer(10),          // buffer size of the consumer
		pipe.WithSource(0, evenodd),  // consumes output 0 (odds) from evenodd
		pipe.WithSource(1, evenodd),  // consumes output 1 (evens) from evenodd
		pipe.WithFunc(func(c pipe.Consumer) error {
			return c.Consume(func(vv interface{}) error {
				v := c.Value().(string) // we expect strings
				res = append(res, v)
				return nil
			})
		}),
	)

	// Run will start the procs binded to `origin` and wait until all finishes
	// if an error is returned in any proc func the context will be canceled
	// and the first error will be returned here
	if err := origin.Run(); err != nil {
		log.Fatal(err)
	}
}
```
