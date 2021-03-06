/*Package pipe - an utility to create streamable workers

As sometimes we are bound to IO blocks this will help to create workers to
stream data

*/
package pipe

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// ProcFunc is used by the Options funcs
type ProcFunc func(p *Proc)

type group []*Proc

// Proc is a pipeline processor, should be created with 'NewProc' and must have
// a func option
type Proc struct {
	name     string
	mu       sync.Mutex
	nworkers int
	bufsize  int
	fn       interface{}

	consumerMiddleware func(ConsumerFunc) ConsumerFunc

	outputs []string
	targets map[int]group
}

func (p *Proc) String() string {
	ret := ""

	if p.name != "" {
		ret = p.name
	}
	if len(p.outputs) > 0 {
		ret += ":" + strings.Join(p.outputs, ",")
	}
	if ret == "" {
		return fmt.Sprintf("<unnamed#%p>", p)
	}

	return fmt.Sprintf("<%s>", ret)
}

// NewProc is used to create a Proc
//
//	p := pipe.NewProc(
//		pipe.WithBuffer(8),
//		pipe.WithWorkers(10),
//		pipe.WithFunc(func(c pipe.Consumer, s1, s2 pipe.Sender) error {
//			...
//		}),
//	)
func NewProc(opt ...ProcFunc) *Proc {
	p := &Proc{}
	for _, fn := range opt {
		fn(p)
	}
	return p
}

// Run will start processors sequentially and blocks until all completed
func (p *Proc) Run() error {
	return runLine(context.Background(), p)
}

// RunWithContext starts processors with the given context, if the context is
// canceled all workers should stop
func (p *Proc) RunWithContext(ctx context.Context) error {
	return runLine(ctx, p)
}

// Link send output to specified procs, 'k' can be an int or string
// if it is a string it will query params by name declared in 'Output' option
func (p *Proc) Link(k interface{}, t ...*Proc) {
	n := -1
	switch v := k.(type) {
	case int:
		n = v
	case string:
		n = p.namedOutput(v)
	}

	if n < 0 {
		return
	}

	if p.targets == nil {
		p.targets = map[int]group{}
	}
	p.targets[n] = append(p.targets[n], t...)
}

func (p *Proc) namedOutput(k string) int {
	for i, o := range p.outputs {
		if o == k {
			return i
		}
	}
	return -1
}

// getOutputs returns a new copy of outputs
func (p *Proc) getOutputs(k int) group {
	p.mu.Lock()
	defer p.mu.Unlock()
	g, ok := p.targets[k]
	if !ok || len(g) == 0 {
		return nil
	}
	return append(group{}, g...)
}

// Functional options

// Group groups options in one ProcFunc
func Group(fns ...ProcFunc) ProcFunc {
	return func(p *Proc) {
		for _, fn := range fns {
			fn(p)
		}
	}
}

// WithName sets optional proc name for easier debugging
func WithName(n string) ProcFunc {
	return func(p *Proc) { p.name = n }
}

// WithFunc sets the proc Function option as function must have a consumer
// and optionally 1 or more senders
func WithFunc(fn interface{}) ProcFunc {
	if err := validateProcFunc(reflect.TypeOf(fn)); err != nil {
		panic(err)
	}
	return func(p *Proc) { p.fn = fn }
}

// WithWorkers sets the proc workers
func WithWorkers(n int) ProcFunc {
	return func(p *Proc) { p.nworkers = n }
}

// WithBuffer sets the receive channel buffer
func WithBuffer(n int) ProcFunc {
	return func(p *Proc) { p.bufsize = n }
}

// WithOutputs describes the proc outputs to be used by linkers
// the name index must match the Func signature of senders
func WithOutputs(o ...string) ProcFunc {
	return func(p *Proc) { p.outputs = o }
}

// WithTarget will link this proc output identified by k to targets.
func WithTarget(k int, targets ...*Proc) ProcFunc {
	return func(p *Proc) { p.Link(k, targets...) }
}

// WithNamedTarget will link this proc output identified by name to targets.
func WithNamedTarget(k string, targets ...*Proc) ProcFunc {
	return func(p *Proc) { p.Link(k, targets...) }
}

// WithSource will link this proc to the sources by the outputs identified by
// index n.
func WithSource(n int, source ...*Proc) ProcFunc {
	return func(p *Proc) {
		for _, s := range source {
			s.Link(n, p)
		}
	}
}

// WithNamedSource will link this proc to the sources by the outputs identified
// by name s.
func WithNamedSource(n string, source ...*Proc) ProcFunc {
	return func(p *Proc) {
		for _, s := range source {
			s.Link(n, p)
		}
	}
}

// WithConsumerMiddleware sets a ConsumerMiddleware to be used while consuming data.
func WithConsumerMiddleware(mws ...ConsumerMiddleware) ProcFunc {
	return func(p *Proc) {
		p.consumerMiddleware = mergeMiddlewares(mws...)
	}
}
