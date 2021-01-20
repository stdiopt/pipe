/*Package pipe - an utility to create streamable workers

As sometimes we are bound to IO blocks this will help to create workers to
stream data

*/
package pipe

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// ProcFunc is used by the Options funcs
type ProcFunc func(p *Proc)

type group []*Proc

// Proc is a pipeline processor, should be created with 'NewProc' and must have
// a func option
type Proc struct {
	mu       sync.Mutex
	nworkers int
	bufsize  int
	fn       interface{}
	outputs  []string
	targets  map[int]group
}

// NewProc is used to create a Proc
//
//	p := pipe.NewProc(
//		pipe.Buffer(8),
//		pipe.Workers(10),
//		pipe.Func(func(c pipe.Consumer, s1, s2 pipe.Sender) error {
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

func (p *Proc) String() string {
	return fmt.Sprintf("%T", p.fn)
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

// Func sets the proc Function option as function must have a consumer
// and optionally 1 or more senders
func Func(fn interface{}) ProcFunc {
	if err := validateProcFunc(reflect.TypeOf(fn)); err != nil {
		panic(err)
	}
	return func(p *Proc) { p.fn = fn }
}

// Workers sets the proc workers
func Workers(n int) ProcFunc {
	return func(p *Proc) { p.nworkers = n }
}

// Buffer sets the receive channel buffer
func Buffer(n int) ProcFunc {
	return func(p *Proc) { p.bufsize = n }
}

// Outputs describes the proc outputs to be used by linkers
// the name index must match the Func signature of senders
func Outputs(o ...string) ProcFunc {
	return func(p *Proc) { p.outputs = o }
}

// Target will link this proc output identiied by k to targets
func Target(k interface{}, targets ...*Proc) ProcFunc {
	return func(p *Proc) { p.Link(k, targets...) }
}

// Source will link this proc to the sources by the outputs identified by k
func Source(k interface{}, source ...*Proc) ProcFunc {
	return func(p *Proc) {
		for _, s := range source {
			s.Link(k, p)
		}
	}
}
