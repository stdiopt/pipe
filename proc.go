package pipe

import (
	"context"
	"fmt"
	"sync"
)

type ProcFunc func(p *Proc)

type group []*Proc

// ProcOptions options to start a processor
type ProcOptions struct {
	// Workers number of routines running the Func
	Workers int
	// Buffer input buffer size
	Buffer int
	// Func the worker function that will run to start consume and produce on
	// the processor
	Func interface{}
	// Outputs declare named outputs to be in the same order as the Func params
	Outputs []string
}

type Proc struct {
	opt     ProcOptions
	mu      sync.Mutex
	targets map[int]group
}

// ProcWithOptions creates a new processor
func ProcWithOptions(opt ProcOptions) *Proc {
	if opt.Func == nil {
		panic("missing option: Func")
	}
	return &Proc{opt: opt}
}

func NewProc(opt ...ProcFunc) *Proc {
	p := &Proc{}
	for _, fn := range opt {
		fn(p)
	}
	return p
}

func (p *Proc) String() string {
	return fmt.Sprintf("%T", p.opt.Func)
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
		// panic(fmt.Sprintf("link: '%v' doesn't exists on proc", k))
	}

	if p.targets == nil {
		p.targets = map[int]group{}
	}
	p.targets[n] = append(p.targets[n], t...)
}

func (p *Proc) namedOutput(k string) int {
	for i, o := range p.opt.Outputs {
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
	return func(p *Proc) { p.opt.Func = fn }
}

// Workers sets the proc workers
func Workers(n int) ProcFunc {
	return func(p *Proc) { p.opt.Workers = n }
}

// Buffer sets the receive channel buffer
func Buffer(n int) ProcFunc {
	return func(p *Proc) { p.opt.Buffer = n }
}

// Outputs describes the proc outputs to be used by linkers
// the name index must match the Func signature of senders
func Outputs(s ...string) ProcFunc {
	return func(p *Proc) { p.opt.Outputs = s }
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
