package pipe

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Message is the type that flows along a line, with the current value and
// origin
type Message interface {
	Origin() *Proc
	Value() interface{}
}

type message struct {
	origin *Proc
	value  interface{}
}

func (m message) Origin() *Proc      { return m.origin }
func (m message) Value() interface{} { return m.value }

type line struct {
	sync.Mutex

	eg  *errgroup.Group
	ctx context.Context

	procs map[*Proc]chan Message
	chans map[chan Message]int
}

func runLine(ctx context.Context, p *Proc) error {
	g, ctx := errgroup.WithContext(ctx)
	l := line{
		eg:    g,
		ctx:   ctx,
		procs: map[*Proc]chan Message{},
		chans: map[chan Message]int{},
	}

	l.get(p, 1)

	return g.Wait()
}

func (l *line) add(n int, chs ...chan Message) {
	l.Lock()
	defer l.Unlock()

	for _, ch := range chs {
		v := l.chans[ch] + n
		if v == 0 {
			close(ch)
			delete(l.chans, ch)
			continue
		}
		l.chans[ch] = v
	}
}

// get will get or start a proc and return an output chan to that proc.
func (l *line) get(p *Proc, n int) chan Message {
	if ch, ok := l.procs[p]; ok {
		l.add(n, ch)
		return ch
	}
	ch := make(chan Message, p.bufsize)
	l.add(n, ch)

	l.procs[p] = ch

	fnVal := reflect.ValueOf(p.fn)
	fnTyp := fnVal.Type()

	nworkers := p.nworkers
	if nworkers <= 0 {
		nworkers = 1
	}

	// Senders are shared across workers
	senders := []sender{}
	nsenders := fnTyp.NumIn()
	if fnTyp.In(0) == consumerTyp {
		nsenders--
	}
	for i := 0; i < nsenders; i++ {
		// get Indexed outputs
		outputs := []chan Message{}
		for _, t := range p.getOutputs(i) {
			outputs = append(outputs, l.get(t, nworkers))
		}
		s := sender{
			ctx:     l.ctx,
			origin:  p,
			outputs: outputs,
		}
		senders = append(senders, s)
	}

	args := make([]reflect.Value, 0, fnTyp.NumIn())
	if fnTyp.In(0) == consumerTyp {
		c := &consumer{
			ctx:        l.ctx,
			input:      ch,
			middleware: p.consumerMiddleware,
		}
		args = append(args, reflect.ValueOf(c))
	}
	for _, s := range senders {
		args = append(args, reflect.ValueOf(s))
	}

	for i := 0; i < nworkers; i++ {
		l.eg.Go(func() error {
			defer func() {
				for _, s := range senders {
					l.add(-1, s.outputs...)
				}
			}()

			ret := fnVal.Call(args)
			if len(ret) > 0 {
				if err, ok := ret[0].Interface().(error); ok && err != nil {
					return err
				}
			}
			return nil
		})
	}

	return ch
}

var (
	consumerTyp = reflect.TypeOf((*Consumer)(nil)).Elem()
	senderTyp   = reflect.TypeOf((*Sender)(nil)).Elem()
	errTyp      = reflect.TypeOf((*error)(nil)).Elem()
)

func validateProcFunc(fnTyp reflect.Type) error {
	if fnTyp.NumIn() == 0 {
		return errors.New("func must have at least 1 param")
	}
	for i := 0; i < fnTyp.NumIn(); i++ {
		arg := fnTyp.In(i)
		if arg != consumerTyp && arg != senderTyp {
			return errors.New("func param must be either a pipe.Consumer or pipe.Sender")
		}
		if arg == consumerTyp && i != 0 {
			return errors.New("func can only have 1 pipe.Consumer and must be the first argument")
		}
	}
	if fnTyp.NumOut() != 1 || fnTyp.Out(0) != errTyp {
		return errors.New("func should have an error return")
	}
	return nil
}
