package pipe

import (
	"context"
	"reflect"
	"sync"

	"golang.org/x/sync/errgroup"
)

type line struct {
	sync.Mutex

	eg  *errgroup.Group
	ctx context.Context

	procs map[*Proc]chan interface{}
	chans map[chan interface{}]int
}

func runLine(ctx context.Context, p *Proc) error {
	g, ctx := errgroup.WithContext(ctx)
	l := line{
		eg:    g,
		ctx:   ctx,
		procs: map[*Proc]chan interface{}{},
		chans: map[chan interface{}]int{},
	}

	l.get(p, 1)

	return g.Wait()
}

func (l *line) add(n int, chs ...chan interface{}) {
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

// get will get or start a proc and return an output cchan to that proc
func (l *line) get(p *Proc, n int) chan interface{} {
	if ch, ok := l.procs[p]; ok {
		l.add(n, ch)
		return ch
	}
	ch := make(chan interface{}, p.opt.Buffer)
	l.add(n, ch)

	l.procs[p] = ch

	fnVal := reflect.ValueOf(p.opt.Func)
	fnTyp := fnVal.Type()

	nworkers := p.opt.Workers
	if nworkers == 0 {
		nworkers = 1
	}

	senders := []sender{}
	for i := 0; i < fnTyp.NumIn()-1; i++ {
		s := sender{ctx: l.ctx}
		// get Indexed outputs
		for _, t := range p.getOutputs(i) {
			s.outputs = append(s.outputs, l.get(t, nworkers))
		}
		senders = append(senders, s)
	}

	for i := 0; i < nworkers; i++ {
		c := &consumer{ctx: l.ctx, input: ch}
		args := []reflect.Value{
			reflect.ValueOf(c),
		}
		for _, s := range senders {
			args = append(args, reflect.ValueOf(s))
		}

		l.eg.Go(func() error {
			defer func() {
				for _, s := range senders {
					l.add(-1, s.outputs...)
				}
			}()

			ret := fnVal.Call(args)
			if err, ok := ret[0].Interface().(error); ok && err != nil {
				return err
			}
			return nil
		})
	}

	return ch
}
