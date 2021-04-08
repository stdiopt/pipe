package pipeutil

import "github.com/stdiopt/pipe"

// Line connects the procs specified in variadic arguments by their 0 sender.
func Line(ps ...*pipe.Proc) *pipe.Proc {
	orig := ps[0]
	last := orig
	for _, p := range ps[1:] {
		last.Link(0, p)
		last = p
	}
	return orig
}
