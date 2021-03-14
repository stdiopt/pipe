package pipe

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

// DumpDOT a proc line in graphviz dot language format
func DumpDOT(p *Proc) string {
	buf := bytes.NewBuffer(nil)

	fmt.Fprintf(buf, "digraph {\n")
	fmt.Fprintln(buf, "\t"+`node[shape=square, style="filled,rounded", width=1, color="#aaaaaa"]`)
	d := &dotWriter{}
	d.links(buf, p)

	for k, v := range d.style {
		fmt.Fprintf(buf, "\t%q[%s]\n", k, v)
	}
	fmt.Fprintf(buf, "}\n")
	return buf.String()
}

type dotWriter struct {
	touched map[*Proc]struct{}
	style   map[string]string

	names     map[*Proc]string
	nameCount int
}

func (d *dotWriter) nodeName(p *Proc) string {
	if d.names == nil {
		d.names = map[*Proc]string{}
	}
	if p.name != "" {
		return p.name
	}
	if n, ok := d.names[p]; ok {
		return n
	}
	d.nameCount++
	name := fmt.Sprintf("<unnamed#%d", d.nameCount)
	d.names[p] = name
	return name
}

func (d *dotWriter) links(w io.Writer, p *Proc) {
	name := d.nodeName(p)
	if d.style == nil {
		d.style = map[string]string{}
	}
	if d.touched == nil {
		d.touched = map[*Proc]struct{}{}
		// default style
		d.style[name] = `shape=circle, fillcolor="#77ee77"`
	}
	if _, ok := d.touched[p]; ok {
		return
	}
	d.touched[p] = struct{}{}

	style := ""
	label := []string{}
	if len(p.targets) == 0 {
		style = `shape=circle fillcolor="#aaaaff"`
	}
	if p.nworkers > 1 {
		style += " peripheries=3"
		label = append(label,
			fmt.Sprintf(`workers: %d`, p.nworkers),
		)
	}
	if p.bufsize > 1 {
		label = append(label,
			fmt.Sprintf(`bufsize: %d`, p.bufsize),
		)
	}
	if len(label) != 0 {
		style += fmt.Sprintf(` label=<%s<br/><br/><font point-size="8">%s</font>>`,
			name,
			strings.Join(label, "<br/>"),
		)
	}

	if style != "" {
		d.style[name] = style
	}

	for i, group := range p.targets {
		for _, o := range group {
			d.links(w, o)
			fmt.Fprintf(w, "\t%q -> %q", name, d.nodeName(o))
			if i < len(p.outputs) {
				fmt.Fprintf(w, " [label=%q]", p.outputs[i])
			}
			fmt.Fprintln(w)
		}
	}
}
