package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
)

const maxTypes = 16

type Header struct {
	Format  string           `json:"format"`
	Types   []string         `json:"types"`
	Initial map[string]int32 `json:"initial,omitempty"`
}

type Op struct {
	ID      int64            `json:"id"`
	Client  int              `json:"client"`
	Replica int              `json:"replica"`
	Kind    string           `json:"kind"`
	Call    int64            `json:"call"`
	Ret     int64            `json:"ret"`
	Status  string           `json:"status"`
	Writes  map[string]int32 `json:"writes,omitempty"`
	Reads   map[string]int32 `json:"reads,omitempty"`
	Skipped []string         `json:"skipped,omitempty"`
}

type input struct {
	write bool
	mask  uint32
	vals  [maxTypes]int32
}

type output struct {
	skip uint32
	vals [maxTypes]int32
}

type state [maxTypes]int32

type summary struct {
	Result            string  `json:"result"`
	Operations        int     `json:"operations"`
	Checked           int     `json:"checked"`
	Writes            int     `json:"writes"`
	Reads             int     `json:"reads"`
	PendingWrites     int     `json:"pendingWrites"`
	DroppedFailed     int     `json:"droppedFailed"`
	DroppedUnobserved int     `json:"droppedUnobserved"`
	ElapsedMs         float64 `json:"elapsedMs"`
	Viz               string  `json:"viz,omitempty"`
}

func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "checker: "+format+"\n", a...)
	os.Exit(3)
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			fail("internal error: %v", r)
		}
	}()
	flags := flag.NewFlagSet("checker", flag.ContinueOnError)
	path := flags.String("history", "", "history file (JSONL)")
	timeout := flags.Duration("timeout", 10*time.Minute, "checker timeout; 0 = none")
	viz := flags.String("viz", "", "write an HTML visualisation here when the history is not linearizable")
	if err := flags.Parse(os.Args[1:]); err != nil {
		fail("%v", err)
	}
	if *path == "" {
		fail("-history is required")
	}
	hdr, ops := load(*path)
	history, sum := build(hdr, ops)

	model := configModel(len(hdr.Types), hdr.Types, initialState(hdr))
	start := time.Now()
	res, info := porcupine.CheckOperationsVerbose(model, history, *timeout)
	sum.ElapsedMs = float64(time.Since(start).Microseconds()) / 1000
	sum.Result = string(res)
	if res == porcupine.Illegal && *viz != "" {
		if err := porcupine.VisualizePath(model, info, *viz); err != nil {
			fmt.Fprintf(os.Stderr, "checker: viz: %v\n", err)
		} else {
			sum.Viz = *viz
		}
	}
	out, _ := json.Marshal(sum)
	fmt.Println(string(out))
	switch res {
	case porcupine.Ok:
		os.Exit(0)
	case porcupine.Illegal:
		os.Exit(1)
	default:
		os.Exit(2)
	}
}

func load(path string) (Header, []Op) {
	f, err := os.Open(path)
	if err != nil {
		fail("%v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	if !sc.Scan() {
		fail("empty history")
	}
	var hdr Header
	if err := json.Unmarshal(sc.Bytes(), &hdr); err != nil || hdr.Format != "reeve-history/1" {
		fail("first line must be a reeve-history/1 header")
	}
	if len(hdr.Types) == 0 || len(hdr.Types) > maxTypes {
		fail("1..%d types supported", maxTypes)
	}
	for t := range hdr.Initial {
		if !contains(hdr.Types, t) {
			fail("initial names %q, which is not in types", t)
		}
	}
	var ops []Op
	for line := 2; sc.Scan(); line++ {
		if len(strings.TrimSpace(sc.Text())) == 0 {
			continue
		}
		var o Op
		if err := json.Unmarshal(sc.Bytes(), &o); err != nil {
			fail("line %d: %v", line, err)
		}
		ops = append(ops, o)
	}
	return hdr, ops
}

func build(hdr Header, ops []Op) ([]porcupine.Operation, summary) {
	idx := map[string]int{}
	for i, t := range hdr.Types {
		idx[t] = i
	}
	sum := summary{Operations: len(ops)}
	type key struct {
		t int
		v int32
	}
	seen := map[key]bool{}
	for t, v := range hdr.Initial {
		seen[key{idx[t], v}] = true
	}
	for _, o := range ops {
		for t := range o.Reads {
			if _, ok := idx[t]; !ok {
				fail("op %d: read of %q, which is not in types", o.ID, t)
			}
		}
		for _, t := range o.Skipped {
			if _, ok := idx[t]; !ok {
				fail("op %d: skipped %q, which is not in types", o.ID, t)
			}
		}
		if o.Kind == "write" {
			for t, v := range o.Writes {
				ti, ok := idx[t]
				if !ok || v == 0 {
					fail("op %d: unknown type %q or value 0", o.ID, t)
				}
				if seen[key{ti, v}] {
					fail("op %d: value %d of %q written twice; values must be unique", o.ID, v, t)
				}
				seen[key{ti, v}] = true
			}
		}
	}
	observed := map[key]int64{}
	for _, o := range ops {
		if o.Kind != "read" || o.Status != "ok" {
			continue
		}
		skipped := map[string]bool{}
		for _, t := range o.Skipped {
			skipped[t] = true
		}
		for t, v := range o.Reads {
			if skipped[t] {
				continue
			}
			if r, ok := observed[key{idx[t], v}]; !ok || o.Ret < r {
				observed[key{idx[t], v}] = o.Ret
			}
		}
	}
	var h []porcupine.Operation
	for _, o := range ops {
		switch o.Kind {
		case "write":
			in := input{write: true}
			for t, v := range o.Writes {
				in.mask |= 1 << idx[t]
				in.vals[idx[t]] = v
			}
			ret := o.Ret
			switch o.Status {
			case "ok":
			case "failed":
				sum.DroppedFailed++
				continue
			case "unknown":
				ret = math.MaxInt64
				for t, v := range o.Writes {
					if r, ok := observed[key{idx[t], v}]; ok && r < ret {
						ret = r
					}
				}
				if ret == math.MaxInt64 {
					sum.DroppedUnobserved++
					continue
				}
				sum.PendingWrites++
			default:
				fail("op %d: status %q", o.ID, o.Status)
			}
			sum.Writes++
			h = append(h, porcupine.Operation{ClientId: o.Client, Input: in, Call: o.Call, Output: output{}, Return: ret})
		case "read":
			if o.Status != "ok" {
				sum.DroppedFailed++
				continue
			}
			var out output
			for i := range out.vals {
				out.vals[i] = 0
			}
			if len(o.Reads) != len(hdr.Types) {
				fail("op %d: a read must return every register", o.ID)
			}
			for t, v := range o.Reads {
				out.vals[idx[t]] = v
			}
			for _, t := range o.Skipped {
				out.skip |= 1 << idx[t]
			}
			sum.Reads++
			h = append(h, porcupine.Operation{ClientId: o.Client, Input: input{}, Call: o.Call, Output: out, Return: o.Ret})
		default:
			fail("op %d: kind %q", o.ID, o.Kind)
		}
	}
	sort.Slice(h, func(i, j int) bool { return h[i].Call < h[j].Call })
	sum.Checked = len(h)
	return h, sum
}

func contains(list []string, name string) bool {
	for _, n := range list {
		if n == name {
			return true
		}
	}
	return false
}

func initialState(hdr Header) state {
	var st state
	for i, t := range hdr.Types {
		st[i] = hdr.Initial[t]
	}
	return st
}

func configModel(n int, names []string, init state) porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} { return init },
		Step: func(s, in, out interface{}) (bool, interface{}) {
			st, i, o := s.(state), in.(input), out.(output)
			if i.write {
				for t := 0; t < n; t++ {
					if i.mask&(1<<t) != 0 {
						st[t] = i.vals[t]
					}
				}
				return true, st
			}
			for t := 0; t < n; t++ {
				if o.skip&(1<<t) == 0 && st[t] != o.vals[t] {
					return false, st
				}
			}
			return true, st
		},
		Equal: func(a, b interface{}) bool { return a.(state) == b.(state) },
		Hash: func(s interface{}) uint64 {
			st := s.(state)
			h := fnv.New64a()
			var b [4]byte
			for _, v := range st {
				b[0], b[1], b[2], b[3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
				h.Write(b[:])
			}
			return h.Sum64()
		},
		DescribeOperation: func(in, out interface{}) string {
			i, o := in.(input), out.(output)
			var parts []string
			for t := 0; t < n; t++ {
				if i.write {
					if i.mask&(1<<t) != 0 {
						parts = append(parts, fmt.Sprintf("%s=%d", names[t], i.vals[t]))
					}
				} else if o.skip&(1<<t) != 0 {
					parts = append(parts, names[t]+"=skipped")
				} else {
					parts = append(parts, fmt.Sprintf("%s=%d", names[t], o.vals[t]))
				}
			}
			if i.write {
				return "write{" + strings.Join(parts, ",") + "}"
			}
			return "read→{" + strings.Join(parts, ",") + "}"
		},
		DescribeState: func(s interface{}) string {
			st := s.(state)
			var parts []string
			for t := 0; t < n; t++ {
				parts = append(parts, fmt.Sprintf("%s=%d", names[t], st[t]))
			}
			return "{" + strings.Join(parts, ",") + "}"
		},
	}
}
