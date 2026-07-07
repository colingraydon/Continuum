// Package histcheck checks recorded client histories for per-key
// linearizability using porcupine. It models the CAS write path: a register
// whose writes are compare-and-set operations conditioned on the exact value
// the writer last read. The default sloppy-quorum path is eventually
// consistent by design and is not expected to linearize; CAS requests
// (?cas=true) serialize through the key's primary replica and should, so a
// violation in a pure-CAS history is a real consistency bug (typically a
// forked or lost update across primary failover).
//
// Operations with unknown outcomes — timeouts and 5xx responses, where the
// mutation may or may not have committed — are handled with the standard
// open-interval technique: the operation's return time is set past the end of
// the history, so the checker may linearize it at any point after its call,
// including after every observed read, which is observationally equivalent to
// it never taking effect.
package histcheck

import (
	"fmt"
	"time"

	"github.com/anishathalye/porcupine"
)

// OpKind distinguishes the two operations a checked client performs.
type OpKind uint8

const (
	// Read is a coordinator GET for a key.
	Read OpKind = iota
	// CASPut is a conditional write (?cas=true): apply the new value only
	// if the current state is exactly the value the writer last read.
	CASPut
)

// Status is the observed outcome of a CASPut.
type Status uint8

const (
	// StatusOK means the write was acknowledged (204): it committed.
	StatusOK Status = iota
	// StatusConflict means the precondition failed (412): the store
	// guarantees no side effects.
	StatusConflict
	// StatusUnknown means the outcome is unknown (timeout or 5xx): the
	// write may have committed on the primary even though the client saw
	// an error, e.g. a 503 returned after the local commit because the
	// replica fan-out missed quorum.
	StatusUnknown
)

// Op is one recorded client operation. Call and Return are nanosecond
// timestamps on a single monotonic scale shared by every recorded operation
// (e.g. time.Since of a common base). The interval is closed: operations
// whose intervals overlap are concurrent.
type Op struct {
	Client int    // recording client, for visualization lanes
	Key    string // histories are checked independently per key
	Kind   OpKind

	// CASPut input: write Value expecting the current state to be exactly
	// Expected ("" = key absent).
	Expected string
	Value    string

	// CASPut output.
	Status Status

	// Read output: the value returned, or absent, or a sibling conflict.
	// A conflict can never appear in a linearizable pure-CAS history (CAS
	// refuses to create siblings), so a conflicting read always fails the
	// check and pinpoints a forked write.
	ReadValue string
	Found     bool
	Conflict  bool

	Call   int64
	Return int64
}

// input and output split an Op into the halves porcupine's Step consumes.
type input struct {
	Kind     OpKind
	Key      string
	Expected string
	Value    string
}

type output struct {
	Status   Status
	Value    string
	Found    bool
	Conflict bool
}

// casRegister is the sequential specification: per key, a register holding
// one value ("" = absent) whose writes are compare-and-set.
var casRegister = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		for _, op := range history {
			key := op.Input.(input).Key
			byKey[key] = append(byKey[key], op)
		}
		parts := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			parts = append(parts, ops)
		}
		return parts
	},
	Init: func() interface{} { return "" },
	Step: func(state, in, out interface{}) (bool, interface{}) {
		cur := state.(string)
		i := in.(input)
		o := out.(output)
		switch i.Kind {
		case Read:
			if o.Conflict {
				return false, state
			}
			if o.Found {
				return cur == o.Value, state
			}
			return cur == "", state
		case CASPut:
			switch o.Status {
			case StatusOK:
				return cur == i.Expected, i.Value
			case StatusConflict:
				return cur != i.Expected, state
			default: // StatusUnknown: may or may not have applied.
				if cur == i.Expected {
					return true, i.Value
				}
				return true, state
			}
		}
		return false, state
	},
	DescribeOperation: func(in, out interface{}) string {
		i := in.(input)
		o := out.(output)
		switch i.Kind {
		case Read:
			switch {
			case o.Conflict:
				return fmt.Sprintf("read(%s) -> CONFLICT", i.Key)
			case !o.Found:
				return fmt.Sprintf("read(%s) -> absent", i.Key)
			default:
				return fmt.Sprintf("read(%s) -> %s", i.Key, o.Value)
			}
		default:
			verdict := map[Status]string{StatusOK: "ok", StatusConflict: "412", StatusUnknown: "?"}[o.Status]
			return fmt.Sprintf("cas(%s, %q -> %q) -> %s", i.Key, i.Expected, i.Value, verdict)
		}
	},
	DescribeState: func(state interface{}) string {
		if s := state.(string); s != "" {
			return s
		}
		return "absent"
	},
}

// Result is the outcome of a history check.
type Result struct {
	result porcupine.CheckResult
	info   porcupine.LinearizationInfo
	ops    int
}

// Linearizable reports whether the checker proved the history linearizable.
func (r Result) Linearizable() bool { return r.result == porcupine.Ok }

// Undecided reports whether the checker timed out before reaching a verdict.
func (r Result) Undecided() bool { return r.result == porcupine.Unknown }

// Ops returns the number of operations that were checked.
func (r Result) Ops() int { return r.ops }

// Visualize writes an interactive HTML rendering of the history and the
// checker's linearization attempt to path. Most useful on failure: the first
// non-linearizable operation is highlighted.
func (r Result) Visualize(path string) error {
	return porcupine.VisualizePath(casRegister, r.info, path)
}

// Check verifies a recorded history for per-key linearizability under the
// CAS-register model. timeout bounds the search (linearizability checking is
// NP-hard); an exceeded timeout yields an Undecided result. Operations with
// StatusUnknown have their return times pushed past the end of the history,
// so they may linearize anywhere after their call, or effectively never.
func Check(ops []Op, timeout time.Duration) Result {
	var maxTime int64
	for _, op := range ops {
		if op.Return > maxTime {
			maxTime = op.Return
		}
		if op.Call > maxTime {
			maxTime = op.Call
		}
	}
	history := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		ret := op.Return
		if op.Kind == CASPut && op.Status == StatusUnknown {
			ret = maxTime + 1
		}
		history = append(history, porcupine.Operation{
			ClientId: op.Client,
			Input:    input{Kind: op.Kind, Key: op.Key, Expected: op.Expected, Value: op.Value},
			Output:   output{Status: op.Status, Value: op.ReadValue, Found: op.Found, Conflict: op.Conflict},
			Call:     op.Call,
			Return:   ret,
		})
	}
	res, info := porcupine.CheckOperationsVerbose(casRegister, history, timeout)
	return Result{result: res, info: info, ops: len(ops)}
}
