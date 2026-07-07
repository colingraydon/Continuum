// Command histbisect pinpoints where a recorded operation history stops
// being linearizable. The fault and simulation harnesses dump the raw
// history as JSON next to the porcupine visualization when a check fails;
// this tool re-checks each key's subhistory and binary-searches the minimal
// failing prefix (by response order), printing the operations around the
// first one no sequential CAS register can explain.
//
//	go run ./cmd/histbisect /tmp/TestFault_..._history.json
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/colingraydon/continuum/internal/histcheck"
)

const checkTimeout = time.Minute

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: histbisect <history.json>")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var hist []histcheck.Op
	if err := json.Unmarshal(raw, &hist); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	byKey := map[string][]histcheck.Op{}
	for _, op := range hist {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	for key, ops := range byKey {
		sort.Slice(ops, func(i, j int) bool { return ops[i].Return < ops[j].Return })
		if histcheck.Check(ops, checkTimeout).Linearizable() {
			fmt.Printf("%s: linearizable (%d ops)\n", key, len(ops))
			continue
		}
		// Binary search the minimal failing prefix: prefixes of a
		// linearizable history are linearizable, so failure is monotone in
		// the prefix length.
		lo, hi := 1, len(ops)
		for lo < hi {
			mid := (lo + hi) / 2
			if histcheck.Check(ops[:mid], checkTimeout).Linearizable() {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		fmt.Printf("%s: fails at prefix %d/%d; operations around the break:\n", key, lo, len(ops))
		for i := max(0, lo-10); i < lo; i++ {
			fmt.Printf("  %s\n", describe(ops[i]))
		}
	}
}

func describe(op histcheck.Op) string {
	ms := func(ns int64) string { return fmt.Sprintf("%9.1fms", float64(ns)/1e6) }
	if op.Kind == histcheck.Read {
		res := op.ReadValue
		switch {
		case op.Conflict:
			res = "CONFLICT"
		case !op.Found:
			res = "absent"
		}
		return fmt.Sprintf("[%s,%s] c%d read -> %s", ms(op.Call), ms(op.Return), op.Client, res)
	}
	status := map[histcheck.Status]string{
		histcheck.StatusOK:       "ok",
		histcheck.StatusConflict: "412",
		histcheck.StatusUnknown:  "unknown",
	}[op.Status]
	expected := op.Expected
	if expected == "" {
		expected = "absent"
	}
	return fmt.Sprintf("[%s,%s] c%d cas(%s -> %s) = %s", ms(op.Call), ms(op.Return), op.Client, expected, op.Value, status)
}
