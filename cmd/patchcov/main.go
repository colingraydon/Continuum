// Command patchcov gates diff coverage in CI.
//
// Codecov reports patch coverage but cannot gate it here: codecov.yml marks the
// patch status informational, so `codecov/patch` always reports success no
// matter what the number is. Requiring that context therefore blocks a merge
// only when Codecov fails to answer at all - an availability dependency with no
// signal in it. This computes the same measure inside CI, where the verdict is
// reproducible locally and the logs are readable.
//
// It measures statements, not lines: a coverage profile records statement
// blocks, so a changed line carrying no statement (a comment, an import, a bare
// brace) is neither covered nor uncovered and is excluded from both sides of
// the ratio - which is why a docs-heavy diff reports "no coverable statements"
// rather than 0%.
//
// The unified diff arrives on stdin rather than being produced here, so this
// binary never shells out and has no dependency on what PATH resolves. The
// `make patch-coverage` target owns the git invocation, which keeps the
// merge-base semantics in one place for both CI and local runs.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

func main() {
	var (
		profile = flag.String("profile", "coverage.out", "go coverage profile")
		module  = flag.String("module", "", "module path to strip from profile paths (default: read go.mod)")
		minPct  = flag.Float64("min", 80, "minimum percent of changed statements covered")
		ignore  = flag.String("ignore", "cmd/", "comma-separated path prefixes to exclude")
	)
	flag.Parse()

	mod := *module
	if mod == "" {
		var err error
		if mod, err = modulePath("go.mod"); err != nil {
			fatal("read module path: %v", err)
		}
	}

	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fatal("read diff from stdin: %v", err)
	}
	diff := parseDiff(string(raw))
	blocks, err := readProfile(*profile, mod)
	if err != nil {
		fatal("read profile %s: %v", *profile, err)
	}

	prefixes := splitPrefixes(*ignore)
	covered, total, uncovered := score(diff, blocks, prefixes)

	if total == 0 {
		fmt.Println("patch coverage: no coverable statements in this diff - nothing to gate")
		return
	}

	pct := 100 * float64(covered) / float64(total)
	fmt.Printf("patch coverage: %.2f%% (%d/%d changed statements)\n", pct, covered, total)

	if len(uncovered) > 0 {
		fmt.Println("\nUncovered changed lines:")
		for _, u := range uncovered {
			fmt.Printf("  %s\n", u)
		}
	}
	if pct+1e-9 < *minPct {
		fmt.Printf("\nFAIL: patch coverage %.2f%% is below the %.2f%% floor.\n", pct, *minPct)
		os.Exit(1)
	}
	fmt.Printf("\nOK: at or above the %.2f%% floor.\n", *minPct)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "patchcov: "+format+"\n", args...)
	os.Exit(2)
}

func splitPrefixes(raw string) []string {
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// block is one statement block from a coverage profile.
type block struct {
	start, end int
	covered    bool
}

// modulePath reads the module line from a go.mod.
func modulePath(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.TrimSpace(rest), nil
		}
	}
	return "", fmt.Errorf("no module directive in %s", path)
}

// readProfile parses a Go coverage profile into per-file statement blocks,
// keyed by repo-relative path. A block is covered if any profile entry for it
// has a non-zero count: with -covermode=atomic the same block can appear once
// per test binary, and one execution anywhere makes it covered.
func readProfile(path, module string) (map[string][]block, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	out := make(map[string][]block)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		file, b, ok := parseProfileLine(line, module)
		if !ok {
			continue
		}
		out[file] = append(out[file], b)
	}
	return out, sc.Err()
}

// parseProfileLine parses "path/file.go:12.34,15.2 3 1" into a block.
func parseProfileLine(line, module string) (string, block, bool) {
	colon := strings.LastIndex(line, ":")
	if colon < 0 {
		return "", block{}, false
	}
	file := strings.TrimPrefix(line[:colon], module+"/")

	fields := strings.Fields(line[colon+1:])
	if len(fields) != 3 {
		return "", block{}, false
	}
	rangePart, countPart := fields[0], fields[2]

	startEnd := strings.Split(rangePart, ",")
	if len(startEnd) != 2 {
		return "", block{}, false
	}
	start, ok := lineOf(startEnd[0])
	if !ok {
		return "", block{}, false
	}
	end, ok := lineOf(startEnd[1])
	if !ok {
		return "", block{}, false
	}
	count, err := strconv.Atoi(countPart)
	if err != nil {
		return "", block{}, false
	}
	return file, block{start: start, end: end, covered: count > 0}, true
}

// lineOf takes the line number from a "line.column" pair.
func lineOf(s string) (int, bool) {
	dot := strings.Index(s, ".")
	if dot < 0 {
		return 0, false
	}
	n, err := strconv.Atoi(s[:dot])
	return n, err == nil
}

// parseDiff extracts added line numbers per file from unified=0 diff output.
func parseDiff(diff string) map[string]map[int]bool {
	changed := make(map[string]map[int]bool)
	var file string
	for _, line := range strings.Split(diff, "\n") {
		if target, ok := strings.CutPrefix(line, "+++ b/"); ok {
			file = target
			if file == "/dev/null" {
				file = ""
			}
			continue
		}
		if file != "" && strings.HasPrefix(line, "@@") {
			recordHunk(changed, file, line)
		}
	}
	return changed
}

// recordHunk marks the lines a single hunk header adds to file.
func recordHunk(changed map[string]map[int]bool, file, header string) {
	start, count, ok := parseHunk(header)
	if !ok {
		return
	}
	if changed[file] == nil {
		changed[file] = make(map[int]bool)
	}
	for i := range count {
		changed[file][start+i] = true
	}
}

// parseHunk reads the new-file line range from "@@ -a,b +c,d @@".
func parseHunk(line string) (start, count int, ok bool) {
	plus := strings.Index(line, "+")
	if plus < 0 {
		return 0, 0, false
	}
	rest := line[plus+1:]
	if sp := strings.IndexAny(rest, " \t"); sp >= 0 {
		rest = rest[:sp]
	}
	nums := strings.SplitN(rest, ",", 2)
	start, err := strconv.Atoi(nums[0])
	if err != nil {
		return 0, 0, false
	}
	count = 1 // an omitted count means exactly one line
	if len(nums) == 2 {
		if count, err = strconv.Atoi(nums[1]); err != nil {
			return 0, 0, false
		}
	}
	return start, count, true
}

// score counts changed statements and their coverage. A changed line is scored
// once even when several blocks span it: nested blocks would otherwise
// double-count the same line, and a line is covered if any block covering it
// ran.
func score(changed map[string]map[int]bool, blocks map[string][]block, ignore []string) (covered, total int, uncovered []string) {
	for file, lines := range changed {
		if hasPrefix(file, ignore) {
			continue
		}
		fileBlocks, ok := blocks[file]
		if !ok {
			continue // not in the profile: a test file, or a package with no tests
		}
		for line := range lines {
			state, inBlock := lineState(line, fileBlocks)
			if !inBlock {
				continue // no statement on this line
			}
			total++
			if state {
				covered++
			} else {
				uncovered = append(uncovered, fmt.Sprintf("%s:%d", file, line))
			}
		}
	}
	sort.Strings(uncovered)
	return covered, total, uncovered
}

// lineState reports whether any block covers this line and whether it ran.
func lineState(line int, blocks []block) (covered, inBlock bool) {
	for _, b := range blocks {
		if line < b.start || line > b.end {
			continue
		}
		inBlock = true
		if b.covered {
			return true, true
		}
	}
	return false, inBlock
}

func hasPrefix(path string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}
