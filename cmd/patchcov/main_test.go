package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseProfileLine(t *testing.T) {
	const mod = "github.com/colingraydon/continuum"

	file, b, ok := parseProfileLine(mod+"/api/handlers.go:100.34,104.3 2 7", mod)
	if !ok {
		t.Fatal("expected a parse")
	}
	if file != "api/handlers.go" {
		t.Errorf("file = %q, want api/handlers.go (module prefix stripped)", file)
	}
	if b.start != 100 || b.end != 104 {
		t.Errorf("range = %d-%d, want 100-104", b.start, b.end)
	}
	if !b.covered {
		t.Error("count 7 should be covered")
	}

	if _, b, _ := parseProfileLine(mod+"/api/handlers.go:100.34,104.3 2 0", mod); b.covered {
		t.Error("count 0 should be uncovered")
	}
	for _, bad := range []string{"", "garbage", "no/colon 1 2", mod + "/a.go:1,2 3 4"} {
		if _, _, ok := parseProfileLine(bad, mod); ok {
			t.Errorf("parsed malformed line %q", bad)
		}
	}
}

func TestParseDiffHunks(t *testing.T) {
	diff := `diff --git a/api/handlers.go b/api/handlers.go
--- a/api/handlers.go
+++ b/api/handlers.go
@@ -10,0 +11,3 @@ func foo() {
+one
+two
+three
@@ -40 +43 @@ func bar() {
+replaced
diff --git a/internal/ring/ring.go b/internal/ring/ring.go
--- a/internal/ring/ring.go
+++ b/internal/ring/ring.go
@@ -5,0 +6 @@
+single
`
	got := parseDiff(diff)

	want := map[string][]int{
		"api/handlers.go":       {11, 12, 13, 43},
		"internal/ring/ring.go": {6},
	}
	for file, lines := range want {
		if got[file] == nil {
			t.Fatalf("no changed lines for %s", file)
		}
		if len(got[file]) != len(lines) {
			t.Errorf("%s: %d changed lines, want %d (%v)", file, len(got[file]), len(lines), got[file])
		}
		for _, l := range lines {
			if !got[file][l] {
				t.Errorf("%s: line %d missing", file, l)
			}
		}
	}
}

// TestParseHunkOmittedCount pins the unified-diff rule that a hunk header with
// no count means exactly one line. Reading it as zero would silently drop
// single-line changes from the measurement - the ones most likely to be an
// untested bug fix.
func TestParseHunkOmittedCount(t *testing.T) {
	start, count, ok := parseHunk("@@ -40 +43 @@ func bar() {")
	if !ok || start != 43 || count != 1 {
		t.Errorf("parseHunk(omitted count) = (%d, %d, %v), want (43, 1, true)", start, count, ok)
	}
	start, count, ok = parseHunk("@@ -10,0 +11,3 @@")
	if !ok || start != 11 || count != 3 {
		t.Errorf("parseHunk = (%d, %d, %v), want (11, 3, true)", start, count, ok)
	}
	if _, _, ok := parseHunk("not a hunk"); ok {
		t.Error("parsed a non-hunk line")
	}
}

func TestScore(t *testing.T) {
	changed := map[string]map[int]bool{
		"api/handlers.go":       {10: true, 11: true, 20: true, 99: true},
		"cmd/continuum/main.go": {5: true}, // ignored prefix
	}
	blocks := map[string][]block{
		"api/handlers.go": {
			{start: 10, end: 11, covered: true},
			{start: 20, end: 20, covered: false},
			// line 99 falls in no block: not a statement, excluded from both sides
		},
		"cmd/continuum/main.go": {{start: 5, end: 5, covered: false}},
	}

	covered, total, uncovered := score(changed, blocks, []string{"cmd/"})

	if total != 3 {
		t.Errorf("total = %d, want 3 (line 99 has no statement, cmd/ ignored)", total)
	}
	if covered != 2 {
		t.Errorf("covered = %d, want 2", covered)
	}
	if len(uncovered) != 1 || uncovered[0] != "api/handlers.go:20" {
		t.Errorf("uncovered = %v, want [api/handlers.go:20]", uncovered)
	}
}

// TestScoreCountsOverlappingBlocksOnce pins the nesting rule: Go emits a block
// per branch, so an `if` inside a function body puts several blocks over one
// line. Counting per block would inflate both sides of the ratio and let a
// densely-branched file drown out a sparse one.
func TestScoreCountsOverlappingBlocksOnce(t *testing.T) {
	changed := map[string]map[int]bool{"a.go": {5: true}}
	blocks := map[string][]block{"a.go": {
		{start: 1, end: 10, covered: false},
		{start: 4, end: 6, covered: true},
	}}

	covered, total, _ := score(changed, blocks, nil)

	if total != 1 || covered != 1 {
		t.Errorf("got %d/%d, want 1/1 - one line, covered because some block over it ran", covered, total)
	}
}

// TestScoreSkipsFilesAbsentFromProfile covers test files and packages with no
// tests: they carry no statements in the profile, so they must not be counted
// as uncovered.
func TestScoreSkipsFilesAbsentFromProfile(t *testing.T) {
	changed := map[string]map[int]bool{"api/handlers_test.go": {1: true, 2: true}}

	covered, total, uncovered := score(changed, map[string][]block{}, nil)

	if total != 0 || covered != 0 || len(uncovered) != 0 {
		t.Errorf("got %d/%d uncovered=%v, want 0/0 with none listed", covered, total, uncovered)
	}
}

func TestReadProfile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "coverage.out")
	const mod = "github.com/colingraydon/continuum"
	content := "mode: atomic\n" +
		mod + "/api/handlers.go:10.1,12.2 2 1\n" +
		mod + "/api/handlers.go:20.1,22.2 1 0\n" +
		"\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	blocks, err := readProfile(path, mod)
	if err != nil {
		t.Fatalf("readProfile: %v", err)
	}
	got := blocks["api/handlers.go"]
	if len(got) != 2 {
		t.Fatalf("got %d blocks, want 2", len(got))
	}
	if !got[0].covered || got[1].covered {
		t.Errorf("coverage flags = %v/%v, want true/false", got[0].covered, got[1].covered)
	}
}

func TestModulePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "go.mod")
	if err := os.WriteFile(path, []byte("module github.com/example/thing\n\ngo 1.26\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := modulePath(path)
	if err != nil || got != "github.com/example/thing" {
		t.Errorf("modulePath = %q (err %v), want github.com/example/thing", got, err)
	}

	bad := filepath.Join(dir, "bad.mod")
	if err := os.WriteFile(bad, []byte("go 1.26\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := modulePath(bad); err == nil {
		t.Error("expected an error for a go.mod with no module directive")
	}
}

func TestSplitPrefixes(t *testing.T) {
	got := splitPrefixes(" cmd/ , internal/mock/ ,, ")
	if len(got) != 2 || got[0] != "cmd/" || got[1] != "internal/mock/" {
		t.Errorf("splitPrefixes = %v, want [cmd/ internal/mock/]", got)
	}
	if got := splitPrefixes(""); len(got) != 0 {
		t.Errorf("splitPrefixes(\"\") = %v, want empty", got)
	}
}

// TestParseDiffAgainstRealGit feeds genuine `git diff` output through the
// parser in a throwaway repository, rather than trusting a hand-written
// fixture to match what git actually emits.
//
// It also pins the **three-dot** semantics the `patch-coverage` make target
// relies on: diffing against the merge base means a commit landing on the base
// branch after this one forked is not attributed to it. Two-dot would blame
// this change for someone else's untested code. The flags here must stay in
// step with the Makefile, which owns the invocation in CI.
func TestParseDiffAgainstRealGit(t *testing.T) {
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@e",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@e")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	run("init", "-q", "-b", "main")
	write("a.go", "package a\n\nfunc A() {}\n")
	run("add", ".")
	run("commit", "-qm", "base")

	baseSHA := gitOut(t, dir, "rev-parse", "HEAD")

	// Our branch adds a line.
	run("checkout", "-q", "-b", "feature")
	write("a.go", "package a\n\nfunc A() {}\n\nfunc B() {}\n")
	run("add", ".")
	run("commit", "-qm", "feature")

	// Meanwhile the base branch moves on independently.
	run("checkout", "-q", "main")
	write("other.go", "package a\n\nfunc Other() {}\n")
	run("add", ".")
	run("commit", "-qm", "unrelated")
	run("checkout", "-q", "feature")

	raw := gitOut(t, dir, "diff", "--unified=0", baseSHA+"...HEAD", "--", "*.go")
	changed := parseDiff(raw)

	if len(changed["a.go"]) == 0 {
		t.Error("expected the added line in a.go to be reported")
	}
	if _, blamed := changed["other.go"]; blamed {
		t.Error("other.go landed on the base branch after the fork; three-dot diff must not attribute it to us")
	}
}

func gitOut(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}
