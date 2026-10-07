package main

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func provenanceTestDir(t *testing.T) string {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(old); err != nil {
			t.Error(err)
		}
	})
	return dir
}

func fakeProvenanceGit(t *testing.T, body string) {
	t.Helper()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
}

func TestGitStateUnavailableWithoutLocalMetadata(t *testing.T) {
	dir := provenanceTestDir(t)
	// Even a Git command capable of returning a parent repository's HEAD
	// must not be used for an export lacking its own .git entry.
	marker := filepath.Join(dir, "git-was-called")
	fakeProvenanceGit(t, "echo called > '"+marker+"'\necho parent-commit\n")
	commit, dirty, err := gitState()
	if !errors.Is(err, errGitUnavailable) || commit != "" || dirty != nil {
		t.Fatalf("got commit=%q dirty=%v err=%v", commit, dirty, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Git was consulted without local metadata")
	}
}

func TestGitStateUnavailableWithoutExecutable(t *testing.T) {
	provenanceTestDir(t)
	if err := os.Mkdir(".git", 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", t.TempDir())
	commit, dirty, err := gitState()
	if !errors.Is(err, errGitUnavailable) || commit != "" || dirty != nil {
		t.Fatalf("got commit=%q dirty=%v err=%v", commit, dirty, err)
	}
}

func TestGitStateKeepsKnownStateAndQueryErrors(t *testing.T) {
	for _, tc := range []struct {
		name, script   string
		dirty, wantErr bool
	}{
		{"clean", "case \"$1\" in rev-parse) echo 0123456789abcdef0123456789abcdef01234567;; status) exit 0;; esac\n", false, false},
		{"dirty", "case \"$1\" in rev-parse) echo 0123456789abcdef0123456789abcdef01234567;; status) echo ' M main.go';; esac\n", true, false},
		{"revision failure", "exit 128\n", false, true},
		{"empty revision", "exit 0\n", false, true},
		{"status failure", "case \"$1\" in rev-parse) echo 0123456789abcdef0123456789abcdef01234567;; status) exit 128;; esac\n", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provenanceTestDir(t)
			// Worktrees have a .git file rather than a directory.
			if err := os.WriteFile(".git", []byte("gitdir: /test/worktree\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			fakeProvenanceGit(t, tc.script)
			commit, dirty, err := gitState()
			if tc.wantErr {
				if err == nil || errors.Is(err, errGitUnavailable) || dirty != nil {
					t.Fatalf("Git failure was hidden: commit=%q dirty=%v err=%v", commit, dirty, err)
				}
				return
			}
			if err != nil || commit != "0123456789abcdef0123456789abcdef01234567" || dirty == nil || *dirty != tc.dirty {
				t.Fatalf("got commit=%q dirty=%v err=%v", commit, dirty, err)
			}
		})
	}
}

// Exercise main's actual manifest startup, not --help. An invalid scenario
// deliberately stops immediately AFTER initManifest, before any database path.
// The child has neither Git nor Docker on PATH and no .git directory.
func TestBenchmarkStartupWithoutGit(t *testing.T) {
	if os.Getenv("UUID_BENCHMARK_TEST_NO_GIT") == "1" {
		flag.CommandLine = flag.NewFlagSet("uuid-benchmark", flag.ExitOnError)
		os.Args = []string{"uuid-benchmark", "-database=postgres", "-scenario=offline-provenance-check", "-output=campaign.csv"}
		main()
		os.Exit(0)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	t.Setenv("PATH", t.TempDir())
	cmd := exec.Command(executable, "-test.run=^TestBenchmarkStartupWithoutGit$")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "UUID_BENCHMARK_TEST_NO_GIT=1")
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(out), "Invalid scenario: offline-provenance-check") {
		t.Fatalf("startup did not reach scenario dispatch: %v\n%s", err, out)
	}
	if strings.Count(string(out), "Warning: Git provenance unavailable") != 1 {
		t.Fatalf("expected exactly one warning:\n%s", out)
	}
	data, err := os.ReadFile(filepath.Join(dir, "campaign.csv.meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got["git_provenance"] != "unavailable" || got["commit"] != "" {
		t.Fatalf("incorrect unavailable provenance: %s", data)
	}
	if value, exists := got["working_tree_dirty"]; !exists || value != nil {
		t.Fatalf("unknown dirty state must be explicit null: %s", data)
	}
	if hash, ok := got["orchestrator_md5"].(string); !ok || len(hash) != 32 {
		t.Fatalf("binary hash missing: %s", data)
	}
}
