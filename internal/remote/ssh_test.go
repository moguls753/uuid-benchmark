package remote

import (
	"slices"
	"testing"
)

func TestBuildShellCommand(t *testing.T) {
	cases := map[string]struct {
		argv []string
		want string
	}{
		// buildShellCommand always quotes every element. Single-word
		// elements like "nodetool" become 'nodetool' on the wire — the
		// remote shell still parses them back identically.
		"simple argv":         {[]string{"nodetool", "status"}, "'nodetool' 'status'"},
		"spaces in element":   {[]string{"echo", "hello world"}, "'echo' 'hello world'"},
		"embedded single":     {[]string{"echo", "it's"}, `'echo' 'it'\''s'`},
		"shell metacharacter": {[]string{"echo", "$HOME"}, "'echo' '$HOME'"},
		// Pins the helper's contract on empty input. Exec rejects this
		// before calling, but the helper itself must return "" rather
		// than panic so an accidental inlined caller fails loudly later
		// (Session.Run("") returns an error from the remote shell).
		"empty argv": {[]string{}, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := buildShellCommand(tc.argv)
			if got != tc.want {
				t.Errorf("buildShellCommand(%v): got %q want %q", tc.argv, got, tc.want)
			}
		})
	}
}

func TestShellQuote(t *testing.T) {
	cases := map[string]string{
		"":            `''`,
		"abc":         `'abc'`,
		"hello world": `'hello world'`,
		"it's":        `'it'\''s'`,
		"$HOME":       `'$HOME'`,
	}
	for in, want := range cases {
		if got := shellQuote(in); got != want {
			t.Errorf("shellQuote(%q): got %q want %q", in, got, want)
		}
	}
}

func TestHasPort(t *testing.T) {
	cases := map[string]bool{
		"taurus5":      false,
		"taurus5:22":   true,
		"127.0.0.1":    false,
		"127.0.0.1:22": true,
		"":             false,
		// Bracketed IPv6 forms work; raw IPv6 literals are not supported
		// (see hasPort doc) — pinned here so the limitation is testable.
		"[::1]:22": true,
	}
	for in, want := range cases {
		if got := hasPort(in); got != want {
			t.Errorf("hasPort(%q): got %v want %v", in, got, want)
		}
	}
}

func TestBuildScpArgs(t *testing.T) {
	cases := map[string]struct {
		keyPath string
		user    string
		host    string
		src     string
		dst     string
		want    []string
	}{
		// src != dst here so a future refactor that swapped the two
		// positional args in buildScpArgs would fail this test.
		"with key path": {
			keyPath: "/home/u/.ssh/id_ed25519",
			user:    "benchuser",
			host:    "taurus5",
			src:     "/local/workload",
			dst:     "/opt/workload",
			want: []string{
				"-i", "/home/u/.ssh/id_ed25519",
				"-o", "StrictHostKeyChecking=no",
				"-o", "UserKnownHostsFile=/dev/null",
				"-o", "BatchMode=yes",
				"/local/workload",
				"benchuser@taurus5:/opt/workload",
			},
		},
		"without key path falls back to ssh-agent / default keys": {
			keyPath: "",
			user:    "benchuser",
			host:    "taurus5",
			src:     "/tmp/workload",
			dst:     "/tmp/workload",
			want: []string{
				"-o", "StrictHostKeyChecking=no",
				"-o", "UserKnownHostsFile=/dev/null",
				"-o", "BatchMode=yes",
				"/tmp/workload",
				"benchuser@taurus5:/tmp/workload",
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := buildScpArgs(tc.keyPath, tc.user, tc.host, tc.src, tc.dst)
			if !slices.Equal(got, tc.want) {
				t.Errorf("buildScpArgs: got %v want %v", got, tc.want)
			}
		})
	}
}
