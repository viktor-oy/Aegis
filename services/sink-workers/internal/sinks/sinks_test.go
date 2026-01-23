package sinks

import "testing"

func TestArchiveKey(t *testing.T) {
	got := ArchiveKey("node/a", "inc/1")
	want := "postmortems/node_a/inc_1/postmortem.md"
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

