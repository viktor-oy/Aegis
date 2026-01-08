package hashring

import "testing"

func TestFingerprintIgnoresInputOrder(t *testing.T) {
	left := []Member{{ID: "cp-b", Address: "b:50051"}, {ID: "cp-a", Address: "a:50051"}}
	right := []Member{{ID: "cp-a", Address: "a:50051"}, {ID: "cp-b", Address: "b:50051"}}
	if Fingerprint(left) != Fingerprint(right) {
		t.Fatal("fingerprint must be deterministic regardless of input order")
	}
}

func TestOwnerIsStable(t *testing.T) {
	ring, err := New([]Member{
		{ID: "cp-a", Address: "a:50051"},
		{ID: "cp-b", Address: "b:50051"},
	}, 64)
	if err != nil {
		t.Fatal(err)
	}
	owner1, ok := ring.Owner("worker-17")
	if !ok {
		t.Fatal("missing owner")
	}
	owner2, _ := ring.Owner("worker-17")
	if owner1 != owner2 {
		t.Fatalf("owner changed for stable ring: %v != %v", owner1, owner2)
	}
}

func TestNewRingErrorsOnEmptyMembers(t *testing.T) {
	_, err := New(nil, 64)
	if err == nil {
		t.Fatal("expected error for empty members")
	}
}

func TestNewRingErrorsOnZeroVirtualNodes(t *testing.T) {
	_, err := New([]Member{{ID: "cp-a", Address: "a:50051"}}, 0)
	if err == nil {
		t.Fatal("expected error for zero virtual nodes")
	}
}

func TestNewRingErrorsOnNegativeVirtualNodes(t *testing.T) {
	_, err := New([]Member{{ID: "cp-a", Address: "a:50051"}}, -1)
	if err == nil {
		t.Fatal("expected error for negative virtual nodes")
	}
}

func TestRingDeduplicatesMembers(t *testing.T) {
	ring, err := New([]Member{
		{ID: "cp-a", Address: "a:50051"},
		{ID: "cp-a", Address: "a:50051"}, // duplicate
		{ID: "cp-b", Address: "b:50051"},
	}, 64)
	if err != nil {
		t.Fatal(err)
	}
	// Should still work correctly with deduplicated members.
	if _, ok := ring.Owner("worker-1"); !ok {
		t.Fatal("expected owner from deduplicated ring")
	}
}

func TestRingSkipsInvalidMembers(t *testing.T) {
	ring, err := New([]Member{
		{ID: "", Address: "a:50051"},           // empty ID
		{ID: "cp-a", Address: ""},              // empty address
		{ID: "cp-valid", Address: "v:50051"},   // valid
	}, 64)
	if err != nil {
		t.Fatal(err)
	}
	owner, ok := ring.Owner("worker-1")
	if !ok {
		t.Fatal("expected owner from ring with valid member")
	}
	if owner.ID != "cp-valid" {
		t.Fatalf("expected cp-valid, got %s", owner.ID)
	}
}

func TestNilRingOwnerReturnsFalse(t *testing.T) {
	var ring *Ring
	_, ok := ring.Owner("worker-1")
	if ok {
		t.Fatal("nil ring should return false")
	}
}

func TestFingerprintDiffersForDifferentMembers(t *testing.T) {
	a := []Member{{ID: "cp-a", Address: "a:50051"}}
	b := []Member{{ID: "cp-b", Address: "b:50051"}}
	if Fingerprint(a) == Fingerprint(b) {
		t.Fatal("different members should produce different fingerprints")
	}
}

