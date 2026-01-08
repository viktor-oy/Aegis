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

