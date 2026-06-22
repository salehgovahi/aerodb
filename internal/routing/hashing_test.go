package routing

import (
	"testing"
)

func TestHashRing_AddAndGet(t *testing.T) {
	ring := NewHashRing(3)
	
	node1 := "127.0.0.1:8000"
	node2 := "127.0.0.1:8001"
	
	ring.Add(node1)
	ring.Add(node2)

	key := "user:100"
	expectedNode := ring.Get(key)

	if expectedNode != node1 && expectedNode != node2 {
		t.Fatalf("Expected a valid node, got: %s", expectedNode)
	}
	if ring.Get(key) != expectedNode {
		t.Errorf("HashRing is inconsistent! Expected %s", expectedNode)
	}
}

func TestHashRing_Remove(t *testing.T) {
	ring := NewHashRing(3)
	node1 := "127.0.0.1:8000"
	
	ring.Add(node1)
	ring.Remove(node1)

	if node := ring.Get("some_key"); node != "" {
		t.Errorf("Expected empty string for empty ring, got: %s", node)
	}
}