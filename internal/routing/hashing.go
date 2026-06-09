package routing

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type HashRing struct {
	mu       sync.RWMutex
	replicas int
	ring     []uint32
	hashMap  map[uint32]string
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		replicas: replicas,
		hashMap:  make(map[uint32]string),
	}
}

func (h *HashRing) hashFunction(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (h *HashRing) Add(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.replicas; i++ {
		vNodeName := node + "#" + strconv.Itoa(i)
		hash := h.hashFunction(vNodeName)

		h.ring = append(h.ring, hash)
		h.hashMap[hash] = node
	}

	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})
}

func (h *HashRing) Remove(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.replicas; i++ {
		vNodeName := node + "#" + strconv.Itoa(i)
		hash := h.hashFunction(vNodeName)

		delete(h.hashMap, hash)

		for idx, v := range h.ring {
			if v == hash {
				h.ring = append(h.ring[:idx], h.ring[idx+1:]...)
				break
			}
		}
	}
}

func (h *HashRing) Get(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return ""
	}

	hash := h.hashFunction(key)

	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	if idx == len(h.ring) {
		idx = 0
	}

	return h.hashMap[h.ring[idx]]
}

func (h *HashRing) GetReplicas(key string, count int) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return nil
	}

	hash := h.hashFunction(key)
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	if idx == len(h.ring) {
		idx = 0
	}

	replicas := make([]string, 0, count)
	seen := make(map[string]bool)
	
	for len(replicas) < count && len(seen) < len(h.hashMap) {
		nodeAddr := h.hashMap[h.ring[idx]]
		if !seen[nodeAddr] { 
			seen[nodeAddr] = true
			replicas = append(replicas, nodeAddr)
		}		
		idx++
		if idx == len(h.ring) {
			idx = 0 
		}
	}

	return replicas
}