package dlock

import (
	"sync"
	"time"
)

type LockManager struct {
	mu    sync.Mutex
	locks map[string]time.Time
}

func NewLockManager() *LockManager {
	lm := &LockManager{
		locks: make(map[string]time.Time),
	}
	go lm.startCleanupRoutine()
	return lm
}

func (lm *LockManager) TryLock(key string, ttl time.Duration) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if expiration, exists := lm.locks[key]; exists {
		if time.Now().Before(expiration) {
			return false
		}
	}
	lm.locks[key] = time.Now().Add(ttl)
	return true
}

func (lm *LockManager) Unlock(key string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	delete(lm.locks, key)
}

func (lm *LockManager) startCleanupRoutine() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		lm.mu.Lock()
		now := time.Now()
		for k, expiration := range lm.locks {
			if now.After(expiration) {
				delete(lm.locks, k)
			}
		}
		lm.mu.Unlock()
	}
}
