package storage

import (
	"os"
	"strconv"
	"testing"
)

func cleanupTestFiles(filename string) {
	os.Remove(filename)
	os.Remove(filename[:len(filename)-4] + ".wal")
}

func TestMemoryStorage_SetAndGet(t *testing.T) {
	testFile := "test_dump.bin"
	defer cleanupTestFiles(testFile)

	store := NewMemoryStorage(testFile, 100)

	success := store.Set("name", "AeroDB", 1)
	if !success {
		t.Errorf("Expected Set to return true for a new key")
	}

	val, ver, err := store.Get("name")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if val != "AeroDB" || ver != 1 {
		t.Errorf("Expected 'AeroDB' with version 1, got: '%s' with version %d", val, ver)
	}

	success = store.Set("name", "OldAeroDB", 0)
	if success {
		t.Errorf("Expected Set to return false for an older version")
	}

	val, ver, _ = store.Get("name")
	if val != "AeroDB" || ver != 1 {
		t.Errorf("Expected value to remain 'AeroDB' (v1), but got '%s' (v%d)", val, ver)
	}

	success = store.Set("name", "NewAeroDB", 2)
	if !success {
		t.Errorf("Expected Set to return true for a newer version")
	}

	_, _, err = store.Get("non_existent_key")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got: %v", err)
	}
}

func TestMemoryStorage_Delete(t *testing.T) {
	testFile := "test_dump2.bin"
	defer cleanupTestFiles(testFile)

	store := NewMemoryStorage(testFile, 100)

	store.Set("temp_key", "temp_value", 5)

	store.Delete("temp_key", 4)
	_, _, err := store.Get("temp_key")
	if err == ErrKeyNotFound {
		t.Errorf("Key should not be deleted when using an older version (4 < 5)")
	}

	store.Delete("temp_key", 5)
	_, _, err = store.Get("temp_key")
	if err != ErrKeyNotFound {
		t.Errorf("Key should be deleted, but got error: %v", err)
	}
}

func BenchmarkMemoryStorage_Set(b *testing.B) {
	testFile := "bench_dump.bin"
	defer cleanupTestFiles(testFile)
	store := NewMemoryStorage(testFile, 100000)

	b.ResetTimer() 
	for i := 0; i < b.N; i++ {
		
		store.Set("key", "value", int64(i))
	}
}

func BenchmarkMemoryStorage_Get(b *testing.B) {
	testFile := "bench_dump2.bin"
	defer cleanupTestFiles(testFile)
	store := NewMemoryStorage(testFile, 100000)
	
	
	store.Set("key", "value", 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		
		_, _, _ = store.Get("key")
	}
}


func BenchmarkMemoryStorage_SetParallel(b *testing.B) {
	testFile := "bench_dump_parallel.bin"
	defer cleanupTestFiles(testFile)
	store := NewMemoryStorage(testFile, 100000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int64 = 0
		for pb.Next() {
			i++
			key := "key_" + strconv.FormatInt(i, 10)
			store.Set(key, "value", i)
		}
	})
}