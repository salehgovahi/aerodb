package storage

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrKeyNotFound = errors.New("key not found")

const ShardCount = 16

type Storage interface {
	Set(key string, value string, version int64) bool
	Get(key string) (string, int64, error)
	Delete(key string, version int64)
}

type entry struct {
	key     string
	value   string
	version int64
}

type cacheShard struct {
	mu    sync.Mutex
	ll    *list.List
	cache map[string]*list.Element
}

type MemoryStorage struct {
	shards       [ShardCount]*cacheShard
	capacity     int
	filename     string
	walFilename  string
	walFile      *os.File
	walMu        sync.Mutex
	dirtyCounter int
	dirtyMu      sync.Mutex
}

type Record struct {
	Value   string
	Version int64
}

func NewMemoryStorage(filename string, capacity int) *MemoryStorage {
	walName := strings.Replace(filename, ".bin", ".wal", 1)

	s := &MemoryStorage{
		capacity:    capacity,
		filename:    filename,
		walFilename: walName,
	}

	for i := range ShardCount {
		s.shards[i] = &cacheShard{
			ll:    list.New(),
			cache: make(map[string]*list.Element),
		}
	}

	s.LoadSnapshot()
	s.replayWAL()

	file, err := os.OpenFile(s.walFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("Failed to open WAL file: " + err.Error())
	}
	s.walFile = file

	go s.StartSnapshotRoutine(60*time.Second, 5)

	return s
}

func (s *MemoryStorage) getShard(key string) *cacheShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	shardIndex := h.Sum32() % ShardCount
	return s.shards[shardIndex]
}

func (s *MemoryStorage) restoreData(key, value string, version int64) {
	shard := s.getShard(key)
	if ele, hit := shard.cache[key]; hit {
		shard.ll.MoveToFront(ele)
		ent := ele.Value.(*entry)
		ent.value = value
		ent.version = version
	} else {
		ele := shard.ll.PushFront(&entry{key: key, value: value, version: version})
		shard.cache[key] = ele
	}
}

func (s *MemoryStorage) removeData(key string) {
	shard := s.getShard(key)
	if ele, hit := shard.cache[key]; hit {
		shard.ll.Remove(ele)
		delete(shard.cache, key)
	}
}

func (s *MemoryStorage) appendToWAL(command, key, value string, version int64) {
	s.walMu.Lock()
	defer s.walMu.Unlock()

	var line string
	if command == "SET" {

		line = fmt.Sprintf("SET %s %d %s\n", key, version, value)
	} else {

		line = fmt.Sprintf("DEL %s %d\n", key, version)
	}
	s.walFile.WriteString(line)
	s.walFile.Sync()
}

func (s *MemoryStorage) SaveSnapshot() error {
	snapshotData := make(map[string]Record)

	for i := range ShardCount {
		shard := s.shards[i]
		shard.mu.Lock()
		for e := shard.ll.Front(); e != nil; e = e.Next() {
			kv := e.Value.(*entry)
			snapshotData[kv.key] = Record{Value: kv.value, Version: kv.version}
		}
		shard.mu.Unlock()
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(snapshotData)
	if err != nil {
		return err
	}

	err = os.WriteFile(s.filename, buf.Bytes(), 0644)
	if err == nil {
		s.walMu.Lock()
		s.walFile.Truncate(0)
		s.walFile.Seek(0, 0)
		s.walMu.Unlock()
	}
	return err
}

func (s *MemoryStorage) Set(key string, value string, version int64) bool {
	shard := s.getShard(key)
	shard.mu.Lock()

	if ele, hit := shard.cache[key]; hit {
		ent := ele.Value.(*entry)
		if version <= ent.version {
			shard.mu.Unlock()
			return false
		}
		ent.value = value
		ent.version = version
		shard.ll.MoveToFront(ele)
	} else {
		ele := shard.ll.PushFront(&entry{key: key, value: value, version: version})
		shard.cache[key] = ele
	}
	shard.mu.Unlock()

	s.appendToWAL("SET", key, value, version)

	s.dirtyMu.Lock()
	s.dirtyCounter++
	s.dirtyMu.Unlock()

	shardCapacity := s.capacity / ShardCount
	if s.capacity > 0 && shard.ll.Len() > shardCapacity {
		s.evictOldestFromShard(shard)
	}

	return true
}

func (s *MemoryStorage) evictOldestFromShard(shard *cacheShard) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	ele := shard.ll.Back()
	if ele != nil {
		kv := ele.Value.(*entry)
		shard.ll.Remove(ele)
		delete(shard.cache, kv.key)

		go s.appendToWAL("DEL", kv.key, "", kv.version)

		s.dirtyMu.Lock()
		s.dirtyCounter++
		s.dirtyMu.Unlock()
		fmt.Printf("[LRU-SHARD] Evicted key '%s' due to shard memory limits.\n", kv.key)
	}
}

func (s *MemoryStorage) Get(key string) (string, int64, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if ele, hit := shard.cache[key]; hit {
		shard.ll.MoveToFront(ele)
		ent := ele.Value.(*entry)
		return ent.value, ent.version, nil
	}
	return "", 0, ErrKeyNotFound
}

func (s *MemoryStorage) Delete(key string, version int64) {
	shard := s.getShard(key)
	shard.mu.Lock()

	deleted := false
	if ele, hit := shard.cache[key]; hit {
		ent := ele.Value.(*entry)

		if version >= ent.version {
			shard.ll.Remove(ele)
			delete(shard.cache, key)
			deleted = true
		}
	}
	shard.mu.Unlock()

	if deleted {
		s.appendToWAL("DEL", key, "", version)
		s.dirtyMu.Lock()
		s.dirtyCounter++
		s.dirtyMu.Unlock()
	}
}

func (s *MemoryStorage) replayWAL() {
	file, err := os.Open(s.walFilename)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()

		parts := strings.SplitN(line, " ", 4)
		if len(parts) < 3 {
			continue
		}

		cmd, key := parts[0], parts[1]
		version, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			continue
		}

		if cmd == "SET" && len(parts) == 4 {
			s.restoreData(key, parts[3], version)
			count++
		} else if cmd == "DEL" {
			s.removeData(key)
			count++
		}
	}
	if count > 0 {
		fmt.Printf("[WAL] Replayed %d operations from %s\n", count, s.walFilename)
	}
}

func (s *MemoryStorage) LoadSnapshot() {
	bytesData, err := os.ReadFile(s.filename)
	if err != nil {
		return
	}

	buf := bytes.NewBuffer(bytesData)
	decoder := gob.NewDecoder(buf)

	var snapshotData map[string]Record
	err = decoder.Decode(&snapshotData)
	if err != nil {
		fmt.Println("[STORAGE] Error decoding snapshot:", err)
		return
	}

	for k, v := range snapshotData {
		s.restoreData(k, v.Value, v.Version)
	}

	totalItems := 0
	for i := 0; i < ShardCount; i++ {
		totalItems += len(s.shards[i].cache)
	}
	fmt.Printf("[STORAGE] Successfully loaded %d items across %d shards from %s\n", totalItems, ShardCount, s.filename)
}

func (s *MemoryStorage) StartSnapshotRoutine(interval time.Duration, threshold int) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		s.dirtyMu.Lock()
		currentDirty := s.dirtyCounter
		s.dirtyMu.Unlock()

		if currentDirty < threshold {
			continue
		}

		err := s.SaveSnapshot()
		if err == nil {
			fmt.Printf("[STORAGE] Snapshot saved! WAL truncated. (Triggered by %d changes)\n", currentDirty)
			s.dirtyMu.Lock()
			s.dirtyCounter -= currentDirty
			s.dirtyMu.Unlock()
		}
	}
}
