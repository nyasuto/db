package storage

import (
	"fmt"
	"hash/fnv"
	"path/filepath"
)

// ShardedDB wraps multiple DB instances (shards) to reduce lock contention.
type ShardedDB struct {
	shards    []*DB
	numShards int
}

// NewShardedDB creates a new ShardedDB with the specified number of shards.
// Each shard is stored in a subdirectory "shard-N" under dirPath.
func NewShardedDB(dirPath string, numShards int) (*ShardedDB, error) {
	if numShards <= 0 {
		numShards = 1
	}

	shards := make([]*DB, numShards)

	// Open/Create each shard
	for i := 0; i < numShards; i++ {
		shardPath := filepath.Join(dirPath, fmt.Sprintf("shard-%d", i))
		db, err := NewDB(shardPath)
		if err != nil {
			// Cleanup already opened shards
			for j := 0; j < i; j++ {
				_ = shards[j].Close()
			}
			return nil, err
		}
		shards[i] = db
	}

	return &ShardedDB{
		shards:    shards,
		numShards: numShards,
	}, nil
}

// getShard returns the DB instance responsible for the given key.
func (s *ShardedDB) getShard(key []byte) *DB {
	h := fnv.New32a()
	_, _ = h.Write(key)
	// Use bitwise operation if numShards is power of 2, but module is fine for generic.
	// int(uint32) is safe on 64-bit arch. On 32-bit arch, it might wrap, but we take Abs or assume 64bit env (darwin/arm64).
	idx := int(h.Sum32()) % s.numShards
	if idx < 0 {
		idx = -idx
	}
	return s.shards[idx]
}

// Put delegates to the appropriate shard.
func (s *ShardedDB) Put(key, value []byte) error {
	return s.getShard(key).Put(key, value)
}

// Get delegates to the appropriate shard.
func (s *ShardedDB) Get(key []byte) ([]byte, error) {
	return s.getShard(key).Get(key)
}

// Delete delegates to the appropriate shard.
func (s *ShardedDB) Delete(key []byte) error {
	return s.getShard(key).Delete(key)
}

// Close closes all shards.
func (s *ShardedDB) Close() error {
	var firstErr error
	for _, db := range s.shards {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Merge triggers compaction on all shards. It runs sequentially to avoid excessive I/O load.
func (s *ShardedDB) Merge() error {
	for _, db := range s.shards {
		if err := db.Merge(); err != nil {
			return err
		}
	}
	return nil
}
