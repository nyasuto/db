package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestShardedDB(t *testing.T) {
	dir := "test_sharded_db"
	defer func() { _ = os.RemoveAll(dir) }()

	db, err := NewShardedDB(dir, 4)
	if err != nil {
		t.Fatalf("Failed to create db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Put/Get
	if err := db.Put([]byte("key1"), []byte("val1")); err != nil {
		t.Fatal(err)
	}
	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val1" {
		t.Errorf("Expected val1, got %s", val)
	}

	// Delete
	if err := db.Delete([]byte("key1")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("key1")); err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func BenchmarkShardedPutParallel(b *testing.B) {
	dir := "bench_sharded_put"
	defer func() { _ = os.RemoveAll(dir) }()

	db, err := NewShardedDB(dir, 16)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			// Unique key per routine is hard in RunParallel without ID,
			// but we just want to load the DB. Overwrites are fine for benchmark.
			// Using random suffix or large space helps distribution.
			key := []byte(fmt.Sprintf("key-%d", i))
			val := []byte("value")
			if err := db.Put(key, val); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkShardedGetParallel(b *testing.B) {
	dir := "bench_sharded_get"
	defer func() { _ = os.RemoveAll(dir) }()

	db, err := NewShardedDB(dir, 16)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Pre-fill
	const numItems = 10000
	val := []byte("value")
	for i := 0; i < numItems; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			key := []byte(fmt.Sprintf("key-%d", i%numItems))
			if _, err := db.Get(key); err != nil {
				b.Fatal(err)
			}
		}
	})
}
