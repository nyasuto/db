package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestPutGet(t *testing.T) {
	dbPath := "test_db.data"
	defer os.Remove(dbPath)

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	key := []byte("my-key")
	value := []byte("my-value")

	if err := db.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(got) != string(value) {
		t.Errorf("Expected %s, got %s", string(value), string(got))
	}
}

func TestRecovery(t *testing.T) {
	dbPath := "test_recovery.data"
	defer os.Remove(dbPath)

	// 1. 書き込み
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	key := []byte("persistent-key")
	value := []byte("persistent-value")

	if err := db.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	db.Close() // ここで閉じる

	// 2. 再起動と読み込み
	db2, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to re-open DB: %v", err)
	}
	defer db2.Close()

	got, err := db2.Get(key)
	if err != nil {
		t.Fatalf("Get after recovery failed: %v", err)
	}

	if string(got) != string(value) {
		t.Errorf("Expected %s, got %s", string(value), string(got))
	}
}

func BenchmarkPut(b *testing.B) {
	dbPath := "bench_put.data"
	defer os.Remove(dbPath)

	db, err := NewDB(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("val-%d", i))
		if err := db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPut1KB(b *testing.B) {
	dbPath := "bench_put_1kb.data"
	defer os.Remove(dbPath)

	db, err := NewDB(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// 1KB のダミーデータを作成
	val := make([]byte, 1024)
	for i := range val {
		val[i] = 'x'
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		// valは使い回す（内容生成時間は計測外）
		if err := db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	dbPath := "bench_get.data"
	defer os.Remove(dbPath)

	db, err := NewDB(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// データを事前に書き込む
	const itemCount = 1000
	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("val-%d", i))
		if err := db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i%itemCount))
		if _, err := db.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet1KB(b *testing.B) {
	dbPath := "bench_get_1kb.data"
	defer os.Remove(dbPath)

	db, err := NewDB(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// データを事前に書き込む (10,000件 x 1KB ≒ 10MB)
	const itemCount = 10000
	val := make([]byte, 1024)
	for i := range val {
		val[i] = 'x'
	}

	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// ランダムアクセス風にキーを選択
		key := []byte(fmt.Sprintf("key-%d", i%itemCount))
		if _, err := db.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}
