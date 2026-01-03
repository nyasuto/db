package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestPutGet(t *testing.T) {
	dbPath := "test_db.data"
	defer func() { _ = os.Remove(dbPath) }()

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer func() { _ = db.Close() }()

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
	defer func() { _ = os.Remove(dbPath) }()

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
	_ = db.Close() // ここで閉じる

	// 2. 再起動と読み込み
	db2, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to re-open DB: %v", err)
	}
	defer func() { _ = db2.Close() }()

	got, err := db2.Get(key)
	if err != nil {
		t.Fatalf("Get after recovery failed: %v", err)
	}

	if string(got) != string(value) {
		t.Errorf("Expected %s, got %s", string(value), string(got))
	}
}

func TestDelete(t *testing.T) {
	dbPath := "test_delete.data"
	defer func() { _ = os.Remove(dbPath) }()

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	key := []byte("my-key")
	value := []byte("my-value")

	// 1. Put
	if err := db.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Delete
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 3. Get -> NotFound
	_, err = db.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

	// 4. Close & Reopen (Persistence check)
	_ = db.Close()
	db2, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to re-open DB: %v", err)
	}
	defer func() { _ = db2.Close() }()

	_, err = db2.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after recovery, got %v", err)
	}

	// 5. Resurrection
	newValue := []byte("new-value")
	if err := db2.Put(key, newValue); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	got, err := db2.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(newValue) {
		t.Errorf("Expected %s, got %s", string(newValue), string(got))
	}
}

func TestMerge(t *testing.T) {
	dbPath := "test_merge.data"
	defer func() { _ = os.Remove(dbPath) }()

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	k1 := []byte("key1")
	k2 := []byte("key2")
	v1 := []byte("val1")
	v2 := []byte("val2")

	// 1. Write initial data
	_ = db.Put(k1, v1)                     // オフセット 0
	_ = db.Put(k2, v2)                     // オフセット X
	_ = db.Put(k1, []byte("val1-updated")) // オフセット Y (k1上書き)
	_ = db.Delete(k1)                      // オフセット Z (k1削除)

	// この時点でファイルには4レコードあるが、有効なのは k2 のみ。
	beforeInfo, _ := os.Stat(dbPath)
	beforeSize := beforeInfo.Size()

	// 2. Merge実行
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// 3. サイズチェック (劇的に減っているはず)
	// k2のレコードサイズ = 16(header) + 4(key:key2) + 4(val:val2) = 24 bytes
	info, _ := os.Stat(dbPath)
	afterSize := info.Size()

	if afterSize >= beforeSize {
		t.Errorf("Expected file size to shrink, but got before=%d, after=%d", beforeSize, afterSize)
	}

	// 4. データ整合性チェック
	val, err := db.Get(k2)
	if err != nil {
		t.Errorf("Get(k2) failed: %v", err)
	}
	if string(val) != string(v2) {
		t.Errorf("Expected %s, got %s", string(v2), string(val))
	}

	_, err = db.Get(k1)
	if err != ErrKeyNotFound {
		t.Errorf("Expected k1 to be deleted, but got: %v", err)
	}

	_ = db.Close()

	// 5. 再起動後チェック
	db2, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer func() { _ = db2.Close() }()

	val, err = db2.Get(k2)
	if err != nil {
		t.Errorf("Get(k2) after reopen failed: %v", err)
	}
	if string(val) != string(v2) {
		t.Errorf("Expected %s, got %s", string(v2), string(val))
	}
}

func TestChecksum(t *testing.T) {
	dbPath := "test_checksum.data"
	defer func() { _ = os.Remove(dbPath) }()

	// 1. 正常なデータを書き込む
	func() {
		db, err := NewDB(dbPath)
		if err != nil {
			t.Fatalf("Failed to open DB: %v", err)
		}
		defer func() { _ = db.Close() }()

		if err := db.Put([]byte("key"), []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}()

	// 2. ファイルを直接改ざんする
	file, err := os.OpenFile(dbPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open file for corruption: %v", err)
	}
	// 末尾の1バイト（Valueの一部）を変更
	off, _ := file.Seek(-1, 2) // 2 = SeekEnd
	if _, err := file.WriteAt([]byte{0xFF}, off); err != nil {
		t.Fatalf("Failed to corrupt file: %v", err)
	}
	file.Close()

	// 3. 起動時チェック (Guardian)
	// loadKeyDirでCRC不整合を検知してエラーになるはず
	_, err = NewDB(dbPath)
	if err != ErrDataCorruption {
		t.Errorf("Expected ErrDataCorruption during recovery, got %v", err)
	}
}

func BenchmarkPut(b *testing.B) {
	dbPath := "bench_put.data"
	defer func() { _ = os.Remove(dbPath) }()

	db, err := NewDB(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

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
	defer func() { _ = os.Remove(dbPath) }()

	db, err := NewDB(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

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
	defer func() { _ = os.Remove(dbPath) }()

	db, err := NewDB(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

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
	defer func() { _ = os.Remove(dbPath) }()

	db, err := NewDB(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

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

func BenchmarkPutParallel(b *testing.B) {
	dbPath := "bench_put_parallel.data"
	defer func() { _ = os.Remove(dbPath) }()

	db, err := NewDB(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			// 簡易的なキー生成（並行実行内でのユニーク性は保証しないが、競合負荷を見る目的には十分）
			// 本来は atomic add などでユニークにするが、Putは上書きでも問題ない
			key := []byte(fmt.Sprintf("key-%d", i))
			val := []byte(fmt.Sprintf("val-%d", i))
			if err := db.Put(key, val); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGetParallel(b *testing.B) {
	dbPath := "bench_get_parallel.data"
	defer func() { _ = os.Remove(dbPath) }()

	db, err := NewDB(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	const itemCount = 10000
	val := make([]byte, 128)
	for i := 0; i < itemCount; i++ {
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
			key := []byte(fmt.Sprintf("key-%d", i%itemCount))
			if _, err := db.Get(key); err != nil {
				b.Fatal(err)
			}
		}
	})
}
