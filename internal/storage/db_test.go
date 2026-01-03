package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestPutGet(t *testing.T) {
	dbDir := "test_db_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	db, err := NewDB(dbDir)
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
	dbDir := "test_recovery_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	// 1. 書き込み
	db, err := NewDB(dbDir)
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
	db2, err := NewDB(dbDir)
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
	dbDir := "test_delete_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	db, err := NewDB(dbDir)
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
	db2, err := NewDB(dbDir)
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

func TestMerge_DisabledForSegmentation(t *testing.T) {
	dbDir := "test_merge_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	db, err := NewDB(dbDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Merge should currently fail with Not Implemented
	if err := db.Merge(); err != ErrCompactionNotImp {
		t.Errorf("Expected ErrCompactionNotImp, got %v", err)
	}
}

// Old TestMerge logic commented out or removed until Compaction is re-implemented
/*
func TestMerge(t *testing.T) {
    ...
}
*/

func TestRotation(t *testing.T) {
	dbDir := "test_rotation_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	db, err := NewDB(dbDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	// 10MBを超えるデータを書き込む
	// 1KB payload + header(20) + key(~10) = ~1054 bytes
	// 10*1024*1024 / 1054 ≒ 9953 records
	// 余裕を見て 11000 レコード書けば確実にローテーションするはず

	val := make([]byte, 1024)
	for i := range val {
		val[i] = 'a'
	}

	totalRecords := 11000
	for i := 0; i < totalRecords; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put failed at index %d: %v", i, err)
		}
	}

	// 書き込み完了後の検証

	// 1. ファイルが複数生成されているか (0.data, 1.data...)
	files, err := os.ReadDir(dbDir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}

	dataFiles := 0
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".data" {
			dataFiles++
		}
	}
	if dataFiles < 2 {
		t.Errorf("Expected multiple data files (rotation), but found %d", dataFiles)
	}

	// 2. データの読み出し検証 (古いファイルと新しいファイル)
	// 最初の方のキー (0.dataのはず)
	firstKey := []byte("key-0")
	if _, err := db.Get(firstKey); err != nil {
		t.Errorf("Failed to get first key: %v", err)
	}

	// 最後の方のキー (active fileのはず)
	lastKey := []byte(fmt.Sprintf("key-%d", totalRecords-1))
	if _, err := db.Get(lastKey); err != nil {
		t.Errorf("Failed to get last key: %v", err)
	}
}

func TestChecksum(t *testing.T) {
	dbDir := "test_checksum_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	// 1. 正常なデータを書き込む
	func() {
		db, err := NewDB(dbDir)
		if err != nil {
			t.Fatalf("Failed to open DB: %v", err)
		}
		defer func() { _ = db.Close() }()

		if err := db.Put([]byte("key"), []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}()

	// 2. ファイルを直接改ざんする
	// 単一ファイルの時は dbPath を開けばよかったが、今はディレクトリ内の "0.data" を開く
	targetFile := filepath.Join(dbDir, "0.data")

	file, err := os.OpenFile(targetFile, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open file for corruption: %v", err)
	}
	// 末尾の1バイト（Valueの一部）を変更
	off, _ := file.Seek(-1, 2) // 2 = SeekEnd
	if _, err := file.WriteAt([]byte{0xFF}, off); err != nil {
		t.Fatalf("Failed to corrupt file: %v", err)
	}
	_ = file.Close()

	// 3. 起動時チェック (Guardian)
	// loadKeyDirでCRC不整合を検知してエラーになるはず
	_, err = NewDB(dbDir)
	if err != ErrDataCorruption {
		t.Errorf("Expected ErrDataCorruption during recovery, got %v", err)
	}
}

func BenchmarkPut(b *testing.B) {
	dbDir := "bench_put_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	db, err := NewDB(dbDir)
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
	dbDir := "bench_put_1kb_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	db, err := NewDB(dbDir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	val := make([]byte, 1024)
	for i := range val {
		val[i] = 'x'
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	dbDir := "bench_get_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	db, err := NewDB(dbDir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

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
	dbDir := "bench_get_1kb_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	db, err := NewDB(dbDir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

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
