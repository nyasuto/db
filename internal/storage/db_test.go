package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestMerge(t *testing.T) {
	dbDir := "test_merge_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	// テスト用にファイルサイズ制限を一時的に小さくする
	originalMax := MaxFileSize
	MaxFileSize = 100 // 100 bytes
	defer func() { MaxFileSize = originalMax }()

	db, err := NewDB(dbDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	// データ書き込み (レコードサイズ 約28-30バイト)
	// 1. File 0 に書き込む (Limit 100)
	// Key1 (Put) -> ~30 bytes
	_ = db.Put([]byte("key1"), []byte("value1"))
	// Key1 (Update) -> ~30 bytes (ここで key1 の古い値がゴミになる)
	_ = db.Put([]byte("key1"), []byte("value1-new"))
	// Key2 (Delete) -> ~30 bytes (Tombstone)
	// ここで 90 bytes 程度。次の書き込みでローテーションするはず
	_ = db.Delete([]byte("key2"))

	// 2. ローテーション発生させる (File 0 -> Older, File 1 -> Active)
	_ = db.Put([]byte("key3"), []byte("value3"))

	// 3. マージ前の状態確認
	// 期待: File 0 が存在し、OlderFiles にある。
	// File 0 のサイズは 3レコード分。

	// Merge実行
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// 4. マージ後の検証
	// key1: 最新の値が残っていること
	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get key1: %v", err)
	}
	if string(val) != "value1-new" {
		t.Errorf("Expected value1-new, got %s", string(val))
	}

	// key2: 削除されていること (TombstoneはMergeで消えるはず... いや、セグメントマージの仕様による)
	// Bitcaskでは、Tombstoneは "それより古いファイルに有効な値がない" ことが確定すれば消せるが、
	// 単純な実装ではTombstoneも最新として残る場合がある。
	// 今回の実装では "ActiveFileにあるキーは対象外" で olderFiles のみを処理している。
	// olderFiles 内での重複は解消される。
	// key2 は File 0 にしかないので、Tombstone が残るのか？
	// 実装詳細: 'newKeyPos' に書き込む際、keyDir をイテレートしている。
	// keyDir には Tombstone は存在しない (Delete時に delete(keyDir) しているため)。
	// したがって、keyDir に載っていない key2 (Tombstone) は、Merge時のループに出てこない！
	// つまり、Mergeされたファイルには Tombstone レコードは書き込まれない。 -> 正しい挙動（消滅する）。

	_, err = db.Get([]byte("key2"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for key2, got %v", err)
	}

	// key3: ActiveFileにあるので影響を受けず読めること
	val3, err := db.Get([]byte("key3"))
	if err != nil {
		t.Errorf("Failed to get key3: %v", err)
	}
	if string(val3) != "value3" {
		t.Errorf("Expected value3, got %s", string(val3))
	}

	// 5. DB再起動して永続化確認
	_ = db.Close()
	db2, err := NewDB(dbDir)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer func() { _ = db2.Close() }()

	val, err = db2.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get key1 after reopen: %v", err)
	}
	if string(val) != "value1-new" {
		t.Errorf("Expected value1-new, got %s", string(val))
	}
}

func BenchmarkRecovery(b *testing.B) {
	dbDir := "bench_recovery_dir"
	// ベンチマーク終了後にクリーンアップ
	defer func() { _ = os.RemoveAll(dbDir) }()

	// 1. データセットアップ (1回だけ実行)
	_ = os.RemoveAll(dbDir)
	db, err := NewDB(dbDir)
	if err != nil {
		b.Fatal(err)
	}

	// 10,000件のデータを書き込み
	// データサイズ: Key(16) + Val(4KB) + Header(20) = ~4.1KB
	// Total ~41MB. Difference between hint load (skipping val) and full load (reading val) should be visible.
	val := make([]byte, 4096)
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%09d", i))
		if err := db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
	// マージして Hint File を生成
	if err := db.Merge(); err != nil {
		b.Fatal(err)
	}
	_ = db.Close()

	// 2. Hint File ありの起動ベンチマーク
	b.Run("WithHint", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db, err := NewDB(dbDir)
			if err != nil {
				b.Fatal(err)
			}
			_ = db.Close()
		}
	})

	// Hint File を削除
	files, _ := os.ReadDir(dbDir)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".hint") {
			_ = os.Remove(filepath.Join(dbDir, f.Name()))
		}
	}

	// 3. Hint File なしの起動ベンチマーク
	b.Run("NoHint", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db, err := NewDB(dbDir)
			if err != nil {
				b.Fatal(err)
			}
			_ = db.Close()
		}
	})
}
func TestHintFile(t *testing.T) {
	dbDir := "test_hint_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	// ローテーションしやすく調整
	originalMax := MaxFileSize
	MaxFileSize = 100
	defer func() { MaxFileSize = originalMax }()

	db, err := NewDB(dbDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	// データ投入 (File 0 -> Older)
	_ = db.Put([]byte("key1"), []byte("val1"))
	_ = db.Put([]byte("key2"), []byte("val2"))
	_ = db.Put([]byte("key3"), []byte("val3"))
	_ = db.Put([]byte("key4"), []byte("val4")) // Force Rotate

	// Merge実行
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// .hintファイルの存在確認
	entries, err := os.ReadDir(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	hintFound := false
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".hint") {
			hintFound = true
			break
		}
	}
	if !hintFound {
		t.Error("Hint file not generated after merge")
	}

	_ = db.Close()

	// 再起動 (Hint Fileからのロードを確認)
	db2, err := NewDB(dbDir)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer func() { _ = db2.Close() }()

	val, err := db2.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get key1: %v", err)
	}
	if string(val) != "val1" {
		t.Errorf("Expected val1, got %s", val)
	}
}

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

func BenchmarkGetOlder(b *testing.B) {
	dbDir := "bench_get_older_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	// Save original MaxFileSize
	originalMax := MaxFileSize
	MaxFileSize = 1024 * 1024 // 1MB
	defer func() { MaxFileSize = originalMax }()

	db, err := NewDB(dbDir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Write enough data to create older files
	val := make([]byte, 1024) // 1KB
	const itemCount = 2000    // ~2MB total
	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Access keys likely in older files (first half)
		key := []byte(fmt.Sprintf("key-%d", i%1000))
		if _, err := db.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetOlderParallel(b *testing.B) {
	dbDir := "bench_get_older_parallel_dir"
	defer func() { _ = os.RemoveAll(dbDir) }()

	originalMax := MaxFileSize
	MaxFileSize = 1024 * 1024 // 1MB
	defer func() { MaxFileSize = originalMax }()

	db, err := NewDB(dbDir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	val := make([]byte, 1024)
	const itemCount = 2000
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
			key := []byte(fmt.Sprintf("key-%d", i%1000))
			if _, err := db.Get(key); err != nil {
				b.Fatal(err)
			}
		}
	})
}
