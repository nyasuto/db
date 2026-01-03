package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"time"
)

var ErrKeyNotFound = errors.New("key not found")

// RecordPos はファイル内でのレコードの位置情報を保持します。
type RecordPos struct {
	Offset int64
}

// DB は Bitcask モデルの簡易的な KVS エンジンです。
type DB struct {
	mu     sync.RWMutex
	file   *os.File
	keyDir map[string]RecordPos
	offset int64
}

// NewDB は指定されたパスでデータベースを開きます。
func NewDB(path string) (*DB, error) {
	// ファイルを開く（追記モード）
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	db := &DB{
		file:   file,
		keyDir: make(map[string]RecordPos),
		offset: stat.Size(),
	}

	// 既存データがある場合はインデックスを復元
	if stat.Size() > 0 {
		if err := db.loadKeyDir(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	return db, nil
}

// loadKeyDir はファイル全体を走査してインデックスを再構築します。
func (d *DB) loadKeyDir() error {
	// ファイルの先頭から読み込むためにSeekする
	// ただし、DBのメインのoffsetは追記用なので動かしたくないが、
	// NewDBの中なのでまだPutは走らない。
	// 安全のため、ReadAtを使うか、一時的にSeekして戻すか。
	// ここでは構造上、NewDB内でのみ呼ばれる前提で、Read系関数を使う。
	// しかし効率化のため bufio を使いたいが、標準ライブラリ制約と構造体のシンプルさを優先し、
	// os.FileのReadで順次読み進める。

	// 現在のファイルポインタを保存（通常は0か末尾のはずだが念のため）
	// O_APPENDモードのファイルに対してSeekしてもWrite位置は常に末尾になるが、
	// Read位置はSeekの影響を受ける。
	if _, err := d.file.Seek(0, 0); err != nil {
		return err
	}

	var offset int64
	fileSize := d.offset // NewDBで設定済み

	for offset < fileSize {
		// ヘッダー読み込み
		header := make([]byte, 16)
		if _, err := d.file.Read(header); err != nil {
			return err
		}

		// サイズ取得
		keySize := int64(binary.BigEndian.Uint32(header[8:12]))
		valSizeRaw := binary.BigEndian.Uint32(header[12:16])

		// キーを読み込む
		key := make([]byte, keySize)
		if _, err := d.file.Read(key); err != nil {
			return err
		}

		if valSizeRaw == tombstoneValueSize {
			// 削除レコード (Tombstone)
			delete(d.keyDir, string(key))
			offset += 16 + keySize
		} else {
			// 通常レコード
			d.keyDir[string(key)] = RecordPos{Offset: offset}
			valSize := int64(valSizeRaw)

			// 値の部分をスキップ
			if _, err := d.file.Seek(valSize, 1); err != nil { // 1 = io.SeekCurrent
				return err
			}
			offset += 16 + keySize + valSize
		}
	}

	return nil
}

// Close はデータベースを閉じます。
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}

const tombstoneValueSize = ^uint32(0) // MaxUint32

// Put はキーと値を保存します。
func (d *DB) Put(key, value []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	keySize := uint32(len(key))
	valSize := uint32(len(value))

	if valSize == tombstoneValueSize {
		return errors.New("value too large")
	}

	ts := time.Now().UnixNano()
	totalSize := 8 + 4 + 4 + int64(keySize) + int64(valSize)

	// バッファの作成
	// [Timestamp(8)][KeySize(4)][ValueSize(4)][Key...][Value...]
	buf := make([]byte, totalSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(ts))
	binary.BigEndian.PutUint32(buf[8:12], keySize)
	binary.BigEndian.PutUint32(buf[12:16], valSize)
	copy(buf[16:16+keySize], key)
	copy(buf[16+keySize:], value)

	// ファイルへの書き込み
	if _, err := d.file.Write(buf); err != nil {
		return err
	}

	// インデックスの更新
	d.keyDir[string(key)] = RecordPos{Offset: d.offset}
	d.offset += totalSize

	return nil
}

// Delete はキーを削除します。
func (d *DB) Delete(key []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	ts := time.Now().UnixNano()
	keySize := uint32(len(key))
	// ValueSizeにTombstone用の値を設定
	valSize := tombstoneValueSize

	// ヘッダー(16 bytes) + キー (値はなし)
	totalSize := 8 + 4 + 4 + int64(keySize)

	buf := make([]byte, totalSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(ts))
	binary.BigEndian.PutUint32(buf[8:12], keySize)
	binary.BigEndian.PutUint32(buf[12:16], valSize)
	copy(buf[16:16+keySize], key)

	if _, err := d.file.Write(buf); err != nil {
		return err
	}

	// インデックスから削除
	delete(d.keyDir, string(key))
	d.offset += totalSize

	return nil
}

// Get はキーに対応する値を取得します。
func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	pos, ok := d.keyDir[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// ヘッダー読み込み (Timestamp + KeySize + ValueSize = 16 bytes)
	header := make([]byte, 16)
	if _, err := d.file.ReadAt(header, pos.Offset); err != nil {
		return nil, err
	}

	keySize := binary.BigEndian.Uint32(header[8:12])
	valSize := binary.BigEndian.Uint32(header[12:16])

	// KeyとValueを読み込む
	// データ位置 = Offset + 16
	data := make([]byte, keySize+valSize)
	if _, err := d.file.ReadAt(data, pos.Offset+16); err != nil {
		return nil, err
	}

	// キーの一致確認（念のため）
	readKey := data[:keySize]
	if string(readKey) != string(key) {
		return nil, errors.New("data corruption: key mismatch")
	}

	readValue := data[keySize:]
	// 呼び出し元が変更しても安全なようにコピーを返す
	result := make([]byte, len(readValue))
	copy(result, readValue)

	return result, nil
}
