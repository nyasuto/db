package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync"
	"time"
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrDataCorruption = errors.New("data corruption: crc mismatch")
)

// RecordPos はファイル内でのレコードの位置情報を保持します。
type RecordPos struct {
	Offset int64
}

// DB は Bitcask モデルの簡易的な KVS エンジンです。
type DB struct {
	mu     sync.RWMutex
	path   string
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
		path:   path,
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

// Merge はデータファイルを再構築し、不要な領域を解放します。
func (d *DB) Merge() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 一時ファイルの作成
	tempPath := d.path + ".merge"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		_ = tempFile.Close()
		// エラー時には一時ファイルを削除（成功時はRenameされるので削除されない）
		if err != nil {
			_ = os.Remove(tempPath)
		}
	}()

	newKeyDir := make(map[string]RecordPos)
	var newOffset int64

	// 現在の有効なキーのみを新しいファイルに書き写す
	for key, pos := range d.keyDir {
		// Header Read (CRC 4bytes + Ts 8 + KeySize 4 + ValSize 4 = 20)
		header := make([]byte, 20)
		if _, err := d.file.ReadAt(header, pos.Offset); err != nil {
			return err
		}
		keySize := binary.BigEndian.Uint32(header[12:16])
		valSize := binary.BigEndian.Uint32(header[16:20])

		totalRecSize := 20 + int64(keySize) + int64(valSize)
		data := make([]byte, totalRecSize)
		if _, err := d.file.ReadAt(data, pos.Offset); err != nil {
			return err
		}

		// CRC検証はloadKeyDirやGetで行っている前提だが、
		// Merge時にも壊れたデータを移さないよう検証するのがGuardianの役目
		storedCRC := binary.BigEndian.Uint32(data[0:4])
		// データ部分: data[4:]
		calculatedCRC := crc32.ChecksumIEEE(data[4:])
		if storedCRC != calculatedCRC {
			return ErrDataCorruption
		}

		// 新しいファイルに書き込み
		if _, err := tempFile.Write(data); err != nil {
			return err
		}

		newKeyDir[key] = RecordPos{Offset: newOffset}
		newOffset += totalRecSize
	}

	// ファイルの入れ替え処理
	// 1. ファイルを閉じる
	if err := d.file.Close(); err != nil {
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}

	// 2. リネーム (Atomic Replace)
	if err := os.Rename(tempPath, d.path); err != nil {
		return err
	}

	// 3. 再オープン
	file, err := os.OpenFile(d.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// 4. メタデータ更新
	d.file = file
	d.keyDir = newKeyDir
	d.offset = newOffset

	return nil
}

// loadKeyDir はファイル全体を走査してインデックスを再構築します。
func (d *DB) loadKeyDir() error {
	if _, err := d.file.Seek(0, 0); err != nil {
		return err
	}

	var offset int64
	fileSize := d.offset // NewDBで設定済み

	for offset < fileSize {
		// ヘッダー読み込み (CRC+Ts+Sizes = 20 bytes)
		header := make([]byte, 20)
		if _, err := d.file.Read(header); err != nil {
			return err
		}

		storedCRC := binary.BigEndian.Uint32(header[0:4])
		keySize := int64(binary.BigEndian.Uint32(header[12:16]))
		valSizeRaw := binary.BigEndian.Uint32(header[16:20])

		// キーを読み込む
		key := make([]byte, keySize)
		if _, err := d.file.Read(key); err != nil {
			return err
		}

		// CRC検証のためにはValueの情報も必要
		// しかし、Valueを読むとシーク位置が進むため効率が...
		// The Guardianとしては信頼性優先で検証すべき。

		var valSize int64
		var isTombstone bool

		if valSizeRaw == tombstoneValueSize {
			isTombstone = true
			valSize = 0 // ファイル上のValue実体は0バイト
		} else {
			valSize = int64(valSizeRaw)
		}

		// CRC計算用バッファの構築 (Header[4:] + Key + Value)
		// 効率化: 本当はストリームで計算したいが、簡易実装として読み込む
		checkData := make([]byte, 16+keySize+valSize)
		copy(checkData[0:16], header[4:]) // Timestamp ~ ValueSize
		copy(checkData[16:16+keySize], key)

		if !isTombstone {
			// Valueを読み込む
			if _, err := d.file.Read(checkData[16+keySize:]); err != nil {
				return err
			}
		}

		// CRC計算と検証
		if crc32.ChecksumIEEE(checkData) != storedCRC {
			return ErrDataCorruption
		}

		if isTombstone {
			delete(d.keyDir, string(key))
			// isTombstoneの場合、ReadでValue位置は進んでいないのでSeek不要
		} else {
			d.keyDir[string(key)] = RecordPos{Offset: offset}
			// ValueはCRC計算のために既にRead済みなのでSeek不要
		}

		// 次のレコードへ (Readした分だけ進んでいるはず)
		offset += 20 + keySize + valSize
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
	// CRC(4) + Ts(8) + KSize(4) + VSize(4) + Key + Value
	totalSize := 4 + 8 + 4 + 4 + int64(keySize) + int64(valSize)

	// バッファの作成
	buf := make([]byte, totalSize)
	// Offset 4からメタデータ書き込み
	binary.BigEndian.PutUint64(buf[4:12], uint64(ts))
	binary.BigEndian.PutUint32(buf[12:16], keySize)
	binary.BigEndian.PutUint32(buf[16:20], valSize)
	copy(buf[20:20+keySize], key)
	copy(buf[20+keySize:], value)

	// CRC計算 (Timestamp以降の全データ)
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)

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
	valSize := tombstoneValueSize

	// CRC(4) + Header(16) + Key (TombstoneなのでValueなし)
	totalSize := 4 + 8 + 4 + 4 + int64(keySize)

	buf := make([]byte, totalSize)
	binary.BigEndian.PutUint64(buf[4:12], uint64(ts))
	binary.BigEndian.PutUint32(buf[12:16], keySize)
	binary.BigEndian.PutUint32(buf[16:20], valSize)
	copy(buf[20:20+keySize], key)

	// CRC計算
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)

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

	// ヘッダー読み込み (CRC+Ts+Sizes = 20 bytes)
	header := make([]byte, 20)
	if _, err := d.file.ReadAt(header, pos.Offset); err != nil {
		return nil, err
	}

	storedCRC := binary.BigEndian.Uint32(header[0:4])
	keySize := binary.BigEndian.Uint32(header[12:16])
	valSize := binary.BigEndian.Uint32(header[16:20])

	// データ本体を読み込む (Key + Value)
	dataSize := int64(keySize) + int64(valSize)
	// データ位置 = Offset + 20
	// しかしCRC検証のためには Header[4:] + Data が必要
	// 効率化のため、改めて全体を読むか、部分を読むか。
	// ここではデータ本体を読み、メモリ上で結合してCRCチェックする

	data := make([]byte, dataSize)
	if _, err := d.file.ReadAt(data, pos.Offset+20); err != nil {
		return nil, err
	}

	// CRC計算用のバッファ構築
	// (メモリ効率はやや悪いが正確性重視)
	checkBuf := make([]byte, 16+dataSize)
	copy(checkBuf[0:16], header[4:]) // Timestamp(8) + KSize(4) + VSize(4)
	copy(checkBuf[16:], data)

	if crc32.ChecksumIEEE(checkBuf) != storedCRC {
		return nil, ErrDataCorruption
	}

	// キーの一致確認
	readKey := data[:keySize]
	if string(readKey) != string(key) {
		return nil, errors.New("data corruption: key mismatch")
	}

	readValue := data[keySize:]
	result := make([]byte, len(readValue))
	copy(result, readValue)

	return result, nil
}
