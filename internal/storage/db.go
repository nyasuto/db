package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrDataCorruption   = errors.New("data corruption: crc mismatch")
	ErrCompactionNotImp = errors.New("compaction not implemented for segmented mode")
)

var (
	MaxFileSize = int64(10 * 1024 * 1024) // 10MB (var for testing)
)

const (
	tombstoneValueSize = ^uint32(0) // MaxUint32
)

// RecordPos はファイル内でのレコードの位置情報を保持します。
type RecordPos struct {
	FileID int
	Offset int64
}

// DB は Bitcask モデルの簡易的な KVS エンジンです。
type DB struct {
	mu           sync.RWMutex
	dirPath      string
	activeFile   *os.File
	activeFileID int
	olderFiles   map[int]*os.File
	keyDir       map[string]RecordPos
	writeOffset  int64
}

// NewDB は指定されたディレクトリパスでデータベースを開きます。
func NewDB(dirPath string) (*DB, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	// ディレクトリ内の .data ファイルを取得
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	var fileIDs []int
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".data") {
			name := strings.TrimSuffix(entry.Name(), ".data")
			id, err := strconv.Atoi(name)
			if err == nil {
				fileIDs = append(fileIDs, id)
			}
		}
	}
	sort.Ints(fileIDs)

	db := &DB{
		dirPath:    dirPath,
		olderFiles: make(map[int]*os.File),
		keyDir:     make(map[string]RecordPos),
	}

	// 全ファイルをロードしてインデックス構築
	for _, id := range fileIDs {
		if err := db.loadFile(id); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	// アクティブファイルの設定（ファイルが無い、または最後のファイルが既存の場合）
	if len(fileIDs) == 0 {
		// 新規作成
		if err := db.newActiveFile(0); err != nil {
			_ = db.Close()
			return nil, err
		}
	} else {
		// 最後のファイルをアクティブにする
		// 実際には読み込み専用で開いているものを Reopen するか、そのまま使うか
		// ここでは簡略化のため、最後のIDをアクティブとして設定
		lastID := fileIDs[len(fileIDs)-1]
		// loadFileでolderFilesに入っているので、それをactiveに昇格させる
		f := db.olderFiles[lastID]
		delete(db.olderFiles, lastID)

		db.activeFile = f
		db.activeFileID = lastID

		// オフセットは末尾へ (loadFileでSeekしてないかもしれないので念のため)
		info, err := f.Stat()
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		db.writeOffset = info.Size()
	}

	return db, nil
}

func (d *DB) loadFile(id int) error {
	path := filepath.Join(d.dirPath, fmt.Sprintf("%d.data", id))
	// 読み書きモードで開く（Activeになる可能性があるため）
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	d.olderFiles[id] = file // 一旦olderに入れる

	// インデックス構築
	if err := d.loadKeyDir(id, file); err != nil {
		return err
	}
	return nil
}

func (d *DB) newActiveFile(id int) error {
	// 既存があればOlderへ移動
	if d.activeFile != nil {
		d.olderFiles[d.activeFileID] = d.activeFile
	}

	path := filepath.Join(d.dirPath, fmt.Sprintf("%d.data", id))
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	d.activeFile = file
	d.activeFileID = id
	d.writeOffset = 0
	return nil
}

// loadKeyDir は単一ファイルを走査してインデックスを更新します。
func (d *DB) loadKeyDir(fileID int, file *os.File) error {
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}

	info, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()
	var offset int64

	for offset < fileSize {
		// Header (CRC 4 + Ts 8 + KSize 4 + VSize 4 = 20)
		header := make([]byte, 20)
		if _, err := file.Read(header); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		storedCRC := binary.BigEndian.Uint32(header[0:4])
		keySize := int64(binary.BigEndian.Uint32(header[12:16]))
		valSizeRaw := binary.BigEndian.Uint32(header[16:20])

		key := make([]byte, keySize)
		if _, err := file.Read(key); err != nil {
			return err
		}

		// CRC Check Logic
		var valSize int64
		var isTombstone bool
		if valSizeRaw == tombstoneValueSize {
			isTombstone = true
			valSize = 0
		} else {
			valSize = int64(valSizeRaw)
		}

		checkData := make([]byte, 16+keySize+valSize)
		copy(checkData[0:16], header[4:])
		copy(checkData[16:16+keySize], key)

		if !isTombstone {
			if _, err := file.Read(checkData[16+keySize:]); err != nil {
				return err
			}
		}

		if crc32.ChecksumIEEE(checkData) != storedCRC {
			return ErrDataCorruption
		}

		if isTombstone {
			delete(d.keyDir, string(key))
		} else {
			d.keyDir[string(key)] = RecordPos{FileID: fileID, Offset: offset}
		}

		offset += 20 + keySize + valSize
	}
	return nil
}

// Put はキーと値を保存します。
func (d *DB) Put(key, value []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	keySize := uint32(len(key))
	valSize := uint32(len(value))

	if valSize == tombstoneValueSize {
		return errors.New("value too large")
	}

	// Rotation Check
	currentSize := d.writeOffset
	// CRC(4)+Ts(8)+KS(4)+VS(4)+K+V
	recordSize := 4 + 8 + 4 + 4 + int64(keySize) + int64(valSize)

	if currentSize+recordSize > MaxFileSize {
		// activeFileを閉じて新しいファイルを作成
		if err := d.newActiveFile(d.activeFileID + 1); err != nil {
			return err
		}
	}

	ts := time.Now().UnixNano()
	buf := make([]byte, recordSize)
	binary.BigEndian.PutUint64(buf[4:12], uint64(ts))
	binary.BigEndian.PutUint32(buf[12:16], keySize)
	binary.BigEndian.PutUint32(buf[16:20], valSize)
	copy(buf[20:20+keySize], key)
	copy(buf[20+keySize:], value)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)

	if _, err := d.activeFile.Write(buf); err != nil {
		return err
	}

	d.keyDir[string(key)] = RecordPos{FileID: d.activeFileID, Offset: d.writeOffset}
	d.writeOffset += recordSize

	return nil
}

// Delete はキーを削除します。
func (d *DB) Delete(key []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	keySize := uint32(len(key))
	valSize := tombstoneValueSize
	recordSize := 4 + 8 + 4 + 4 + int64(keySize)

	// Rotation logic included? Yes, simplest is to check active file size for Delete too.
	if d.writeOffset+recordSize > MaxFileSize {
		if err := d.newActiveFile(d.activeFileID + 1); err != nil {
			return err
		}
	}

	ts := time.Now().UnixNano()
	buf := make([]byte, recordSize)
	binary.BigEndian.PutUint64(buf[4:12], uint64(ts))
	binary.BigEndian.PutUint32(buf[12:16], keySize)
	binary.BigEndian.PutUint32(buf[16:20], valSize)
	copy(buf[20:20+keySize], key)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)

	if _, err := d.activeFile.Write(buf); err != nil {
		return err
	}

	delete(d.keyDir, string(key))
	d.writeOffset += recordSize

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

	// どのファイルから読むか特定
	var file *os.File
	if pos.FileID == d.activeFileID {
		file = d.activeFile
	} else {
		var exists bool
		file, exists = d.olderFiles[pos.FileID]
		if !exists {
			return nil, errors.New("file not found: internal error")
		}
	}

	// Read header and data (Same logic as before, just using selected file)
	header := make([]byte, 20)
	if _, err := file.ReadAt(header, pos.Offset); err != nil {
		return nil, err
	}

	storedCRC := binary.BigEndian.Uint32(header[0:4])
	keySize := binary.BigEndian.Uint32(header[12:16])
	valSize := binary.BigEndian.Uint32(header[16:20])

	dataSize := int64(keySize) + int64(valSize)
	data := make([]byte, dataSize)
	if _, err := file.ReadAt(data, pos.Offset+20); err != nil {
		return nil, err
	}

	checkBuf := make([]byte, 16+dataSize)
	copy(checkBuf[0:16], header[4:])
	copy(checkBuf[16:], data)

	if crc32.ChecksumIEEE(checkBuf) != storedCRC {
		return nil, ErrDataCorruption
	}

	if string(data[:keySize]) != string(key) {
		return nil, errors.New("key mismatch")
	}

	result := make([]byte, valSize)
	copy(result, data[keySize:])
	return result, nil
}

// Close はデータベースを閉じます。
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.activeFile != nil {
		if err := d.activeFile.Close(); err != nil {
			return err
		}
	}
	for _, f := range d.olderFiles {
		if err := f.Close(); err != nil {
			return err // Return first error
		}
	}
	return nil
}

// Merge は古いデータファイルを1つに統合し、不要な領域を解放します。
func (d *DB) Merge() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 1. マージ対象（olderFiles）の確認
	if len(d.olderFiles) == 0 {
		return nil // マージするものがない
	}

	var mergeIDs []int
	for id := range d.olderFiles {
		mergeIDs = append(mergeIDs, id)
	}
	sort.Ints(mergeIDs)
	targetID := mergeIDs[0] // 最も若い番号をマージ後のIDとして再利用する

	// 2. 一時ファイルの作成
	tempName := "merge.data"
	tempPath := filepath.Join(d.dirPath, tempName)
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		// 関数終了時にまだ一時ファイルが開いていれば閉じる（エラーパス用）
		// 成功時は明示的に閉じるのでエラーになるが無視してよい
		_ = tempFile.Close()
		// 残っていれば削除（成功時はリネームされている）
		_ = os.Remove(tempPath)
	}()

	// 3. 有効なキーを一時ファイルに書き写す
	newKeyPos := make(map[string]RecordPos)
	var writeOffset int64

	for key, pos := range d.keyDir {
		// ActiveFileにあるキーは対象外
		if pos.FileID == d.activeFileID {
			continue
		}

		// 値の読み出し (olderFilesのいずれかにあるはず)
		file, ok := d.olderFiles[pos.FileID]
		if !ok {
			return errors.New("file not found during merge")
		}

		// Header Read (20 bytes)
		header := make([]byte, 20)
		if _, err := file.ReadAt(header, pos.Offset); err != nil {
			return err
		}
		keySize := binary.BigEndian.Uint32(header[12:16])
		valSize := binary.BigEndian.Uint32(header[16:20])

		totalSize := 20 + int64(keySize) + int64(valSize)
		data := make([]byte, totalSize)
		if _, err := file.ReadAt(data, pos.Offset); err != nil {
			return err
		}

		// チェックサム検証 (The Guardian)
		storedCRC := binary.BigEndian.Uint32(data[0:4])
		if crc32.ChecksumIEEE(data[4:]) != storedCRC {
			return ErrDataCorruption
		}

		// 書き込み
		if _, err := tempFile.Write(data); err != nil {
			return err
		}

		// 新しい位置を記録 (FileIDは後でtargetIDになる)
		newKeyPos[key] = RecordPos{FileID: targetID, Offset: writeOffset}
		writeOffset += totalSize
	}

	// 4. ファイル操作とスワップ
	if err := tempFile.Sync(); err != nil {
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}

	// 古いファイルを全て閉じて削除
	for _, id := range mergeIDs {
		// activeFileはolderFilesに入っていないはずだが念のためチェック
		if id == d.activeFileID {
			continue
		}

		f := d.olderFiles[id]
		if err := f.Close(); err != nil {
			return err
		}
		delete(d.olderFiles, id) // マップから削除

		oldPath := filepath.Join(d.dirPath, fmt.Sprintf("%d.data", id))
		if err := os.Remove(oldPath); err != nil {
			return err
		}
	}

	// 一時ファイルを正規の名前にリネーム
	targetPath := filepath.Join(d.dirPath, fmt.Sprintf("%d.data", targetID))
	if err := os.Rename(tempPath, targetPath); err != nil {
		return err
	}

	// 新しいファイルを読み込み専用で開いて olderFiles に登録
	newFile, err := os.OpenFile(targetPath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	d.olderFiles[targetID] = newFile

	// 5. keyDirの更新
	for key, pos := range newKeyPos {
		d.keyDir[key] = pos
	}

	return nil
}
