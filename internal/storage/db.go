package storage

import (
	"bufio"
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
	olderFiles   map[int]Reader // Changed to Reader interface (DiskReader or MmapReader)
	keyDir       map[string]RecordPos
	writeOffset  int64
}

// NewDB は指定されたディレクトリパスでデータベースを開きます。
func NewDB(dirPath string) (*DB, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

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
		olderFiles: make(map[int]Reader),
		keyDir:     make(map[string]RecordPos),
	}

	// 全ファイルをロードしてインデックス構築 (Mmapとしてロードされる)
	for _, id := range fileIDs {
		if err := db.loadFile(id); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	// アクティブファイルの設定
	if len(fileIDs) == 0 {
		// 新規作成
		if err := db.newActiveFile(0); err != nil {
			_ = db.Close()
			return nil, err
		}
	} else {
		// 最後のファイルをアクティブにする
		lastID := fileIDs[len(fileIDs)-1]

		// olderFilesから取り出し、クローズする (Mmap -> Disk への切り替え)
		if reader, ok := db.olderFiles[lastID]; ok {
			_ = reader.Close()
			delete(db.olderFiles, lastID)
		}

		// Active Fileとして再オープン (RW/Append)
		path := filepath.Join(db.dirPath, fmt.Sprintf("%d.data", lastID))
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			_ = db.Close()
			return nil, err
		}

		db.activeFile = file
		db.activeFileID = lastID

		info, err := file.Stat()
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		db.writeOffset = info.Size()
	}

	return db, nil
}

func (d *DB) loadFile(id int) error {
	dataPath := filepath.Join(d.dirPath, fmt.Sprintf("%d.data", id))

	// Older Files は MmapReader で開く (高速読み込み)
	mmapReader, err := NewMmapReader(dataPath)
	if err != nil {
		return err
	}
	d.olderFiles[id] = mmapReader

	// Hintファイルの存在確認
	hintPath := filepath.Join(d.dirPath, fmt.Sprintf("%d.hint", id))
	if _, err := os.Stat(hintPath); err == nil {
		return d.loadHintFile(id, hintPath)
	}

	// Hintが無ければデータファイルからインデックス構築
	if err := d.loadKeyDir(id, mmapReader); err != nil {
		return err
	}
	return nil
}

func (d *DB) loadHintFile(fileID int, path string) error {
	// Hint File も MmapReader を使うと高速だが、一時的なシーケンシャルリードなので
	// os.Open + bufio でも十分高速。
	// ここでは既存の実装(Buffered I/O)を維持する。
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	var offset int64

	reader := bufio.NewReader(file)

	for offset < fileSize {
		// [CRC(4)][Ts(8)][KSz(4)][VSz(4)][Offset(8)] = 28 bytes
		header := make([]byte, 28)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		storedCRC := binary.BigEndian.Uint32(header[0:4])
		keySize := binary.BigEndian.Uint32(header[12:16])
		dataOffset := binary.BigEndian.Uint64(header[20:28])

		key := make([]byte, keySize)
		if _, err := io.ReadFull(reader, key); err != nil {
			return err
		}

		// CRC検証: Header[4:] + Key
		checkBuf := make([]byte, 24+keySize)
		copy(checkBuf[0:24], header[4:])
		copy(checkBuf[24:], key)

		if crc32.ChecksumIEEE(checkBuf) != storedCRC {
			return ErrDataCorruption
		}

		d.keyDir[string(key)] = RecordPos{FileID: fileID, Offset: int64(dataOffset)}
		offset += 28 + int64(keySize)
	}
	return nil
}

func (d *DB) newActiveFile(id int) error {
	// 既存のActiveFileがあれば、Olderへ移動 (Disk -> Mmap)
	if d.activeFile != nil {
		// Sync & Close current active file
		_ = d.activeFile.Sync()
		oldPath := d.activeFile.Name()
		_ = d.activeFile.Close()

		// Reopen as MmapReader
		mmapReader, err := NewMmapReader(oldPath)
		if err != nil {
			return err
		}
		d.olderFiles[d.activeFileID] = mmapReader
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
// file引数を Reader インターフェースに変更
func (d *DB) loadKeyDir(fileID int, file Reader) error {
	fileSize := file.Size()
	var offset int64

	// Reader (ReaderAt) から bufio.Reader を作るために SectionReader を使用
	r := io.NewSectionReader(file, 0, fileSize)
	reader := bufio.NewReader(r)

	for offset < fileSize {
		// Header (20 bytes)
		header := make([]byte, 20)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		storedCRC := binary.BigEndian.Uint32(header[0:4])
		keySize := int64(binary.BigEndian.Uint32(header[12:16]))
		valSizeRaw := binary.BigEndian.Uint32(header[16:20])

		key := make([]byte, keySize)
		if _, err := io.ReadFull(reader, key); err != nil {
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
			// Read Value into checkData
			if _, err := io.ReadFull(reader, checkData[16+keySize:]); err != nil {
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
	var file Reader
	if pos.FileID == d.activeFileID {
		// ActiveFile is *os.File, we need to wrap it if we want to use Reader interface?
		// But activeFile is *os.File. We can just use it directly or wrap.
		// Wait, ReadAt signature is same.
		// We can cast? No. *os.File implements Reader interface?
		// Reader interface requires: ReadAt, Close, Size. *os.File has ReadAt, Close.
		// Size() is NOT in *os.File. MmapReader has Size().
		// We need to implement Size() for *os.File wrapper?
		// Let's use DiskReader wrapper for ActiveFile?
		// Or just use local variable with type interface{ ReadAt(...) ... }
		// But simplicity: just call ReadAt directly.

		// The issue: We need a common way to ReadAt.
		// activeFile (*os.File) has ReadAt.
		file = NewDiskReader(d.activeFile) // Wait, Size() calls Stat(). It's ok.
	} else {
		var exists bool
		file, exists = d.olderFiles[pos.FileID]
		if !exists {
			return nil, errors.New("file not found: internal error")
		}
	}

	// Read header and data
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

	// 2. 一時ファイルの作成 (Data & Hint)
	tempDataName := "merge.data"
	tempHintName := "merge.hint"
	tempDataPath := filepath.Join(d.dirPath, tempDataName)
	tempHintPath := filepath.Join(d.dirPath, tempHintName)

	tempDataFile, err := os.OpenFile(tempDataPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	tempHintFile, err := os.OpenFile(tempHintPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		_ = tempDataFile.Close()
		_ = os.Remove(tempDataPath)
		return err
	}

	defer func() {
		// エラーパス用クリーンアップ
		_ = tempDataFile.Close()
		_ = tempHintFile.Close()
		// 成功時はリネームされているため削除は失敗するが無視してよい
		if _, err := os.Stat(tempDataPath); err == nil {
			_ = os.Remove(tempDataPath)
		}
		if _, err := os.Stat(tempHintPath); err == nil {
			_ = os.Remove(tempHintPath)
		}
	}()

	// 3. 有効なキーを一時ファイルに書き写す
	newKeyPos := make(map[string]RecordPos)
	var writeOffset int64

	for key, pos := range d.keyDir {
		// ActiveFileにあるキーは対象外
		if pos.FileID == d.activeFileID {
			continue
		}

		// 値の読み出し
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

		// Checksum (Guardian)
		storedCRC := binary.BigEndian.Uint32(data[0:4])
		if crc32.ChecksumIEEE(data[4:]) != storedCRC {
			return ErrDataCorruption
		}

		// --- Data Write ---
		if _, err := tempDataFile.Write(data); err != nil {
			return err
		}

		// --- Hint Write ---
		ts := binary.BigEndian.Uint64(header[4:12])

		hintBuf := make([]byte, 28)
		binary.BigEndian.PutUint64(hintBuf[4:12], ts)
		binary.BigEndian.PutUint32(hintBuf[12:16], keySize)
		binary.BigEndian.PutUint32(hintBuf[16:20], valSize)
		binary.BigEndian.PutUint64(hintBuf[20:28], uint64(writeOffset))

		checkBuf := make([]byte, 24+len(key))
		copy(checkBuf[0:24], hintBuf[4:])
		copy(checkBuf[24:], key)
		hintCRC := crc32.ChecksumIEEE(checkBuf)

		binary.BigEndian.PutUint32(hintBuf[0:4], hintCRC)

		// Write Header and Key
		if _, err := tempHintFile.Write(hintBuf); err != nil {
			return err
		}
		if _, err := tempHintFile.Write([]byte(key)); err != nil {
			return err
		}

		newKeyPos[key] = RecordPos{FileID: targetID, Offset: writeOffset}
		writeOffset += totalSize
	}

	// 4. ファイル操作とスワップ
	if err := tempDataFile.Sync(); err != nil {
		return err
	}
	if err := tempHintFile.Sync(); err != nil {
		return err
	}

	if err := tempDataFile.Close(); err != nil {
		return err
	}
	if err := tempHintFile.Close(); err != nil {
		return err
	}

	// 古いデータファイルとヒントファイルを削除
	for _, id := range mergeIDs {
		if id == d.activeFileID {
			continue
		}

		f := d.olderFiles[id]
		_ = f.Close()
		delete(d.olderFiles, id)

		oldDataPath := filepath.Join(d.dirPath, fmt.Sprintf("%d.data", id))
		_ = os.Remove(oldDataPath)

		oldHintPath := filepath.Join(d.dirPath, fmt.Sprintf("%d.hint", id))
		_ = os.Remove(oldHintPath)
	}

	// Rename Temp -> Target
	targetDataPath := filepath.Join(d.dirPath, fmt.Sprintf("%d.data", targetID))
	targetHintPath := filepath.Join(d.dirPath, fmt.Sprintf("%d.hint", targetID))

	if err := os.Rename(tempDataPath, targetDataPath); err != nil {
		return err
	}
	if err := os.Rename(tempHintPath, targetHintPath); err != nil {
		return err
	}

	// Re-open compacted file as MmapReader
	newFile, err := NewMmapReader(targetDataPath)
	if err != nil {
		return err
	}
	d.olderFiles[targetID] = newFile

	// 5. Update In-Memory Index
	for key, pos := range newKeyPos {
		d.keyDir[key] = pos
	}

	return nil
}
