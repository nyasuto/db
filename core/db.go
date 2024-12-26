package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

var dbPrefix = "db"
var dbSuffix = ".db"
var tmpDbFile = "tmp.db"

const int32Size = 4
const numOfSegments = 3
//lint:ignore U1000 will be used in the future
const sizeOfSegment = 50 // 50MB

var currentSegment = 0
var memoryIndex [numOfSegments]map[string]int64
var dbFiles [numOfSegments]string
var tmpMemoryIndex map[string]int64

func init() {

	for i := 0; i < numOfSegments; i++ {
		dbFiles[i] = fmt.Sprintf("%s%d%s", dbPrefix, i, dbSuffix)
		memoryIndex[i] = make(map[string]int64)
	}
}

func readChunk(offset int64, reader io.ReaderAt) (string, string, int64, error) {
	// 先にチャンク全体サイズを読み取り、offsetを更新
	offset -= int64(4)
	buf := make([]byte, 4)
	_, err := reader.ReadAt(buf, offset)
	if err != nil {
		return "", "", 0, fmt.Errorf("error reading total length: %w", err)
	}
	totalLen := int64(binary.LittleEndian.Uint32(buf))
	offset -= totalLen

	// チャンク本体を読み込み
	buf = make([]byte, totalLen)
	_, err = reader.ReadAt(buf, offset)
	if err != nil {
		return "", "", 0, fmt.Errorf("error reading chunk data: %w", err)
	}

	// bufからキー長・キー・値長・値を順に取り出す
	var keyLen, valLen int32
	pos := 0
	keyLen = int32(binary.LittleEndian.Uint32(buf[pos : pos+4]))
	pos += 4
	keyBytes := buf[pos : pos+int(keyLen)]
	pos += int(keyLen)

	valLen = int32(binary.LittleEndian.Uint32(buf[pos : pos+4]))
	pos += 4
	valBytes := buf[pos : pos+int(valLen)]
	pos += int(valLen)

	return string(keyBytes), string(valBytes), offset, nil
}

type mode int

var normal mode = 0
var tmp mode = 1
var currentMode = normal

func Get(key string) (string, error) {

	if currentMode == tmp {
		file, err := os.Open(tmpDbFile)
		if err != nil {
			return "", fmt.Errorf("error opening file: %s", err)
		}
		defer file.Close()

		if _, exists := tmpMemoryIndex[key]; exists {
			offset := tmpMemoryIndex[key]
			_, value, err := readChunk(offset, file)
			return value, err
		}
	} else {
		for i := currentSegment; i >= 0; i-- {
			if _, exists := memoryIndex[i][key]; exists {
				file, err := os.Open(dbFiles[i])
				if err != nil {
					return "", fmt.Errorf("error opening file: %s", err)
				}
				defer file.Close()

				offset := memoryIndex[i][key]
				_, value, err := readChunk(offset, file)
				return value, err
			}
		}
	}

	return "", fmt.Errorf("key {%s} not found", key)

}

func getDbFile() string {
	if currentMode == tmp {
		return tmpDbFile
	} else {
		return dbFiles[currentSegment]
	}
}

func Set(key string, value string) error {
	file, err := os.OpenFile(getDbFile(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error creating file: %s", err)
	}
	defer file.Close()

	// チャンク全体サイズ（キー+値+それぞれのサイズ分）
	totalLen := int32(len(key) + len(value) + 4 + 4) // keyLen(4byte) + valueLen(4byte)

	// チャンク末尾に書き込む4バイト分を先に書く
	err = binary.Write(file, binary.LittleEndian, totalLen)
	if err != nil {
		return fmt.Errorf("error writing total length: %s", err)
	}

	// キー長とキーを書き込み
	err = binary.Write(file, binary.LittleEndian, int32(len(key)))
	if err != nil {
		return fmt.Errorf("error writing key length: %s", err)
	}
	_, err = file.Write([]byte(key))
	if err != nil {
		return fmt.Errorf("error writing key: %s", err)
	}

	// 値長と値を書き込み
	err = binary.Write(file, binary.LittleEndian, int32(len(value)))
	if err != nil {
		return fmt.Errorf("error writing value length: %s", err)
	}
	_, err = file.Write([]byte(value))
	if err != nil {
		return fmt.Errorf("error writing value: %s", err)
	}

	return nil
}

func Init() error {
	currentSegment = 0

	for i := 0; i < numOfSegments; i++ {
		if err := initializeSegment(i); err != nil {
			return err
		}
	}
	return nil
}

func initializeSegment(segment int) error {
	if _, err := os.Stat(dbFiles[segment]); os.IsNotExist(err) {
		file, err := os.Create(dbFiles[segment])
		if err != nil {
			return err
		}
		file.Close()
		return nil
	}

	file, err := os.Open(dbFiles[segment])
	if err != nil {
		return err
	}
	defer file.Close()

	fileContents, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	offset := int64(len(fileContents))
	reader := bytes.NewReader(fileContents)

	for offset > 0 {
		k, v, nextOffset, err := readChunk(offset, reader)
		if err != nil {
			return err
		}
		memoryIndex[segment][k] = (nextOffset + 4) // offset計算例: 4はchunk末尾サイズ

		offset = nextOffset
		if len(memoryIndex[segment]) != 0 {
			currentSegment = segment
		}
	}
	return nil
}
