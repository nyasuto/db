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

func skipChunk(offset int64, reader io.ReaderAt) (int64, error) {
	var length int32
	offset -= int64(int32Size)

	// Read the length of the chunk
	buf := make([]byte, int32Size)
	_, err := reader.ReadAt(buf, offset)
	if err != nil {
		fmt.Println("Error reading length:", err)
		return 0, err
	}
	length = int32(binary.LittleEndian.Uint32(buf))
	offset -= int64(length)

	return offset, nil
}

func readChunk(offset int64, reader io.ReaderAt) (string, int64, error) {
	var length int64
	offset -= int64(int32Size)

	// Read the length of the chunk
	buf := make([]byte, int32Size)
	_, err := reader.ReadAt(buf, offset)
	if err != nil {
		fmt.Println("Error reading length:", err)
		return "", 0, err
	}
	length = int64(binary.LittleEndian.Uint32(buf))
	offset -= int64(length)

	// Read the chunk data
	if length > 1000 {
		fmt.Println("Error something bad.")
		return "", 0, err
	}
	buf = make([]byte, length)
	_, err = reader.ReadAt(buf, offset)
	if err != nil {
		fmt.Println("Error reading chunk data:", err)
		return "", 0, err
	}

	return string(buf), offset, nil
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
			value, _, err := readChunk(offset, file)
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
				value, _, err := readChunk(offset, file)
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

	for _, value := range []byte(value) {
		err = binary.Write(file, binary.LittleEndian, value)
		if err != nil {
			return writeError(err)
		}
	}
	err = binary.Write(file, binary.LittleEndian, int32(len(value)))
	if err != nil {
		return writeError(err)
	}

	for _, value := range []byte(key) {
		err = binary.Write(file, binary.LittleEndian, value)
		if err != nil {
			return writeError(err)
		}
	}
	err = binary.Write(file, binary.LittleEndian, int32(len(key)))
	if err != nil {
		return writeError(err)
	}

	return nil
}
func writeError(err error) error {
	return fmt.Errorf("error writing to file: %s", err)
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
		key, valOffset, err := readChunk(offset, reader)
		if err != nil {
			return err
		}
		nextKeyOffset, err := skipChunk(valOffset, reader)
		if err != nil {
			return err
		}

		if _, exists := memoryIndex[segment][key]; !exists {
			memoryIndex[segment][key] = int64(valOffset)
		}
		offset = nextKeyOffset

		if len(memoryIndex[segment]) != 0 {
			currentSegment = segment
		}
	}
	return nil
}
