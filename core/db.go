package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

var dbFile = "db.db"

const int32Size = 4

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
	var length int32
	offset -= int64(int32Size)

	// Read the length of the chunk
	buf := make([]byte, int32Size)
	_, err := reader.ReadAt(buf, offset)
	if err != nil {
		fmt.Println("Error reading length:", err)
		return "", 0, err
	}
	length = int32(binary.LittleEndian.Uint32(buf))
	offset -= int64(length)

	// Read the chunk data
	buf = make([]byte, length)
	_, err = reader.ReadAt(buf, offset)
	if err != nil {
		fmt.Println("Error reading chunk data:", err)
		return "", 0, err
	}

	return string(buf), offset, nil
}

func Get(key string) (string, error) {
	file, err := os.Open(dbFile)
	if err != nil {
		log.Fatal("Error opening file:", err)
		return "", err
	}
	defer file.Close()

	if _, exists := memoryIndex[key]; !exists {
		return "", fmt.Errorf("key {%s} not found", key)
	}
	offset := memoryIndex[key]
	value, _, err := readChunk(offset, file)

	return value, err
}

func Set(key string, value string) {

	file, err := os.OpenFile(dbFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Error creating file:", err)
		return
	}
	defer file.Close()

	for _, value := range []byte(value) {
		err = binary.Write(file, binary.LittleEndian, value)
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
	}
	err = binary.Write(file, binary.LittleEndian, int32(len(value)))
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}

	for _, value := range []byte(key) {
		err = binary.Write(file, binary.LittleEndian, value)
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
	}
	err = binary.Write(file, binary.LittleEndian, int32(len(key)))
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}

}

var memoryIndex = make(map[string]int64)

func Init() error {

	file, err := os.Open(dbFile)
	if err != nil {
		log.Fatal("Error opening file:", err)
		return err
	}
	defer file.Close()

	fileContents, err := io.ReadAll(file)
	if err != nil {
		log.Fatal("Error reading file:", err)
		return err
	}

	offset := int64(len(fileContents))
	reader := bytes.NewReader(fileContents)

	for offset > 0 {
		// read key
		key, valOffset, err := readChunk(offset, reader)
		if err != nil {
			fmt.Println("Error reading chunk in file:", err)
			return err
		}
		nextKeyOffset, err := skipChunk(valOffset, reader)
		if err != nil {
			fmt.Println("Error reading chunk in file:", err)
			return err
		}

		if _, exists := memoryIndex[key]; !exists {
			memoryIndex[key] = int64(valOffset)
		}
		offset = nextKeyOffset
	}
	// fmt.Print(memoryIndex)
	return nil
}
