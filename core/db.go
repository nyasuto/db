package db

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

var dbFile = "db.db"

const int32Size = 4

func skipChunk(offset int64, file *os.File) (int64, error) {
	var length int32
	offset -= int64(int32Size)

	_, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		fmt.Println("Error seeking in file:", err)
		return 0, err
	}

	err = binary.Read(file, binary.LittleEndian, &length)
	if err != nil {
		fmt.Println("Error reading from file:", err)
		return 0, err
	}
	offset -= int64(length)

	return offset, nil
}

func skipChunkFromMemory(offset int32, contents []byte) (int32, error) {
	var length int32
	offset -= int32Size

	length = int32(binary.LittleEndian.Uint32(contents[offset : offset+int32Size]))
	offset -= length

	return offset, nil
}
func readChunkFromMemory(offset int32, contents []byte) (string, int32, error) {

	var length int32
	offset -= int32Size

	length = int32(binary.LittleEndian.Uint32(contents[offset : offset+int32Size]))
	offset -= length

	val := contents[offset : offset+length]
	return string(val), offset, nil
}

func readChunk(offset int64, file *os.File) (string, int64, error) {

	var length int32
	offset -= int64(int32Size)

	_, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		fmt.Println("Error seeking in file:", err)
		return "", 0, err
	}

	err = binary.Read(file, binary.LittleEndian, &length)
	if err != nil {
		fmt.Println("Error reading from file:", err)
		return "", 0, err
	}
	offset -= int64(length)

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		fmt.Println("Error seeking in file:", err)
		return "", 0, err
	}

	val := make([]byte, length)

	err = binary.Read(file, binary.LittleEndian, &val)
	if err != nil {
		fmt.Println("Error reading from file:", err)
		return "", 0, err
	}

	return string(val), offset, nil
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

	offset := int32(len(fileContents))

	for offset > 0 {
		// read key
		key, valOffset, err := readChunkFromMemory(offset, fileContents)
		if err != nil {
			fmt.Println("Error reading chunk in file:", err)
			return err
		}
		nextKeyOffset, err := skipChunkFromMemory(valOffset, fileContents)
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
