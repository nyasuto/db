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

	key := make([]byte, length)

	err = binary.Read(file, binary.LittleEndian, &key)
	if err != nil {
		fmt.Println("Error reading from file:", err)
		return "", 0, err
	}

	return string(key), offset, nil
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

func GetFromFile(key string) (string, error) {
	file, err := os.Open(dbFile)
	if err != nil {
		log.Fatal("Error opening file:", err)
		return "", err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file stats:", err)
		return "", err
	}
	fileSize := stat.Size()

	offset := fileSize

	for offset > 0 {
		var readKey string
		readKey, offset, err = readChunk(offset, file)
		if err != nil {
			fmt.Println("Error reading chunk in file:", err)
			return "", err
		}

		// fmt.Println("Data read from file (reverse order):", string(readKey))
		if key == string(readKey) {
			val, _, err := readChunk(offset, file)
			return val, err
		}

		offset, err = skipChunk(offset, file)
		if err != nil {
			return "", err
		}

	}

	return "", fmt.Errorf("key {%s} not found", key)
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
	stat, _ := file.Stat()

	offset := stat.Size()

	for offset > 0 {
		// read key
		key, valOffset, err := readChunk(offset, file)
		if err != nil {
			fmt.Println("Error reading chunk in file:", err)
			return err
		}
		nextKeyOffset, err := skipChunk(valOffset, file)
		if err != nil {
			fmt.Println("Error reading chunk in file:", err)
			return err
		}

		if _, exists := memoryIndex[key]; !exists {
			memoryIndex[key] = valOffset
		}
		offset = nextKeyOffset
	}
	// fmt.Print(memoryIndex)
	return nil
}
