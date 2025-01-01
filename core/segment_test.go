package db

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	manager := Write()

	if manager == nil {
		manager, _ = NewDefaultSegmentManager()
	}

	assert.NotNil(t, manager)

	// インデックス実装版 no offset
	//  10k: 0.614sec, 16k read/sec
	// 100k: 0.766sec, 130k read/sec
	//   1m: 2.196sec, 454k read/sec
	testSize := 1000 * 1000

	for i := 1; i <= testSize; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)

		val, err := manager.Read(key)
		assert.Nil(t, err)
		assert.Equal(t, value, val)
	}

	for i := 1; i <= testSize; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i + 10000)

		err := manager.Write(key, value)
		assert.Nil(t, err)
	}

	for i := 1; i <= testSize; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i + 10000)

		val, err := manager.Read(key)
		assert.Nil(t, err)
		assert.Equal(t, value, val)
	}

	manager.CloseAll()
}

func Write() *SegmentManager {
	_, err := os.Stat(dir)
	if err == nil {
		files, err := os.ReadDir(dir)
		if err != nil {
			return nil
		}
		if len(files) != 0 {
			return nil
		}
	}
	// Ensure the directory exists
	_ = os.MkdirAll(dir, 0755)

	manager, err := NewSegmentManager(dir, maxSize)
	if err != nil {
		return nil
	}

	// Write data
	// 10 M
	for i := 1; i <= 1*1000*1000; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		err := manager.Write(key, value)
		if err != nil {
			fmt.Println("Error writing to segment:", err)
		}
	}
	return manager
}
