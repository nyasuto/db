package db

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	manager, err := NewDefaultSegmentManager()

	assert.Nil(t, err)
	assert.NotNil(t, manager)

	// 単純な実装　セグメントを一個ずつヒットするまで読む
	// 100 : 10sec, 10 read/sec
	// 1000	: 99sec, 10 read/sec

	// インデックス実装版 no offset
	// 100: 0.833sec, 120 read/sec
	// 1000: 2.894sec, 345 read/sec
	// 10000: 23.930sec, 418 read/sec

	// インデックス実装版 with offset
	// バイトオフセットを使えないのでファイルの中のキーの位置によって性能が大激変する
	// 100: 0.804sec, 124 read/sec
	// 1000: 2.867sec, 349 read/sec
	// 10000: 23sec, 434 read/sec
	// offset := 964662
	for i := 1; i <= 10000; i++ {
		//key := "key" + strconv.Itoa(offset-i)
		//value := "value" + strconv.Itoa(offset-i)
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)

		val, err := manager.Read(key)
		assert.Nil(t, err)
		assert.Equal(t, value, val)
	}
}

/*
func TestWrite(t *testing.T) {

	// Ensure the directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Error("Error creating directory:", err)
		return
	}

	manager, err := NewSegmentManager(dir, maxSize)
	if err != nil {
		t.Error("Error creating segment manager:", err)
		return
	}

	// Write data
	for i := 1; i <= 1000000; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		err := manager.Write(key, value)
		if err != nil {
			fmt.Println("Error writing to segment:", err)
		}
	}

}
*/
