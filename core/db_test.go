package db

import (
	"fmt"
	"os"
	"testing"
)

func setupTestDB() error {
	
	// 66 sec if open and close file per write
	for i := 0; i < numOfSegments; i++ {
		dbFiles[i] = fmt.Sprintf("%s%d%s", dbPrefix, i, dbSuffix)
		os.Remove(dbFiles[i])
	}

	err := Set("key1", "value1")
	if err != nil {
		return err
	}
	err = Set("key2", "value2")
	if err != nil {
		return err
	}
	err = Set("key3", "value3")
	if err != nil {
		return err
	}
	err = Set("key2", "hoge")
	if err != nil {
		return err
	}
	err = Set("key1", "{\"name\":\"huga\"}")
	if err != nil {
		return err
	}
	err = Set("key2", "hoge")
	if err != nil {
		return err
	}
	for i := 10; i < 1000000; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("value%d", i)
		err = Set(key, val)
		if err != nil {
			return err
		}
	}

	for i := 10; i < 1000000; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("value%dxxx", i)
		err = Set(key, val)
		if err != nil {
			return err
		}
	}

	for i := 10; i < 1000000; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("value%dzzz", i)
		err = Set(key, val)
		if err != nil {
			return err
		}
	}
	return nil
}

func teardownTestDB() {
	// NOP
}
func TestGet(t *testing.T) {
	// 22.8 sec with DB insertion 1M keys
	// 50000 keys / sec
	// 2.4 sec with memory index
	// 0.48 sec with initialize from memory

	err := setupTestDB()
	if err != nil {
		t.Error(err)
		return
	}

	defer teardownTestDB()

	err = Init()

	if err != nil {
		t.Error(err)
		return 
	}

	// Override the dbFile variable to use the test DB file
	// Test cases
	tests := []struct {
		key      string
		expected string
	}{
		{"key1", "{\"name\":\"huga\"}"},
		{"key2", "hoge"},
		{"key3", "value3"},
		{"key4", "key not found: key4"},
		{"key10", "value10zzz"},
		{"key100", "value100zzz"},
		{"key110", "value110zzz"},
		{"key110", "value110zzz"},
		{"key120", "value120zzz"},
		{"key1000", "value1000zzz"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result, err := Get(tt.key)
			if err != nil && tt.key != "key4" {
				t.Error(err)
			}

			if result != tt.expected && tt.key != "key4" {
				t.Errorf("Get(%s) = %s; want %s", tt.key, result, tt.expected)
			}
		})
	}
}
