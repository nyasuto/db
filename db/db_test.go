package db

import (
	"os"
	"testing"
)

const testDBFile = "test_db.txt"

func setupTestDB() {
	os.Remove(testDBFile)
	dbFile = testDBFile

	Set("key1", "value1")
	Set("key2", "value2")
	Set("key3", "value3")
	Set("key2", "hoge")
	Set("key1", "{\"name\":\"huga\"}")

}

func teardownTestDB() {
}
func TestGet(t *testing.T) {
	// Setup
	setupTestDB()
	defer teardownTestDB()

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
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result,err := Get(tt.key)
			if err != nil && tt.key != "key4" {
				t.Errorf("Error(%s)", err)
			}

			if result != tt.expected && tt.key != "key4" {
				t.Errorf("Get(%s) = %s; want %s", tt.key, result, tt.expected)
			}
		})
	}
}
