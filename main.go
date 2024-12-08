package main

import (
	"fmt"

	db "github.com/nyasuto/db/core"
)

func main() {
	
	err := db.Init()
	if err != nil {
		fmt.Println("Error in Init:", err)
		return
	}
	db.Set("key1", "value1")
	ret, err := db.Get("key1")

	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}
	fmt.Print(ret)

}
