package main

import (
	"fmt"

	db "github.com/nyasuto/db/core"
)

func main() {

	// ret, err := db.Read("super")
	//ret, err := db.Read("hello")
	err := db.Init()
	if err != nil {
		fmt.Println("Error in Init:", err)
		return
	}

	ret, err := db.Get("key1")

	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}
	fmt.Print(ret)
}
