package main

import (
	"fmt"

	"github.com/nyasuto/db/db"
)

func main() {

	db.Set("hello", "nuhunuhu workd")
	//db.Write("super", "dude")
	// ret, err := db.Read("super")
	//ret, err := db.Read("hello")
	ret, err := db.Get("piyo")

	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}
	fmt.Print(ret)
}
