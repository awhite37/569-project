package main

import (
	"fmt"
)

const NUM_NODES = 5

func main() {
	db := NewDB(3, 2, 2)

	for i := 0; i < NUM_NODES; i++ {
		db.AddNode()
	}

	db.put("Maria", 100, &Context{version: 0})
	db.put("John", 20, &Context{version: 0})
	db.put("Anna", 40, &Context{version: 0})
	db.put("Tim", 100, &Context{version: 0})
	db.put("Alex", 10, &Context{version: 0})

	keys := [5]string{
		"Maria", "John", "Anna", "Tim", "Alex",
	}

	fmt.Println("Preference lists for keys:")
	for _, k := range keys {
		db.displayPreference(k)
	}
	fmt.Println()

	db.displayData()

	//update value at key
	data := db.get("Maria")
	db.put("Maria", 200, data[0].context)

	db.displayData()
}
