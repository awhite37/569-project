package main

import (
	"fmt"
	"hash/crc32"
)

const NUM_NODES = 5


func main() {
	db := NewDB(3,2,2)

	for i := 0; i < NUM_NODES; i++ {
		db.AddNode()
	}

	db.put("Maria", 100, &Context{version:0})
	db.put("John", 20, &Context{version:0})
	db.put("Anna", 40, &Context{version:0})
	db.put("Tim", 100, &Context{version:0})
	db.put("Alex", 10, &Context{version:0})

	keys := [5]string{
		"Maria", "John", "Anna", "Tim", "Alex",
	}

	fmt.Println("Preference lists for keys:")
	for _, k := range keys {
		fmt.Printf("key: %s, hash value: %d\n", k, int(crc32.ChecksumIEEE([]byte(k)))%360)
		prefList := db.getPreferenceList(k)
		for _, node := range prefList {
			fmt.Printf("node: %d, position: %d\t", node.node.id, node.position)
		}
		fmt.Println()
	}
	fmt.Println()

	db.displayData()
}
