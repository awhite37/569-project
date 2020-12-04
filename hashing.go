package main

import (
	"fmt"
	"hash/crc32"
)

const NUM_NODES = 5

func display(db *DB) {
	fmt.Printf("hash ring state:\n")
	for _, vn := range db.ring {
		fmt.Printf("Node id: %d, position: %d\n", vn.node.id, vn.position)
	}
	fmt.Println()
}

func main() {
	db := NewDB(3,2,2)

	for i := 0; i < NUM_NODES; i++ {
		db.AddNode()
	}
	display(db)

	db.put("Maria", 100, &Context{})
	db.put("John", 20, &Context{})
	db.put("Anna", 40, &Context{})
	db.put("Tim", 100, &Context{})
	db.put("Alex", 10, &Context{})

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

}
