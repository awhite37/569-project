package main

import (
	"math/rand"
	"hash/crc32"
)

type Node struct {
	id        int
	tokens	 [5]int
	data      [5]map[string]Data
}

type VirtualNode struct {
	position    int
	indexInNode int
	node        *Node
}

type Data struct {
	context	*Context
	data     int
}

type Context struct{
	version	int
	clock		[]*ClockEntry
}

type ClockEntry struct{
	nodeID   int
	counter	int
}

type DB struct {
	nodes  		[]*Node
	ring		   []VirtualNode
	nextId 		int
	tokens 		[]int 
	N           int
	R           int
	W           int
}

func (node *Node) run() {
	//main function for running a node
	for {

	}
}

//create new Dynamo instance
func NewDB(n int, r int, w int) *DB {
	db = &DB{
		nodes:  []*Node{},
		ring:   []int{},
		nextId: 0,
		tokens: []int{},
		N:		  n,
		R:		  r,
		W: 	  w,
	}
	for i := 0; i < 360; i++ {
		db.tokens = append(db.tokens, i)
	}
	return db
}

func (db *DB) getTokens() [5]int{
	//get 5 random positions in the ring
	tokens := make([]int, 5)
	for i := 0; i < 5; i++ {
		pos = rand.Intn(len(db.tokens))]
		tokens[i] = db.tokens[pos]
		db.tokens = append(db.tokens[:pos], db.tokens[pos+1:]...)
	}
	return tokens
}

//add a new node to the hash ring
func (db *DB) AddNode() {
	node := Node{
		id:        db.nextId,
		tokens: 	  getTokens(),
		data:   	  [5]map[string]Data{
						map[string]Data{},
						map[string]Data{},
						map[string]Data{},
						map[string]Data{},
						map[string]Data{},
		},
	}
	db.nextId += 1
	db.nodes = append(db.nodes, &node)
	//add 5 virtual nodes to hash ring for this node
	for j := 0; j < len(node.tokens); j++ {
		vnode := VirtualNode{
			position:    node.tokens[j],
			node:        &node,
			indexInNode: j,
		}
		if len(db.ring) == 0 {
			db.ring = append(db.ring, vnode)
		} else {
			for i, vn := range db.ring {
				if vn.position > node.tokens[j] {
					db.ring = append(db.ring, VirtualNode{})
					copy(db.ring[i+1:], db.ring[i:])
					db.ring[i] = vnode
					break
				}
				if i == len(db.ring)-1 {
					db.ring = append(db.ring, vnode)
				}
			}
		}
	}
	//launch node as goroutine
	go node.run()
}

func (vn VirtualNode) data() *map[string]int {
	return &vn.node.data[vn.indexInNode]
}

func (db *DB) getPreferenceList(key string) []int{
	physicalNodes := []int{}
	prefereceList := []*Node{}
	
	//get next N physical nodes in ring
	keyHash := int(crc32.ChecksumIEEE([]byte(key)))
	keyHash %= 360
	pos := len(db.ring)-1
	coordinator := db.ring[pos]
	for i, _ := range db.ring {
		if db.ring[len(db.ring)-i-1].position < keyHash {
			break
		} else {
			pos = len(db.ring)-i-1
			coordinator = db.ring[pos]
		}
	}
	curr := pos
	for len(prefereceList) < db.N {
		
	}
	
}

//delete a node from the hash ring 
func (db *DB) DeleteNode(node *Node) {
	for i, n := range db.nodes {
		if node == n {
			db.nodes = append(db.nodes[:i], db.nodes[i+1:]...)
			break
		}
	}
	for i, vn := range db.ring {
		if vn.node == node {
			db.ring = append(db.ring[:i], db.ring[i+1:]...)

			toTransfer := vn.data()
			sendTo := db.ring[i].data()

			for key, value := range *toTransfer {
				(*sendTo)[key] = value
			}
		}
	}
}

func (db *DB) getVNodeForKey(key string) VirtualNode {
	keyHash := int(crc32.ChecksumIEEE([]byte(key)))
	keyHash %= 360
	target := db.ring[len(db.ring)-1]
	for i, _ := range db.ring {
		if db.ring[len(db.ring)-i-1].position < keyHash {
			break
		} else {
			target = db.ring[len(db.ring)-i-1]
		}
	}

	return target
}

func (db *DB) getDataForKey(key string) *map[string]int {
	return db.getVNodeForKey(key).data()
}

func (db *DB) GetNodeIdForKey(key string) int {
	return db.getVNodeForKey(key).node.id
}

func (db *DB) Get(key string) Data {
	return (*db.getDataForKey(key))[key]
}

func (db *DB) Put(key string, value Data) {
	(*db.getDataForKey(key))[key] = value
}
