package main

import (
	"math/rand"
	"hash/crc32"
)

type Node struct {
	id        int
	tokens	 [5]int
	data      [5]map[string]Data
	request 	 chan Request
	getReturn chan Data
	putReturn chan int
	doneCh    chan int
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

type Request struct {
	//kind is 0 for get, 1 for put
	kind     int
	key 		string
	data     Data
	prefList []*VirtualNode
	N 			int
	R 			int 
	W 	      int       
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
		//read and handle requests as are received
		request := <-node.request
		if request.type == 0 {
			node.handleGet(request)
		} else {
			node.handlePut(request)
		}
		//wait for done signal before handling more requests
		<-node.doneCh
	}
}

func (node *Node) handleGet(request Request) {
	//signal that request is done being handled
	node.doneCh <- 1
}

func (node *Node) handlePut(request Request) {
	//signal that request is done being handled
	node.doneCh <- 1
}


func (db *DB) get(key string) Data {
	prefList = db.getPreferenceList(key)
	//request handled by coordinator
	prefList[0].sendGetRequest(key, prefList, db.N, db.R, db.W)
	//done when getReturn value is read
	value <-prefList[0].getReturn
	return value

}

func (db *DB) put(key string, data int, context *Context) {
	value = &Data{data:data, context:context}
	prefList = db.getPreferenceList(key)
	//request handled by coordinator
	prefList[0].sendPutRequest(key, value, prefList, db.N, db.R, db.W)
	//done when putReturn signal is read
   <-prefList[0].putReturn
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
		request: make(chan Request, 20)
		getReturn: make(chan Data),
		putReturn: make(chan int),
		doneCh:    make(chan int)
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

func (db *DB) getPreferenceList(key string) []*VirtualNode {
	//find coordinator node
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
	return db.getNextNPhysical(coordinator)
}

func (db *DB) getNextNPhysical(curr *VirtualNode) []*VirtualNode{
	nextN := []*VirtualNode{}
	nextN = append(nextN, curr)
	physicalNodes := map[int]bool{}
	for _, node := range db.nodes{
		physicalNodes[node.id] = false
	}
	physicalNodes[curr.node.id] = true
	curr := curr.position
	//get next N-1 physical nodes in ring 
	for len(nextN) < db.N ; i++ {
		for {
			curr += 1
			if curr >= len(db.ring){
				curr = 0
			}
			next = db.ring[curr]
			if !physicalNodes[next.node.id] {
				physicalNodes[next.node.id] = true
				nextN = append(nextN, next)
				break
			}
		}
	}
	return nextN
}

func (node *Node) sendPutRequest(key string, data Data, prefList []*VirtualNode, N int, R int, W int, ) {
	node.request <- Request{
				type: 1,
				key: key,
				data: Data,
				prefList: prefList,
				N: N,
				R: R,
				W: W,
	}
}

func (node *Node) sendGetRequest(key string, prefList []*VirtualNode, N int, R int, W int, ) {
	node.request <- Request{
				type: 0,
				key: key,
				prefList: prefList,
				N: N,
				R: R,
				W: W,
	}
}


//ignore this stuff for now//

// func (db *DB) getPrevNPhysical(curr *VirtualNode) []*VirtualNode{
// 	prevN := []*VirtualNode{}
// 	prevN = append(prevN, curr)
// 	physicalNodes := map[int]bool{}
// 	for _, node := range db.nodes{
// 		physicalNodes[node.id] = false
// 	}
// 	physicalNodes[curr.node.id] = true
// 	curr := curr.position
// 	//get prev N-1 physical nodes in ring
// 	for len(nextN) < db.N ; i++ {
// 		for {
// 			curr -= 1
// 			if curr < 0 {
// 				curr = len(db.ring) - 1
// 			}
// 			next = db.ring[curr]
// 			if !physicalNodes[next.node.id] {
// 				physicalNodes[next.node.id] = true
// 				prevN = append(prevN, next)
// 				break
// 			}
// 		}
// 	}
// 	return prevN
// }

// //delete a node from the hash ring 
// func (db *DB) DeleteNode(node *Node) {
// 	for i, n := range db.nodes {
// 		if node == n {
// 			db.nodes = append(db.nodes[:i], db.nodes[i+1:]...)
// 			break
// 		}
// 	}
// 	for i, vn := range db.ring {
// 		if vn.node == node {
// 			//transfer data to appropriate nodes
// 			toTransfer := vn.data()
// 			next = i+1
// 			if next >= len(db.ring) {
// 				next = 0
// 			}
// 			nextNodes := getNextNPhysical(db.ring[next)
// 			prevNodes := getPrevNPhysical(vn)
// 			for key, value := range *toTransfer {
// 				vn := getVNodeForKey(key)
// 				for j, n := range prevNodes{
// 					if vn.position == n.position {
// 						break
// 					}
// 				}
// 				sendTo := nextNodes[len(nextNodes)-j].data()
// 				(*sendTo)[key] = value
// 			}
// 		}
// 		//remove virtual node from ring
// 		db.ring = append(db.ring[:i], db.ring[i+1:]...)
// 	}
// // 	//send kill signal to this node so it quits
// // 	//TODO
// // }

// func (db *DB) getVNodeForKey(key string) VirtualNode {
// 	keyHash := int(crc32.ChecksumIEEE([]byte(key)))
// 	keyHash %= 360
// 	target := db.ring[len(db.ring)-1]
// 	for i, _ := range db.ring {
// 		if db.ring[len(db.ring)-i-1].position < keyHash {
// 			break
// 		} else {
// 			target = db.ring[len(db.ring)-i-1]
// 		}
// 	}
// 	return target
// }

// func (db *DB) getDataForKey(key string) *map[string]int {
// 	return db.getVNodeForKey(key).data()
// }

// func (db *DB) GetNodeIdForKey(key string) int {
// 	return db.getVNodeForKey(key).node.id
// }



