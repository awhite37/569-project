package main

import (
	"fmt"
	"hash/crc32"
	"math/rand"
)

type Node struct {
	id        int
	tokens    [5]int
	data      [5]map[string][]Data
	request   chan Request
	getReturn chan []Data
	putReturn chan int
	putDone   chan int
	getDone   chan []Data
	doneCh    chan int
	table         []*TableEntry
	tableInput    chan []*TableEntry
}

type VirtualNode struct {
	position    int
	indexInNode int
	node        *Node
}

type Data struct {
	context *Context
	data    int
}

type Context struct {
	version int
	clock   []*ClockEntry
}

type ClockEntry struct {
	nodeID  int
	counter int
}

type Request struct {
	//kind is 0 for get, 1 for put
	kind          int
	prefListIndex int
	key           string
	data          *Data
	prefList      []*VirtualNode
	N             int
	R             int
	W             int
}

type DB struct {
	nodes  []*Node
	ring   []VirtualNode
	nextId int
	tokens []int
	N      int
	R      int
	W      int
}

func (node *Node) run(db *DB) {
	//main function for running a node
	//launch gossip protocol
	go node.gossip(db)
	for {
		//read and handle requests as are received
		request := <-node.request
		if request.kind == 0 {
			node.handleGet(request)
		} else {
			node.handlePut(request)
		}
		//wait for done signal before handling more requests
		<-node.doneCh
	}
}

func dataMapToDataArray(m map[int]Data) []Data {
	i := 0
	a := make([]Data,len(m))
	for _,val := range m {
		a[i] = val
		i += 1
	}
	return a
}

func consolidateDataVals(vals []Data) []Data {
	m := make(map[int]Data)
	for _,val := range vals {
		for i,clockEntry := range val.context.clock {
			stored := m[clockEntry.nodeID]
			if _,ok := m[clockEntry.nodeID]; !ok {
				m[clockEntry.nodeID] = val
				continue
			}
			if clockEntry.counter > stored.context.clock[i].counter {
				m[clockEntry.nodeID] = val
			}
		}
	}
	return dataMapToDataArray(m)
}

func (node *Node) handleGet(request Request) {
	//check if this node is the coordinator
	if request.prefListIndex == 0 {
		//clear out old responses
		for len(node.getDone) > 0 {
			<-node.getDone
		}
		vals := []Data{}
		responses := 0
		//request get for nodes in this keys preference list
		for i, vn := range request.prefList {
			//local get
			if i == 0 {
				val := (*vn.data())[request.key]
				vals = append(vals, val...)
				responses++
			} else { 
				//send request to healthy nodes
				if !node.hasFailed(vn.node.id) {
					vn.node.sendGetRequest(i, request.key, request.prefList, request.N, request.R, request.W)
				}
			}
		}
		//wait for R get responses
		for responses < request.R {
			val := <-node.getDone
			vals = append(vals, val...)
			responses += 1
		}
		//TODO: consolidate vals
		node.getReturn <- consolidateDataVals(vals)
	} else { //handle local put and respond to coordinator
		vals := (*request.prefList[request.prefListIndex].data())[request.key]
		request.prefList[0].node.getDone <- vals
	}
	//signal that request is done being handled
	node.doneCh <- 1
}

func (node *Node) handlePut(request Request) {
	//check if this node is the coordinator
	if request.prefListIndex == 0 {
		//update vector clock
		node.addToClock(request.data)
		//clear out old responses
		for len(node.putDone) > 0 {
			<-node.putDone
		}
		responses := 0
		//request put for nodes in this keys preference list
		for i, vn := range request.prefList {
			//local put
			if i == 0 {
				//TODO: consolidate on put
				(*vn.data())[request.key] = append((*vn.data())[request.key], *request.data)
				responses++
			} else { 
				//send request to healthy nodes
				if !node.hasFailed(vn.node.id) {
					vn.node.sendPutRequest(i, request.key, request.data, request.prefList, request.N, request.R, request.W)
				}
			}
		}
		//wait for W put responses
		for responses < request.W {
			responses += <-node.putDone
		}
		node.putReturn <- 1
	} else { //handle local put and respond to coordinator
		//TODO: consolidate on put
		(*request.prefList[request.prefListIndex].data())[request.key] = append((*request.prefList[request.prefListIndex].data())[request.key], *request.data)
		request.prefList[0].node.putDone <- 1
	}
	//signal that request is done being handled
	node.doneCh <- 1
}

func (db *DB) get(key string) []Data {
	prefList := db.getPreferenceList(key)
	//request handled by coordinator
	prefList[0].node.sendGetRequest(0, key, prefList, db.N, db.R, db.W)
	//done when getReturn value is read
	value := <-prefList[0].node.getReturn
	return value

}

func (db *DB) put(key string, data int, context *Context) {
	//initialize new object version
	clock := []*ClockEntry{}
	for _, entry := range context.clock {
		clock = append(clock, &ClockEntry{nodeID: entry.nodeID, counter: entry.counter})
	}
	newContext := &Context{version: context.version + 1, clock: clock}
	value := &Data{data: data, context: newContext}
	prefList := db.getPreferenceList(key)
	//request handled by coordinator
	prefList[0].node.sendPutRequest(0, key, value, prefList, db.N, db.R, db.W)
	//done when putReturn signal is read
	<-prefList[0].node.putReturn
}

func (node *Node) addToClock(data *Data) {
	clock := data.context.clock
	for _, entry := range clock {
		if entry.nodeID == node.id {
			entry.counter += 1
			return
		}
	}
	newEntry := &ClockEntry{nodeID: node.id, counter: 1}
	data.context.clock = append(clock, newEntry)
}

//create new Dynamo instance
func NewDB(n int, r int, w int) *DB {
	db := &DB{
		nodes:  []*Node{},
		ring:   []VirtualNode{},
		nextId: 0,
		tokens: []int{},
		N:      n,
		R:      r,
		W:      w,
	}
	for i := 0; i < 360; i++ {
		db.tokens = append(db.tokens, i)
	}
	return db
}

func (db *DB) getTokens() [5]int {
	//get 5 random positions in the ring
	tokens := [5]int{}
	for i := 0; i < 5; i++ {
		pos := rand.Intn(len(db.tokens))
		tokens[i] = db.tokens[pos]
		db.tokens = append(db.tokens[:pos], db.tokens[pos+1:]...)
	}
	return tokens
}

//add a new node to the hash ring
func (db *DB) AddNode() {
	node := Node{
		id:     db.nextId,
		tokens: db.getTokens(),
		data: [5]map[string][]Data{
			map[string][]Data{},
			map[string][]Data{},
			map[string][]Data{},
			map[string][]Data{},
			map[string][]Data{},
		},
		request:   make(chan Request, 20),
		getReturn: make(chan []Data, 1),
		putReturn: make(chan int, 1),
		putDone:   make(chan int, db.N*2),
		getDone:   make(chan []Data, db.N*2),
		doneCh:    make(chan int, 1),
		table:	  make([]*TableEntry, NUM_NODES),
		tableInput:make(chan []*TableEntry, NUM_NODES*2),
	}
	for j := 0; j < NUM_NODES; j++ {
		node.table[j] = &TableEntry{id: j, hb: 0, t: 0}
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
	go node.run(db)
}

func (vn VirtualNode) data() *map[string][]Data {
	return &vn.node.data[vn.indexInNode]
}

func (db *DB) getPreferenceList(key string) []*VirtualNode {
	//find coordinator node
	keyHash := int(crc32.ChecksumIEEE([]byte(key)))
	keyHash %= 360
	pos := len(db.ring) - 1
	coordinator := db.ring[pos]
	for i, _ := range db.ring {
		if db.ring[len(db.ring)-i-1].position < keyHash {
			break
		} else {
			pos = len(db.ring) - i - 1
			coordinator = db.ring[pos]
		}
	}
	return db.getNextNPhysical(coordinator, pos)
}

func (db *DB) getNextNPhysical(vn VirtualNode, pos int) []*VirtualNode {
	nextN := []*VirtualNode{}
	nextN = append(nextN, &vn)
	physicalNodes := map[int]bool{}
	for _, node := range db.nodes {
		physicalNodes[node.id] = false
	}
	physicalNodes[vn.node.id] = true
	curr := pos
	//get next N-1 physical nodes in ring
	for len(nextN) < db.N {
		for {
			curr += 1
			if curr >= len(db.ring) {
				curr = 0
			}
			next := db.ring[curr]
			if !physicalNodes[next.node.id] {
				physicalNodes[next.node.id] = true
				nextN = append(nextN, &next)
				break
			}
		}
	}
	return nextN
}

func (db *DB) displayRing() {
	fmt.Printf("hash ring state:\n")
	for _, vn := range db.ring {
		fmt.Printf("Node id: %d, position: %d\n", vn.node.id, vn.position)
	}
	fmt.Println()
}

func (db *DB) displayData() {
	fmt.Printf("current state:\n")
	for _, vn := range db.ring {
		fmt.Printf("Node id: %d, position: %d\n", vn.node.id, vn.position)
		data := *vn.data()
		for k, v := range data {
			fmt.Printf("\tkey %s: \n", k)
			for _, ver := range v {
				fmt.Printf("\t(value: %d, version: %d, clock: ", ver.data, ver.context.version)
				for _, entry := range ver.context.clock {
					fmt.Printf("(%d, %d)", entry.nodeID, entry.counter)
				}
				fmt.Printf(")\n")
			}
		}
	}
	fmt.Println()
}

func (db *DB) displayPreference(key string) {
	fmt.Printf("key: %s, hash value: %d\n", key, int(crc32.ChecksumIEEE([]byte(key)))%360)
	prefList := db.getPreferenceList(key)
	for _, node := range prefList {
		fmt.Printf("node: %d, position: %d\t", node.node.id, node.position)
	}
	fmt.Println()
}

func (node *Node) sendPutRequest(index int, key string, data *Data, prefList []*VirtualNode, N int, R int, W int) {
	node.request <- Request{
		kind:          1,
		prefListIndex: index,
		key:           key,
		data:          data,
		prefList:      prefList,
		N:             N,
		R:             R,
		W:             W,
	}
}

func (node *Node) sendGetRequest(index int, key string, prefList []*VirtualNode, N int, R int, W int) {
	node.request <- Request{
		kind:          0,
		prefListIndex: index,
		key:           key,
		prefList:      prefList,
		N:             N,
		R:             R,
		W:             W,
	}
}
