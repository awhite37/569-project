package main

import (
	"fmt"
	"sync"
	"math/rand"
	"time"
)

//seconds to wait before updating heartbeat
const X = 1
//seconds to wait before gossiping table
const Y = 1
//seconds to wait before declaring a node failed
const t_fail = 4

type TableEntry struct {
	id 	int
	hb 	int
	t  	int
	mux 	sync.RWMutex
}

func (node *Node) hasFailed(id int) bool {
	node.table[id].mux.RLock()
	status := node.table[id].hb
	node.table[id].mux.RUnlock()
	if status == -1 {
		return true
	}
	return false
}

//update nodes HB and clock periodically
func (node *Node) updateHB() {
	for{
		time.Sleep(X * time.Second)
		node.table[node.id].mux.Lock()
		node.table[node.id].hb++
		node.table[node.id].t += X
		node.table[node.id].mux.Unlock()
	}	
}

//periodically gossip with two random neighbors and check for failures
func (node *Node) gossip(db *DB) {
	go node.updateHB()
	for{
		time.Sleep(Y * time.Second)
		//send table to neighbors
		neighbors := getRandNeighbors(node.id)
		db.nodes[neighbors[0]].tableInput <- node.table
		db.nodes[neighbors[1]].tableInput <- node.table
		//read tables sent from neighbors and update
		for len(node.tableInput) > 0 {
			neighborTable := <- node.tableInput
			node.table[node.id].mux.RLock()
			currTime := node.table[node.id].t 
			node.table[node.id].mux.RUnlock()
			for i := 0; i < NUM_NODES; i++ {
				neighborTable[i].mux.RLock()
				tNeighbor := neighborTable[i].t
				hbNeighbor := neighborTable[i].hb
				neighborTable[i].mux.RUnlock()
				node.table[i].mux.Lock()
				if tNeighbor >= node.table[i].t && hbNeighbor > node.table[i].hb && node.table[i].hb != -1 {
					node.table[i].t = currTime
					node.table[i].hb = hbNeighbor
				}
				node.table[i].mux.Unlock()
			}
		}
		//check for failures
		for i := 0; i < NUM_NODES; i++ {
			node.table[node.id].mux.RLock()
			currTime := node.table[node.id].t 
			node.table[node.id].mux.RUnlock()
			node.table[i].mux.Lock()
			if currTime - node.table[i].t > t_fail && node.table[i].hb != -1 && node.table[i].t != 0 {
				node.table[i].hb = -1	
			}
			node.table[i].mux.Unlock()
		}
	}
}

//pick 2 unique random neighbors
func getRandNeighbors(id int) [2]int {
	rand.Seed(time.Now().UnixNano())
	neighbors := [2]int{}
	rand1 := rand.Intn(NUM_NODES)
	for rand1 == id {
		rand1 = rand.Intn(NUM_NODES)
	}
	neighbors[0] = rand1
	rand2 := rand.Intn(NUM_NODES)
	for rand2 == rand1 || rand2 == id {
		rand2 = rand.Intn(NUM_NODES)
	}
	neighbors[1] = rand2
	return neighbors
}

//print table for node in format [id|hb|time]
func (node *Node) printTable() {
	fmt.Printf("node %v table: ", node.id)
	for i := 0; i < NUM_NODES; i++ {	
		node.table[i].mux.RLock()
		fmt.Printf("[%v|%2v|%2v]   ", node.table[i].id, node.table[i].hb, node.table[i].t)
		node.table[i].mux.RUnlock()
	}
	fmt.Printf("\n")
}

