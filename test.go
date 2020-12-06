package main
//to run this:
//go build test.go node.go gossip.go
//./test
import (
	"fmt"
)

const NUM_NODES = 5

func bruteForce(vals []Data) []Data{
	overwrite := map[int]bool{}
	returnVals := []Data{}
	for i, val1 := range vals {
		VAL2: for j, val2 := range vals {
			if i != j && !overwrite[j]{
				for _, entry1 := range val1.context.clock {
					hasEntry := false;
					for _, entry2 := range val2.context.clock {
						if entry2.nodeID == entry1.nodeID {
							if entry2.counter < entry1.counter {
								continue VAL2
							} else {
								hasEntry = true;
								break;
							}
						}
					}
					if !hasEntry {
						continue VAL2
					}
				}
				overwrite[i] = true; 
				break VAL2
			}
		}
	}
	for i, val := range vals {
		if !overwrite[i] {
			returnVals = append(returnVals, val)
		}
	}
	return returnVals
}

func main(){
	vals := []Data{}
	c1 := []*ClockEntry{}
	c2 := []*ClockEntry{}
	c3 := []*ClockEntry{}
	c4 := []*ClockEntry{}
	// c5 := []*ClockEntry{}

	c1 = append(c1, &ClockEntry{nodeID:0, counter:1})
	c1 = append(c1, &ClockEntry{nodeID:1, counter:1})
	c2 = append(c2, &ClockEntry{nodeID:0, counter:1})
	c2 = append(c2, &ClockEntry{nodeID:1, counter:2})
	c3 = append(c3, &ClockEntry{nodeID:1, counter:2})
	c4 = append(c4, &ClockEntry{nodeID:2, counter:1})
	// c5 = append(c5, &ClockEntry{nodeID:0, counter:1})
	// c5 = append(c5, &ClockEntry{nodeID:1, counter:1})

	vals = append(vals, Data{data:1, context:&Context{version: 1, clock:c1}})
	vals = append(vals, Data{data:2, context:&Context{version: 2, clock:c2}})
	vals = append(vals, Data{data:3, context:&Context{version: 3, clock:c3}})
	vals = append(vals, Data{data:4, context:&Context{version: 4, clock:c4}})
	// vals = append(vals, Data{data:5, context:&Context{version: 5, clock:c5}})

	//version 1: (0,1),(1,1)
	//version 2: (0,1),(1,2)
	//version 3: (1,2)
	//version 4: (2,1)
	//should return version 2 and version 4

	//adding version 5: (0,1),(1,1) gives runtime error for consolidateDataVals

	vals1 := consolidateDataVals(vals)
	vals2 := bruteForce(vals)
	fmt.Println("consolidated data:")
	for _, val := range vals1 {
		fmt.Printf("data:%d, version: %d\n", val.data, val.context.version)
	}
	fmt.Println("bruteForce version:")
	for _, val := range vals2 {
		fmt.Printf("data:%d, version: %d\n", val.data, val.context.version)
	}
}