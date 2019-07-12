/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sharpscheduler

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/willf/bloom"
)

func TestTxnSet(t *testing.T) {
	s := NewTxnSet()
	assert.False(t, s.Exists("a"))
	s.Add("a")
	assert.True(t, s.Exists("a"))
	assert.False(t, s.Exists("b"))

	s1 := NewTxnSet()
	s1.Add("a")
	s1.Add("b")
	s1.Add("c")
	s.InPlaceUnion(s1)

	assert.True(t, s.Exists("a"))
	assert.True(t, s.Exists("b"))
	assert.True(t, s.Exists("c"))
	assert.False(t, s.Exists("d"))
}

func TestRelayFilter(t *testing.T) {
	rf1 := CreateRelayedFilter(10)
	rf1.Add("A")
	assert.True(t, rf1.Exists("A"))
	assert.False(t, rf1.Exists("B"))

	rf2 := CreateRelayedFilter(5)
	rf2.Add("B")
	rf2.Add("C")
	rf1.Merge(rf2)
	assert.True(t, rf1.Exists("A"))
	assert.True(t, rf1.Exists("B"))
	assert.True(t, rf1.Exists("C"))
	assert.Equal(t, uint64(5), rf1.firstHeight)
	assert.Equal(t, uint64(5), rf1.secHeight)

	assert.False(t, rf1.Rotate(4, 10))
	assert.True(t, rf1.Rotate(10, 20))
	assert.Equal(t, uint64(5), rf1.firstHeight)
	assert.Equal(t, uint64(20), rf1.secHeight)
}

func TestGraph(t *testing.T) {
	graph := NewGraph()
	graph.AddNodes("2", "3", "5", "7", "8", "9", "10", "11")

	graph.AddEdge("7", "8")
	graph.AddEdge("7", "11")

	graph.AddEdge("5", "11")

	graph.AddEdge("3", "8")
	graph.AddEdge("3", "10")

	graph.AddEdge("11", "2")
	graph.AddEdge("11", "9")
	graph.AddEdge("11", "10")

	graph.AddEdge("8", "9")

	result, ok := graph.ToposortWithLimit(4)
	assert.True(t, ok, "cycle detected...")
	assert.Equal(t, []string{"3", "5", "7", "8"}, result)
	nodeCount := 0
	edgeCount := 0
	nodeCount, edgeCount = graph.Size()

	assert.Equal(t, 4, nodeCount)
	assert.Equal(t, 3, edgeCount)

	result, ok = graph.ToposortWithLimit(2)
	assert.True(t, ok, "cycle detected...")
	assert.Equal(t, []string{"11", "2"}, result)
	nodeCount, edgeCount = graph.Size()
	assert.Equal(t, 2, nodeCount)
	assert.Equal(t, 0, edgeCount)

	result, ok = graph.ToposortWithLimit(5)
	assert.True(t, ok, "cycle detected...")
	assert.Equal(t, []string{"10", "9"}, result)
	nodeCount, edgeCount = graph.Size()
	assert.Equal(t, 0, nodeCount)
	assert.Equal(t, 0, edgeCount)
}

func TestTxnPQueue(t *testing.T) {
	txnQ := NewTxnPQueue()
	assert.False(t, txnQ.Push("a", 4))
	assert.False(t, txnQ.Push("b", 2))
	assert.False(t, txnQ.Push("c", 1))
	assert.False(t, txnQ.Push("d", 3))
	assert.False(t, txnQ.Push("e", 1))

	// fmt.Printf("Txn PQueue: %v\n", txnQ.Raw())

	assert.True(t, txnQ.Exists("c"))
	txnID, blkHeight, exists := txnQ.Pop()
	assert.True(t, exists)
	assert.Equal(t, "c", txnID)
	assert.Equal(t, uint64(1), blkHeight)
	assert.False(t, txnQ.Exists(txnID))

	txnID, blkHeight, exists = txnQ.Peek()
	assert.True(t, exists)
	assert.Equal(t, "e", txnID)
	assert.Equal(t, uint64(1), blkHeight)

	assert.Equal(t, 4, txnQ.Size())

	assert.True(t, txnQ.Push("e", 3))
	txnID, blkHeight, exists = txnQ.Pop()
	assert.True(t, exists)
	assert.Equal(t, "b", txnID)
	assert.Equal(t, uint64(2), blkHeight)

	assert.True(t, txnQ.Push("a", 1))
	txnID, blkHeight, exists = txnQ.Pop()
	assert.True(t, exists)
	assert.Equal(t, "a", txnID)
	assert.Equal(t, uint64(1), blkHeight)

	txnID, blkHeight, exists = txnQ.Pop()
	assert.True(t, exists)
	assert.Equal(t, "e", txnID)
	assert.Equal(t, uint64(3), blkHeight)

	txnID, blkHeight, exists = txnQ.Pop()
	assert.True(t, exists)
	assert.Equal(t, "d", txnID)
	assert.Equal(t, uint64(3), blkHeight)

	txnID, blkHeight, exists = txnQ.Pop()
	assert.False(t, exists, "Should not pop due to the empty queue")
}

func TestBloomFilter(t *testing.T) {
	n := uint(1000)
	filter := bloom.New(20*n, 5) // load of 20, 5 keys
	filter.Add([]byte("Love"))
	assert.True(t, filter.Test([]byte("Love")))
	assert.False(t, filter.Test([]byte("Like")))
	assert.False(t, filter.Test([]byte("Hate")))
	// fmt.Printf("FP Rate: %f\n", filter.EstimateFalsePositiveRate(1000000))
	m, k := bloom.EstimateParameters(10000, 0.05)
	fmt.Printf("m = %d, k = %d\n", m, k) // m = 62353, k = 5

	filter1 := bloom.New(20*n, 5) // load of 20, 5 keys
	filter1.Add([]byte("Like"))
	assert.True(t, filter1.Test([]byte("Like")))
	assert.False(t, filter1.Test([]byte("Love")))
	assert.False(t, filter1.Test([]byte("Hate")))

	if err := filter.Merge(filter1); err != nil {
		panic("Err with msg " + err.Error())
	}
	assert.True(t, filter.Test([]byte("Like")))
	assert.True(t, filter.Test([]byte("Love")))
	assert.False(t, filter.Test([]byte("Hate")))

	filter2 := bloom.NewWithEstimates(10000, 0.05)
	fmt.Printf("FP Rate: %f\n", filter2.EstimateFalsePositiveRate(20000)) // 0.322330
}

func TestStack(t *testing.T) {
	s := NewStack()
	var ok bool
	var val string
	assert.Equal(t, 0, s.Size())
	_, ok = s.Peek()
	assert.False(t, ok)
	_, ok = s.Pop()
	assert.False(t, ok)

	s.Push("a")
	s.Push("b")
	assert.Equal(t, 2, s.Size())

	val, ok = s.Pop()
	assert.Equal(t, "b", val)
	assert.True(t, ok)
	assert.Equal(t, 1, s.Size())

	val, ok = s.Pop()
	assert.Equal(t, "a", val)
	assert.True(t, ok)
	assert.Equal(t, 0, s.Size())

	_, ok = s.Pop()
	assert.False(t, ok)
}

func OutEdges(from string) (outs []string) {
	outs = []string{}
	if from == "A" {
		outs = []string{"D"}
	} else if from == "B" {
		outs = []string{"D"}
	} else if from == "C" {
		outs = []string{"F"}
	} else if from == "D" {
		outs = []string{"E", "F"}
	} else if from == "E" {
		outs = []string{"G", "H"}
	} else if from == "F" {
		outs = []string{"I"}
		// Create edges
		// outs = []string{"I", "B"}
	} else if from == "H" {
		outs = []string{"I"}
	}
	return outs
}

func TestTopoOrder(t *testing.T) {
	order := TopoOrder([]string{"A", "B", "D"}, OutEdges)
	fmt.Printf("Topo Order: %v\n", order)
}
