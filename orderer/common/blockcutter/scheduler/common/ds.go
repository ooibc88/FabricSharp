package common

import (
	"fmt"
	"sort"

	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/willf/bloom"
)

type TxnSet map[string]bool

func NewTxnSet() TxnSet {
	return make(map[string]bool, 0)
}

func NewTxnSetWith(elements ...string) TxnSet {
	s := make(map[string]bool, 0)
	for _, element := range elements {
		s[element] = true
	}
	return s
}

func (s TxnSet) Size() int {
	return len(s)
}

func (s TxnSet) InPlaceUnion(arg TxnSet) {
	for t := range arg {
		s[t] = true
	}
}

func (s TxnSet) Exists(element string) bool {
	if _, ok := s[element]; ok {
		return true
	} else {
		return false
	}
}

func (s TxnSet) Add(elements ...string) {
	for _, e := range elements {
		s[e] = true
	}
}

func (s TxnSet) String() string {
	return fmt.Sprintf("[%v]", s.ToSlice())
}

func (s TxnSet) ToSlice() []string {
	l := []string{}
	for e := range s {
		l = append(l, e)
	}
	return l
}

// a txn priority queue, weighted by the committed block height of the last anti-dependent txn.
type TxnPQueue struct {
	list        *arraylist.List
	blkHeights  map[string]uint64 // the commited block height
	heapIndices map[string]int    // txn index in the list
}

func NewTxnPQueue() *TxnPQueue {
	return &TxnPQueue{list: arraylist.New(),
		blkHeights:  make(map[string]uint64),
		heapIndices: make(map[string]int)}
}

func (queue *TxnPQueue) Exists(txnID string) (exists bool) {
	// fmt.Printf("Heap Indices: %v\n", queue.heapIndices)
	_, exists = queue.heapIndices[txnID]
	return
}

func (queue *TxnPQueue) SwapElement(txnID1, txnID2 string) {
	id1 := queue.heapIndices[txnID1]
	id2 := queue.heapIndices[txnID2]
	queue.list.Swap(id1, id2)
	queue.heapIndices[txnID1] = id2
	queue.heapIndices[txnID2] = id1
}

func (queue *TxnPQueue) Pop() (txnID string, blkHeight uint64, ok bool) {
	value, ok := queue.list.Get(0)
	if !ok {
		return
	}
	txnID = value.(string)
	blkHeight = queue.blkHeights[txnID]

	lastIndex := queue.list.Size() - 1
	value, _ = queue.list.Get(lastIndex)
	queue.SwapElement(txnID, value.(string))
	queue.list.Remove(lastIndex)
	delete(queue.blkHeights, txnID)
	delete(queue.heapIndices, txnID)
	queue.bubbleDownIndex(0)
	return
}

func (queue *TxnPQueue) bubbleDownIndex(index int) {
	size := queue.list.Size()
	for leftIndex := index<<1 + 1; leftIndex < size; leftIndex = index<<1 + 1 {
		rightIndex := index<<1 + 2
		smallerIndex := leftIndex
		leftValue, _ := queue.list.Get(leftIndex)
		rightValue, _ := queue.list.Get(rightIndex)
		if rightIndex < size && queue.blkHeights[rightValue.(string)] < queue.blkHeights[leftValue.(string)] {
			smallerIndex = rightIndex
		}
		indexValue, _ := queue.list.Get(index)
		smallerValue, _ := queue.list.Get(smallerIndex)

		indexTxnID := indexValue.(string)
		smallerTxnID := smallerValue.(string)

		if queue.blkHeights[smallerTxnID] < queue.blkHeights[indexTxnID] {
			queue.SwapElement(smallerTxnID, indexTxnID)
		} else {
			break
		}
		index = smallerIndex
	}
}

func (queue *TxnPQueue) bubbleUpIndex(index int) {
	for parentIndex := (index - 1) >> 1; index > 0; parentIndex = (index - 1) >> 1 {
		indexValue, _ := queue.list.Get(index)
		parentValue, _ := queue.list.Get(parentIndex)
		indexTxnID := indexValue.(string)
		parentTxnID := parentValue.(string)
		if queue.blkHeights[parentTxnID] <= queue.blkHeights[indexTxnID] {
			break
		}
		queue.SwapElement(indexTxnID, parentTxnID)
		index = parentIndex
	}
}

func (queue *TxnPQueue) Push(txnID string, newHeight uint64) (exists bool) {
	var curHeight uint64
	if curHeight, exists = queue.blkHeights[txnID]; !exists {
		lastNewIndex := queue.list.Size()
		queue.heapIndices[txnID] = lastNewIndex
		queue.blkHeights[txnID] = newHeight
		queue.list.Add(txnID)
		queue.bubbleUpIndex(lastNewIndex)
	}
	curIndex := queue.heapIndices[txnID]
	queue.blkHeights[txnID] = newHeight
	if curHeight < newHeight {
		queue.bubbleDownIndex(curIndex)
	} else if curHeight == newHeight {
		// do nothing
	} else {
		queue.bubbleUpIndex(curIndex)
	}
	return
}

func (queue *TxnPQueue) Size() int {
	return queue.list.Size()
}

func (queue *TxnPQueue) Peek() (txnID string, blkHeight uint64, exists bool) {
	value, exists := queue.list.Get(0)
	if exists {
		txnID = value.(string)
		blkHeight = queue.blkHeights[txnID]
	}
	return
}

func (queue *TxnPQueue) Raw() (result map[string]uint64) {
	result = make(map[string]uint64)
	it := queue.list.Iterator()
	for it.Next() {
		txn := it.Value().(string)
		result[txn] = queue.blkHeights[txn]
	}
	return
}

type Graph struct {
	outputs map[string]map[string]int
	inputs  map[string]int
}

func NewGraph() *Graph {
	return &Graph{
		inputs:  make(map[string]int),
		outputs: make(map[string]map[string]int),
	}
}

func (g *Graph) Nodes() (nodes []string) {
	for node := range g.inputs {
		nodes = append(nodes, node)
	}
	return
}

func (g *Graph) AddNode(name string) bool {
	if _, ok := g.outputs[name]; ok {
		return false
	}
	g.outputs[name] = make(map[string]int)
	g.inputs[name] = 0
	return true
}

func (g *Graph) AddNodes(names ...string) bool {
	for _, name := range names {
		if ok := g.AddNode(name); !ok {
			return false
		}
	}
	return true
}

func (g *Graph) Size() (nodeCount int, edgeCount int) {
	nodeCount = len(g.outputs)
	edgeCount = 0
	for _, v := range g.inputs {
		edgeCount += v
	}
	return
}

func (g *Graph) AddEdge(from, to string) bool {
	// logger.Infof("Add edges from %s to %s", from, to)
	m, ok := g.outputs[from]
	if !ok {
		// m has not been added
		panic("Node " + from + " has not been added...")
	}
	if _, okk := g.inputs[to]; !okk {
		panic("Node " + to + " has no been added...")
	}
	if _, exists := m[to]; exists {
		// edge from -> to has been added before.
		return false
	}
	m[to] = len(m) + 1
	g.inputs[to]++

	return true
}

func (g *Graph) unsafeRemoveEdge(from, to string) {
	delete(g.outputs[from], to)
	g.inputs[to]--
}

func (g *Graph) RemoveEdge(from, to string) bool {
	if _, ok := g.outputs[from]; !ok {
		return false
	}
	g.unsafeRemoveEdge(from, to)
	return true
}

func (g *Graph) Toposort() ([]string, bool) {
	nodeCount, _ := g.Size()
	return g.ToposortWithLimit(nodeCount)
}

func (g *Graph) ToposortWithLimit(sortCount int) ([]string, bool) {
	L := make([]string, 0, sortCount)
	S := make([]string, 0, sortCount)

	for n := range g.outputs {
		if g.inputs[n] == 0 {
			S = append(S, n)
		}
	}
	//Sort the slice to make the iterated order and result determinisitic
	sort.Slice(S, func(i, j int) bool {
		return S[i] < S[j]
	})

	// set to true if can not remove nodes even though the number of removed node < sortCount
	drained := true
	for len(S) > 0 {
		var n string
		n, S = S[0], S[1:]
		L = append(L, n)

		ms := make([]string, len(g.outputs[n]))
		for m, i := range g.outputs[n] {
			ms[i-1] = m
		}

		for _, m := range ms {
			g.unsafeRemoveEdge(n, m)

			if g.inputs[m] == 0 {
				S = append(S, m)
			}
		}
		delete(g.outputs, n)
		delete(g.inputs, n)
		if len(L) == sortCount {
			drained = false
			break
		}
	}
	if drained {
		N := 0
		for _, v := range g.inputs {
			N += v
		}

		if N > 0 {
			return L, false
		}
	}

	return L, true
}

type Stack struct {
	internal []string
}

func NewStack() *Stack {
	internal := []string{}
	return &Stack{internal}
}

func (s *Stack) Size() int {
	return len(s.internal)
}

func (s *Stack) Peek() (string, bool) {
	if s.Size() == 0 {
		return "", false
	} else {
		return s.internal[s.Size()-1], true
	}
}

func (s *Stack) Push(v string) {
	s.internal = append(s.internal, v)
}

func (s *Stack) Pop() (string, bool) {
	if val, ok := s.Peek(); ok {
		s.internal = s.internal[:s.Size()-1]
		return val, true
	} else {
		return "", false
	}
}

func TopoOrder(startTxns []string, outEdges func(string) []string) (order []string) {
	visited := map[string]bool{}
	visiting := map[string]bool{}
	topoReverseOrdered := []string{}
	stack := NewStack()

	for _, txn := range startTxns {
		stack.Push(txn)
		visiting[txn] = true
	}
	// txnCounter := 0

	// oldHeads := map[string]int{}
	for stack.Size() > 0 {
		head, ok := stack.Peek()
		if !ok {
			panic("Stack shall not be empty...")
		}
		if visited[head] {
			stack.Pop()
			continue
		}

		existsUnvisitedOutTxn := false
		for _, outTxn := range outEdges(head) {
			if visiting[outTxn] {
				panic("Cycle detected during topo-ordered traversal...")
			}
			if !visited[outTxn] {
				stack.Push(outTxn)
				existsUnvisitedOutTxn = true
			} // !visited
		} // end for outTxn

		if existsUnvisitedOutTxn {
			visiting[head] = true
		} else {
			if _, ok := stack.Pop(); !ok {
				panic("Stack shall not be empty...")
			}
			visited[head] = true
			delete(visiting, head)
			topoReverseOrdered = append(topoReverseOrdered, head)
		}
	} // end for

	for i := len(topoReverseOrdered) - 1; i >= 0; i-- {
		order = append(order, topoReverseOrdered[i])
	}
	return
}

type RelayedFilter struct {
	firstHeight uint64
	secHeight   uint64
	firstFilter *bloom.BloomFilter
	secFilter   *bloom.BloomFilter
}

func CreateRelayedFilter(startHeight uint64) *RelayedFilter {
	return &RelayedFilter{firstHeight: startHeight, secHeight: startHeight,
		firstFilter: bloom.NewWithEstimates(50000, 0.01),
		secFilter:   bloom.NewWithEstimates(50000, 0.01)}
}

func (filter *RelayedFilter) Add(element string) {
	filter.firstFilter.Add([]byte(element))
	filter.secFilter.Add([]byte(element))
}

func (filter *RelayedFilter) Merge(f *RelayedFilter) {
	if f.firstHeight < filter.firstHeight {
		filter.firstHeight = f.firstHeight
	}
	if f.secHeight < filter.secHeight {
		filter.secHeight = f.secHeight
	}
	filter.firstFilter.Merge(f.firstFilter)
	filter.secFilter.Merge(f.secFilter)
}

func (filter *RelayedFilter) Exists(element string) bool {
	return filter.firstFilter.Test([]byte(element))
}

func (filter *RelayedFilter) Rotate(earliestBlkHeight, curBlkHeight uint64) bool {
	if filter.firstHeight > filter.secHeight {
		panic(fmt.Sprintf("First filter (height: %d) should incorporate earlier blocks than the second (height: %d). ", filter.firstHeight, filter.secHeight))
	}

	if earliestBlkHeight > curBlkHeight {
		panic(fmt.Sprintf("earliestBlkHeight (%d) should be smaller than the curBlkHeight (%d).", earliestBlkHeight, curBlkHeight))
	}

	if filter.secHeight < earliestBlkHeight {
		filter.firstFilter = filter.secFilter
		filter.firstHeight = filter.secHeight
		filter.secHeight = curBlkHeight
		filter.secFilter = bloom.NewWithEstimates(50000, 0.01)
		return true
	}

	return false

}
