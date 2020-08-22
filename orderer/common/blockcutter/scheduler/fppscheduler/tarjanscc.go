package fppscheduler

import (
	sort "sort"
)

// var logger *logging.Logger

// func init() {
// 	logger = flogging.MustGetLogger(pkgLogID)
// }

type ById []int32

func (v ById) Len() int           { return len(v) }
func (v ById) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v ById) Less(i, j int) bool { return v[i] < v[j] }

// function returning min of two int32 numbers
func min(x, y int32) int32 {
	if x < y {
		return x
	}
	return y
}

type SCC struct {
	Vertices []int32
	Member   []bool
}

type TarjanSCC interface {
	SCC() int
	GetSCCs() []SCC
	SCCUtil(u int32)
}

type tarjanscc struct {
	nscc        int
	time        int32      // time
	st          []int32    // stack
	graph       *[][]int32 // graph represented as adjacency list
	sccList     []SCC      // list of strongly connected components
	nvertices   int
	low         []int32
	disc        []int32
	stackMember []bool
}

func NewTarjanSCC(matrix *[][]int32) TarjanSCC {
	return &tarjanscc{
		nscc:        0,
		time:        0,
		graph:       matrix,
		nvertices:   len(*matrix),
		low:         make([]int32, len(*matrix)),
		disc:        make([]int32, len(*matrix)),
		stackMember: make([]bool, len(*matrix)),
		st:          make([]int32, 0, len(*matrix)),
		sccList:     make([]SCC, 0, 512),
	}
}

// driver method for finding all strongly connected components in a graph
func (tscc *tarjanscc) SCC() int {

	// Start by marking all vertices as not visited
	for i := 0; i < tscc.nvertices; i++ {
		tscc.low[i] = -1
		tscc.disc[i] = -1
	}

	for i := int32(0); i < int32(tscc.nvertices); i++ {
		if tscc.disc[i] == -1 {
			tscc.SCCUtil(i)
		}
	}

	tscc.low = nil
	tscc.disc = nil
	tscc.stackMember = nil
	tscc.st = nil

	return tscc.nscc
}

// recursive DFS traversal method to find the SCC
func (tscc *tarjanscc) SCCUtil(u int32) {
	tscc.low[u] = tscc.time
	tscc.disc[u] = tscc.time

	tscc.time += 1
	tscc.stackMember[u] = true

	tscc.st = append(tscc.st, u)

	// do a DFS traversal of neighbourhood
	for _, v := range (*(tscc.graph))[u] {

		// if v is nott already visited, recurse
		if tscc.disc[v] == -1 {
			tscc.SCCUtil(v)

			// check if subtree rooted at v hash a connection to one of the ancestors of u
			tscc.low[u] = min(tscc.low[u], tscc.low[v])
		} else if tscc.stackMember[v] == true {
			// update the tscc.low value of u only if v is already in stack
			tscc.low[u] = min(tscc.low[u], tscc.disc[v])
		}
	}

	member := make([]bool, tscc.nvertices)
	vertices := make([]int32, 0, tscc.nvertices)

	if tscc.low[u] == tscc.disc[u] {
		for w := int32(-1); w != u; {
			w, tscc.st = tscc.st[len(tscc.st)-1], tscc.st[:len(tscc.st)-1]
			vertices = append(vertices, w)
			member[w] = true
			tscc.stackMember[w] = false
		}
	}

	if len(vertices) > 0 {
		sort.Sort(ById(vertices))
		tscc.sccList = append(tscc.sccList, SCC{
			Vertices: vertices,
			Member:   member,
		})
		tscc.nscc += 1
	}
}

func (tscc *tarjanscc) GetSCCs() []SCC {
	return tscc.sccList
}
