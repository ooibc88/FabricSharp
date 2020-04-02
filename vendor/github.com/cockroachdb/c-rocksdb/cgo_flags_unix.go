// +build !windows

package rocksdb

import (
	// This is explicit because these Go libraries do not export any Go symbols.
	_ "github.com/cockroachdb/c-snappy"
)

// #cgo CPPFLAGS: -DSNAPPY
// #cgo CPPFLAGS: -I../c-snappy/internal
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
import "C"
