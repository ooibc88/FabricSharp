package gorocksdb 

// #include <stdlib.h>
import "C"
import "unsafe"

// Slice is used as a wrapper for non-copy values
type Slice struct {
	data  *C.char
	size  C.size_t
	freed bool
  sdata []byte
}

// NewSlice returns a slice with the given data.
func NewSlice(data *C.char, size C.size_t) *Slice {
	return &Slice{data, size, false, nil}
}

func NewUStoreSlice(data string) *Slice {
  return &Slice{nil, 0, false, []byte(data)}
}

// Data returns the data of the slice.
func (s *Slice) Data() []byte {
  if s.sdata == nil {
	  return charToByte(s.data, s.size)
  } else {
    if len(s.sdata)==0 {
      return nil
    }
    return s.sdata
  }
}

// Size returns the size of the data.
func (s *Slice) Size() int {
  if s.sdata == nil { 
	  return int(s.size)
  } else {
    return len(s.sdata)
  }
}

// Free frees the slice data.
func (s *Slice) Free() {
	if !s.freed && s.sdata == nil {
		C.free(unsafe.Pointer(s.data))
		s.freed = true
	}
}
