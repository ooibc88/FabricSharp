// Package jemalloc uses the cgo compilation facilities to build the
// jemalloc library.
package jemalloc

// #cgo CPPFLAGS: -Iinternal/include -std=gnu11
// #cgo !musl CPPFLAGS: -DJEMALLOC_PROF -DJEMALLOC_PROF_LIBGCC
// #cgo CPPFLAGS: -D_REENTRANT
// #cgo !darwin LDFLAGS: -lm -lpthread
// #cgo linux LDFLAGS: -lrt
// #cgo linux CPPFLAGS: -D_GNU_SOURCE
// #cgo darwin CPPFLAGS: -Idarwin_includes/internal/include -Idarwin_includes/internal/include/jemalloc/internal -fno-omit-frame-pointer
// #cgo linux CPPFLAGS: -Ilinux_includes/internal/include -Ilinux_includes/internal/include/jemalloc/internal
// #cgo freebsd CPPFLAGS: -Ifreebsd_includes/internal/include -Ifreebsd_includes/internal/include/jemalloc/internal
import "C"
