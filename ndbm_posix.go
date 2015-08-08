// +build darwin

package ndbm

// #include <ndbm.h>
import "C"

import (
	"unsafe"
)

// bytesToDatum for systems that use the same datum struct as the POSIX standard,
// with void* dptr and size_t dsize.
// So far, this includes Mac OS X, but not FreeBSD or GDBM.
func bytesToDatum(buf []byte) C.datum {
	return C.datum{
		dptr:  unsafe.Pointer(&buf[0]),
		dsize: C.size_t(len(buf)),
	}
}
