// +build linux freebsd

package ndbm

// #ifdef __linux
//     #include "gdbm_compat.h"
// #else
//     #include <ndbm.h>
// #endif
import "C"

import (
	"unsafe"
)

// bytesToDatum for FreeBSD and GDBM datum structs with char* dptr and int dsize.
func bytesToDatum(buf []byte) C.datum {
	return C.datum{
		dptr:  (*C.char)(unsafe.Pointer(&buf[0])),
		dsize: C.int(len(buf)),
	}
}
