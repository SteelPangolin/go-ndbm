package ndbm

// #include "gdbm_compat.h"
import "C"

import (
	"unsafe"
)

// bytesToDatum for GDBM 1.8.
func bytesToDatum(buf []byte) C.datum {
	return C.datum{
		dptr:  (*C.char)(unsafe.Pointer(&buf[0])),
		dsize: C.int(len(buf)),
	}
}
