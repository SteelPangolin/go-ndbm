package ndbm

// #include <stdlib.h>
// #include <string.h>
//
// #ifdef __linux
//     #include "gdbm_compat.h"
// #else
//     #define DBM_ITEM_NOT_FOUND 0
//     #include <ndbm.h>
// #endif
//
// #cgo linux LDFLAGS: -lgdbm -lgdbm_compat
import "C"

import (
	"bytes"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

type NDBM struct {
	cDbm *C.DBM
}

type KeyAlreadyExists struct {
	Key []byte
}

func (err KeyAlreadyExists) Error() string {
	return fmt.Sprintf("Key already exists: %s", err.Key)
}

type KeyNotFound struct {
	Key []byte
}

func (err KeyNotFound) Error() string {
	return fmt.Sprintf("Key not found: %s", err.Key)
}

type NDBMError struct {
	dbErrNum int
	errnoErr error
}

func (err NDBMError) Error() string {
	return fmt.Sprintf("NDBM error #%d (system error %v)", err.dbErrNum, err.errnoErr)
}

const (
	ndbm_CHECK_ERROR    = -1
	ndbm_NO_ERROR       = 0
	ndbm_ALREADY_EXISTS = 1
	ndbm_NOT_FOUND      = 1
)

func OpenWithDefaults(path string) (*NDBM, error) {
	ndbm, err := Open(
		path,
		os.O_RDWR|os.O_CREATE,
		syscall.S_IREAD|syscall.S_IWRITE|syscall.S_IRGRP|syscall.S_IWGRP)
	return ndbm, err
}

func Open(path string, flags, mode int) (*NDBM, error) {
	cPath := C.CString(path)
	cDbm, err := C.dbm_open(cPath, C.int(flags), C.mode_t(mode))
	C.free(unsafe.Pointer(cPath))
	if cDbm == nil {
		return nil, err
	}
	ndbm := &NDBM{
		cDbm: cDbm,
	}
	return ndbm, nil
}

func (ndbm *NDBM) Close() {
	C.dbm_close(ndbm.cDbm)
}

// FD returns a file descriptor that can be used to lock the database.
func (ndbm *NDBM) FD() int {
	return int(C.dbm_dirfno(ndbm.cDbm))
}

func bytesToDatum(buf []byte) C.datum {
	return C.datum{
		dptr:  (*C.char)(unsafe.Pointer(&buf[0])),
		dsize: C.int(len(buf)),
	}
}

func datumToBytes(datum C.datum) []byte {
	if datum.dptr == nil {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(datum.dptr), C.int(datum.dsize))
}

func (ndbm *NDBM) store(key, value []byte, mode C.int) (C.int, error) {
	C.dbm_clearerr(ndbm.cDbm)
	status, err := C.dbm_store(ndbm.cDbm, bytesToDatum(key), bytesToDatum(value), mode)
	if status == ndbm_CHECK_ERROR {
		return status, NDBMError{
			dbErrNum: int(C.dbm_error(ndbm.cDbm)),
			errnoErr: err,
		}
	}
	return status, nil
}

func (ndbm *NDBM) Insert(key, value []byte) error {
	status, err := ndbm.store(key, value, C.DBM_INSERT)
	if err != nil {
		return err
	}
	if status == ndbm_ALREADY_EXISTS {
		return KeyAlreadyExists{key}
	}
	return nil
}

func (ndbm *NDBM) Replace(key, value []byte) error {
	_, err := ndbm.store(key, value, C.DBM_REPLACE)
	return err
}

func (ndbm *NDBM) Fetch(key []byte) ([]byte, error) {
	C.dbm_clearerr(ndbm.cDbm)
	datum, err := C.dbm_fetch(ndbm.cDbm, bytesToDatum(key))
	value := datumToBytes(datum)
	if value == nil {
		dbErrNum := C.dbm_error(ndbm.cDbm)
		if dbErrNum == ndbm_NO_ERROR {
			return nil, KeyNotFound{key}
		}
		if dbErrNum == C.DBM_ITEM_NOT_FOUND {
			return nil, KeyNotFound{key}
		}
		return nil, NDBMError{
			dbErrNum: int(dbErrNum),
			errnoErr: err,
		}
	}
	return value, nil
}

func (ndbm *NDBM) Delete(key []byte) error {
	C.dbm_clearerr(ndbm.cDbm)
	status, err := C.dbm_delete(ndbm.cDbm, bytesToDatum(key))
	if status == ndbm_CHECK_ERROR {
		dbErrNum := C.dbm_error(ndbm.cDbm)
		if dbErrNum == C.DBM_ITEM_NOT_FOUND {
			return KeyNotFound{key}
		}
		return NDBMError{
			dbErrNum: int(dbErrNum),
			errnoErr: err,
		}
	}
	if status == ndbm_NOT_FOUND {
		return KeyNotFound{key}
	}
	return nil
}

var noMoreKeys error = fmt.Errorf("No more keys")

func (ndbm *NDBM) firstKey() ([]byte, error) {
	datum, err := C.dbm_firstkey(ndbm.cDbm)
	key := datumToBytes(datum)
	if key == nil {
		dbErrNum := C.dbm_error(ndbm.cDbm)
		if dbErrNum == ndbm_NO_ERROR {
			return nil, noMoreKeys
		}
		return nil, NDBMError{
			dbErrNum: int(dbErrNum),
			errnoErr: err,
		}
	}
	return key, nil
}

func (ndbm *NDBM) nextKey() ([]byte, error) {
	datum, err := C.dbm_nextkey(ndbm.cDbm)
	key := datumToBytes(datum)
	if key == nil {
		dbErrNum := C.dbm_error(ndbm.cDbm)
		if dbErrNum == ndbm_NO_ERROR {
			return nil, noMoreKeys
		}
		return nil, NDBMError{
			dbErrNum: int(dbErrNum),
			errnoErr: err,
		}
	}
	return key, nil
}

func (ndbm *NDBM) KeysCallback(callback func([]byte) error) error {
	for key, err := ndbm.firstKey(); err == nil; key, err = ndbm.nextKey() {
		if err != nil {
			if err == noMoreKeys {
				return nil
			}
			return err
		}
		err = callback(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ndbm *NDBM) Keys() [][]byte {
	keys := [][]byte{}
	_ = ndbm.KeysCallback(func(key []byte) error {
		keys = append(keys, key)
		return nil
	})
	return keys
}

func (ndbm *NDBM) Len() int {
	count := 0
	_ = ndbm.KeysCallback(func(_ []byte) error {
		count++
		return nil
	})
	return count
}

func (ndbm *NDBM) ValuesCallback(callback func([]byte) error) error {
	return ndbm.KeysCallback(func(key []byte) error {
		value, err := ndbm.Fetch(key)
		if err != nil {
			return nil
		}
		return callback(value)
	})
}

func (ndbm *NDBM) Values() [][]byte {
	values := [][]byte{}
	_ = ndbm.ValuesCallback(func(value []byte) error {
		values = append(values, value)
		return nil
	})
	return values
}

func (ndbm *NDBM) ItemsCallback(callback func(key, value []byte) error) error {
	return ndbm.KeysCallback(func(key []byte) error {
		value, err := ndbm.Fetch(key)
		if err != nil {
			return err
		}
		return callback(key, value)
	})
}

type Item struct {
	Key   []byte
	Value []byte
}

type Items []Item

func (items Items) Len() int {
	return len(items)
}

func (items Items) Swap(i, j int) {
	items[i], items[j] = items[j], items[i]
}

func (items Items) Less(i, j int) bool {
	return bytes.Compare(items[i].Key, items[j].Key) == -1
}

func (ndbm *NDBM) Items() Items {
	items := Items{}
	_ = ndbm.ItemsCallback(func(key, value []byte) error {
		items = append(items, Item{
			Key:   key,
			Value: value,
		})
		return nil
	})
	return items
}

func (ndbm *NDBM) Update(items Items) error {
	for _, item := range items {
		err := ndbm.Replace(item.Key, item.Value)
		if err != nil {
			return err
		}
	}
	return nil
}
