package dbm

// #include <errno.h>
// #include <ndbm.h>
// #include <stdlib.h>
// #include <string.h>
import "C"

import (
	"bytes"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

type DBM struct {
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

const (
	dbm_ALREADY_EXISTS = 1
	dbm_NOT_FOUND      = 1
)

func Open(path string) (*DBM, error) {
	cPath := C.CString(path)
	cDbm, err := C.dbm_open(
		cPath,
		C.int(os.O_RDWR|os.O_CREATE),
		syscall.S_IREAD|syscall.S_IWRITE|syscall.S_IRGRP|syscall.S_IWGRP)
	C.free(unsafe.Pointer(cPath))
	if err != nil {
		return nil, err
	}
	dbm := &DBM{
		cDbm: cDbm,
	}
	return dbm, nil
}

func (dbm *DBM) Close() {
	C.dbm_close(dbm.cDbm)
}

func bytesToDatum(buf []byte) C.datum {
	return C.datum{
		dptr:  unsafe.Pointer(&buf[0]),
		dsize: C.size_t(len(buf)),
	}
}

func datumToBytes(datum C.datum) []byte {
	if datum.dptr == nil {
		return nil
	}
	return C.GoBytes(datum.dptr, C.int(datum.dsize))
}

func (dbm *DBM) store(key, value []byte, mode C.int) (C.int, error) {
	status, err := C.dbm_store(dbm.cDbm, bytesToDatum(key), bytesToDatum(value), mode)
	if err != nil {
		return status, err
	}
	return status, nil
}

func (dbm *DBM) Insert(key, value []byte) error {
	status, err := dbm.store(key, value, C.DBM_INSERT)
	if err != nil {
		return err
	}
	if status == dbm_ALREADY_EXISTS {
		return KeyAlreadyExists{key}
	}
	return nil
}

func (dbm *DBM) Replace(key, value []byte) error {
	_, err := dbm.store(key, value, C.DBM_REPLACE)
	return err
}

func (dbm *DBM) Fetch(key []byte) ([]byte, error) {
	datum, err := C.dbm_fetch(dbm.cDbm, bytesToDatum(key))
	if err != nil {
		return nil, err
	}
	return datumToBytes(datum), nil
}

func (dbm *DBM) Delete(key []byte) error {
	status, err := C.dbm_delete(dbm.cDbm, bytesToDatum(key))
	if err != nil {
		return err
	}
	if status == dbm_NOT_FOUND {
		return KeyNotFound{key}
	}
	return nil
}

func (dbm *DBM) KeysCallback(callback func([]byte) error) error {
	for datum, err := C.dbm_firstkey(dbm.cDbm); datum.dptr != nil; datum, err = C.dbm_nextkey(dbm.cDbm) {
		if err != nil {
			return err
		}
		key := datumToBytes(datum)
		err = callback(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dbm *DBM) Keys() [][]byte {
	keys := [][]byte{}
	_ = dbm.KeysCallback(func(key []byte) error {
		keys = append(keys, key)
		return nil
	})
	return keys
}

func (dbm *DBM) Len() int {
	count := 0
	_ = dbm.KeysCallback(func(_ []byte) error {
		count++
		return nil
	})
	return count
}

func (dbm *DBM) ValuesCallback(callback func([]byte) error) error {
	return dbm.KeysCallback(func(key []byte) error {
		value, err := dbm.Fetch(key)
		if err != nil {
			return nil
		}
		return callback(value)
	})
}

func (dbm *DBM) Values() [][]byte {
	values := [][]byte{}
	_ = dbm.ValuesCallback(func(value []byte) error {
		values = append(values, value)
		return nil
	})
	return values
}

func (dbm *DBM) ItemsCallback(callback func(key, value []byte) error) error {
	return dbm.KeysCallback(func(key []byte) error {
		value, err := dbm.Fetch(key)
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

func (dbm *DBM) Items() Items {
	items := Items{}
	_ = dbm.ItemsCallback(func(key, value []byte) error {
		items = append(items, Item{
			Key:   key,
			Value: value,
		})
		return nil
	})
	return items
}

func (dbm *DBM) Update(items Items) error {
	for _, item := range items {
		err := dbm.Replace(item.Key, item.Value)
		if err != nil {
			return err
		}
	}
	return nil
}
