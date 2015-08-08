package ndbm

// State of the art structured storage for 1986.
//
// ndbm is a Go wrapper around the POSIX NDBM database interface.
// NDBM is built into Mac OS X and FreeBSD,
// and is available as a compatibility mode of the GDBM library on Linux.
//
// Why? I wrote the first version of this library on a Mac on a cruise ship,
// where I needed a persistent key-value store, but had no Internet connection,
// just OS X system libraries, man pages, and Go documentation.
//
// To make this package go gettable, I've targeted the quirky GDBM 1.8,
// which is the version available on Ubuntu 12-15 and Debian 7-8,
// instead of attempting build-time library detection,
// which would allow using the current and more NDBM-compatible GDBM 1.11.
//
// Debian/Ubuntu users should install libgdbm and libgdbm-dev before building.
//
// If you are using a recent GDBM that provides ndbm.h, you will probably need
// to delete gdbm_compat.h and gdbm_compat_linux.c and the #ifdef __linux checks.
//
// POSIX standard for NDBM:
// http://pubs.opengroup.org/onlinepubs/9699919799/functions/dbm_open.html
// http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/ndbm.h.html
//
// Mac OS X NDBM:
// https://developer.apple.com/library/mac/documentation/Darwin/Reference/ManPages/man3/dbm_open.3.html
// (Note that this is a BSD man page, and the datum struct described is wrong for OS X.)
// http://www.opensource.apple.com/source/Libc/Libc-997.90.3/include/ndbm.h
//
// FreeBSD NDBM:
// https://www.freebsd.org/cgi/man.cgi?query=dbm&apropos=0&sektion=3&manpath=FreeBSD+10.0-RELEASE&format=html
// https://github.com/freebsd/freebsd/blob/master/lib/libc/db/hash/ndbm.c
// https://github.com/freebsd/freebsd/blob/master/include/ndbm.h
//
// GDBM:
// http://www.gnu.org.ua/software/gdbm/manual/html_node/ndbm.html

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
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// NDBM or compatible database.
// NDBM is not required by POSIX to be threadsafe, so this library isn't either.
type NDBM struct {
	cDbm *C.DBM
}

// KeyAlreadyExists is returned when trying to insert a key that already exists.
type KeyAlreadyExists struct {
	Key []byte
}

func (err KeyAlreadyExists) Error() string {
	return fmt.Sprintf("Key already exists: %s", err.Key)
}

// KeyNotFound is returned when trying to fetch or delete a key that doesn't exist.
type KeyNotFound struct {
	Key []byte
}

func (err KeyNotFound) Error() string {
	return fmt.Sprintf("Key not found: %s", err.Key)
}

// Error is returned on unexpected NDBM or libc errors.
type Error struct {
	dbErrNum int
	errnoErr error
}

func (err Error) Error() string {
	return fmt.Sprintf("NDBM error #%d (system error %v)", err.dbErrNum, err.errnoErr)
}

const (
	checkError    = -1
	noError       = 0
	alreadyExists = 1
	notFound      = 1
)

// OpenWithDefaults opens an NDBM database in read-write mode
// and will create it if it doesn't exist with permissions ug=rw,o=.
func OpenWithDefaults(path string) (*NDBM, error) {
	ndbm, err := Open(
		path,
		os.O_RDWR|os.O_CREATE,
		syscall.S_IREAD|syscall.S_IWRITE|syscall.S_IRGRP|syscall.S_IWGRP)
	return ndbm, err
}

// Open lets you specify how the database is opened, for example, if you want read-only mode.
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

// Close closes the NDBM database.
func (ndbm *NDBM) Close() {
	C.dbm_close(ndbm.cDbm)
}

// fd returns a file descriptor that can be used to lock the database.
// Not part of the POSIX standard but implemented in FreeBSD, Mac OS X, and GDBM.
func (ndbm *NDBM) fd() int {
	return int(C.dbm_dirfno(ndbm.cDbm))
}

// ErrAlreadyLocked is returned if another process already has a lock.
var ErrAlreadyLocked = fmt.Errorf("Database is already locked")

func (ndbm *NDBM) lock(lockMode int) error {
	err := syscall.Flock(ndbm.fd(), lockMode)
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			if errno == syscall.EWOULDBLOCK {
				return ErrAlreadyLocked
			}
		}
		return err
	}
	return nil
}

// LockShared locks the database for multiple processes.
// For example, multiple readers can share a read-only database.
func (ndbm *NDBM) LockShared() error {
	return ndbm.lock(syscall.LOCK_SH | syscall.LOCK_NB)
}

// LockExclusive locks the database for a single process.
func (ndbm *NDBM) LockExclusive() error {
	return ndbm.lock(syscall.LOCK_EX | syscall.LOCK_NB)
}

// Unlock unlocks the database.
func (ndbm *NDBM) Unlock() error {
	return ndbm.lock(syscall.LOCK_UN)
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
	if status == checkError {
		return status, Error{
			dbErrNum: int(C.dbm_error(ndbm.cDbm)),
			errnoErr: err,
		}
	}
	return status, nil
}

// Insert inserts a new entry into the database.
// Returns KeyAlreadyExists if the key already exists.
func (ndbm *NDBM) Insert(key, value []byte) error {
	status, err := ndbm.store(key, value, C.DBM_INSERT)
	if err != nil {
		return err
	}
	if status == alreadyExists {
		return KeyAlreadyExists{key}
	}
	return nil
}

// Replace inserts a new entry or overwrites an existing entry.
func (ndbm *NDBM) Replace(key, value []byte) error {
	_, err := ndbm.store(key, value, C.DBM_REPLACE)
	return err
}

// Fetch retrieves an entry value by key.
// Returns KeyNotFound if the key can't be found.
func (ndbm *NDBM) Fetch(key []byte) ([]byte, error) {
	C.dbm_clearerr(ndbm.cDbm)
	datum, err := C.dbm_fetch(ndbm.cDbm, bytesToDatum(key))
	value := datumToBytes(datum)
	if value == nil {
		dbErrNum := C.dbm_error(ndbm.cDbm)
		if dbErrNum == noError {
			return nil, KeyNotFound{key}
		}
		if dbErrNum == C.DBM_ITEM_NOT_FOUND {
			return nil, KeyNotFound{key}
		}
		return nil, Error{
			dbErrNum: int(dbErrNum),
			errnoErr: err,
		}
	}
	return value, nil
}

// Delete deletes an entry from the database.
// Returns KeyNotFound if the key can't be found.
func (ndbm *NDBM) Delete(key []byte) error {
	C.dbm_clearerr(ndbm.cDbm)
	status, err := C.dbm_delete(ndbm.cDbm, bytesToDatum(key))
	if status == checkError {
		dbErrNum := C.dbm_error(ndbm.cDbm)
		if dbErrNum == C.DBM_ITEM_NOT_FOUND {
			return KeyNotFound{key}
		}
		return Error{
			dbErrNum: int(dbErrNum),
			errnoErr: err,
		}
	}
	if status == notFound {
		return KeyNotFound{key}
	}
	return nil
}

var errNoMoreKeys = fmt.Errorf("No more keys")

func (ndbm *NDBM) firstKey() ([]byte, error) {
	datum, err := C.dbm_firstkey(ndbm.cDbm)
	key := datumToBytes(datum)
	if key == nil {
		dbErrNum := C.dbm_error(ndbm.cDbm)
		if dbErrNum == noError {
			return nil, errNoMoreKeys
		}
		return nil, Error{
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
		if dbErrNum == noError {
			return nil, errNoMoreKeys
		}
		return nil, Error{
			dbErrNum: int(dbErrNum),
			errnoErr: err,
		}
	}
	return key, nil
}

// KeysCallback executes a callback function for every key in the database.
// The callback should take a key and return an error if there is a problem.
func (ndbm *NDBM) KeysCallback(callback func([]byte) error) error {
	for key, err := ndbm.firstKey(); err == nil; key, err = ndbm.nextKey() {
		if err != nil {
			if err == errNoMoreKeys {
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

// Keys lists every key in the database.
func (ndbm *NDBM) Keys() [][]byte {
	keys := [][]byte{}
	_ = ndbm.KeysCallback(func(key []byte) error {
		keys = append(keys, key)
		return nil
	})
	return keys
}

// Len returns the number of entries in the database.
func (ndbm *NDBM) Len() int {
	count := 0
	_ = ndbm.KeysCallback(func(_ []byte) error {
		count++
		return nil
	})
	return count
}

// ValuesCallback executes a callback function for every value in the database.
// The callback should take a value and return an error if there is a problem.
func (ndbm *NDBM) ValuesCallback(callback func([]byte) error) error {
	return ndbm.KeysCallback(func(key []byte) error {
		value, err := ndbm.Fetch(key)
		if err != nil {
			return nil
		}
		return callback(value)
	})
}

// Values returns every value in the database.
func (ndbm *NDBM) Values() [][]byte {
	values := [][]byte{}
	_ = ndbm.ValuesCallback(func(value []byte) error {
		values = append(values, value)
		return nil
	})
	return values
}

// ItemsCallback executes a callback function for every entry in the database.
// The callback should take a key and value and return an error if there is a problem.
func (ndbm *NDBM) ItemsCallback(callback func(key, value []byte) error) error {
	return ndbm.KeysCallback(func(key []byte) error {
		value, err := ndbm.Fetch(key)
		if err != nil {
			return err
		}
		return callback(key, value)
	})
}

// Item is a database entry.
type Item struct {
	Key   []byte
	Value []byte
}

// Items returns every entry in the database.
func (ndbm *NDBM) Items() []Item {
	items := []Item{}
	_ = ndbm.ItemsCallback(func(key, value []byte) error {
		items = append(items, Item{
			Key:   key,
			Value: value,
		})
		return nil
	})
	return items
}

// Update takes a list of entries and upserts them into the database.
func (ndbm *NDBM) Update(items []Item) error {
	for _, item := range items {
		err := ndbm.Replace(item.Key, item.Value)
		if err != nil {
			return err
		}
	}
	return nil
}
