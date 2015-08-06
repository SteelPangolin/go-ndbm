// The gdbm-ndbm.h header supplied with gdbm 1.8 doesn't include complete
// function prototypes, which confuses cgo, so we need this replacement.
//
// gdbm 1.11, the current version, looks like it has a working ndbm.h and
// won't need this. However, Ubuntu 12 through 15 (current)
// and Debian 7 and 8 (current) all use gdbm 1.8.

#pragma once

#include <sys/types.h>
#include <gdbm.h>

typedef struct {
	int dummy[10];
} DBM;

#define DBM_INSERT 0
#define DBM_REPLACE 1

void dbm_close(DBM* db);
int dbm_delete(DBM* db, datum key);
datum dbm_fetch(DBM* db, datum key);
datum dbm_firstkey(DBM* db);
datum dbm_nextkey(DBM* db);
DBM *dbm_open(const char* file, int open_flags, mode_t file_mode);
int dbm_store(DBM* db, datum key, datum content, int store_mode);
int dbm_dirfno(DBM* db);
void dbm_clearerr(DBM* db);
int dbm_error(DBM* DB);

// gdbm dbm_delete doesn't return 1 if the item is missing like BSD ndbm.
// Instead, we need to check the error code.
#define DBM_ITEM_NOT_FOUND GDBM_ITEM_NOT_FOUND
