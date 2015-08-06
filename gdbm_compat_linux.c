#include "gdbm_compat.h"

// Emulate missing error functions.

void dbm_clearerr(DBM* db) {
	gdbm_errno = 0;
}

int dbm_error(DBM* db) {
	return gdbm_errno; 
}
