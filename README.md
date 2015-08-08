## ndbm

[![Github license](https://img.shields.io/github/license/SteelPangolin/go-ndbm.svg?style=flat)](https://github.com/SteelPangolin/go-ndbm)
[![Travis CI build status](https://img.shields.io/travis/SteelPangolin/go-ndbm.svg?style=flat)](https://travis-ci.org/SteelPangolin/go-ndbm)
[![GoDoc](https://godoc.org/github.com/SteelPangolin/go-ndbm?status.svg)](https://godoc.org/github.com/SteelPangolin/go-ndbm)

State of the art structured storage for 1986.

`ndbm` is a Go wrapper around the POSIX NDBM database interface.
NDBM is built into Mac OS X and FreeBSD,
and is available as a compatibility mode of the GDBM library on Linux.

[API documentation is available on GoDoc](https://godoc.org/github.com/SteelPangolin/go-ndbm).

### Why?

I wrote the first version of this library on a Mac on a cruise ship,
where I needed a persistent key-value store, but had no Internet connection,
just OS X system libraries, man pages, and Go documentation.

### Linux notes

Most Linux distros don't package an NDBM implementation.
Debian/Ubuntu users should install
[`libgdbm3`](http://packages.ubuntu.com/trusty/libgdbm3)
and [`libgdbm-dev`](http://packages.ubuntu.com/trusty/libgdbm-dev)
before building.

To make this package `go get`table, I've targeted the quirky GDBM 1.8,
which is the version available on Ubuntu 12-15 and Debian 7-8,
instead of attempting build-time library detection,
which would allow using the current and more NDBM-compatible GDBM 1.11.

If you are using a recent GDBM that provides `ndbm.h`, you will probably need
to delete `gdbm_compat.h` and `gdbm_compat_linux.c` and the `#ifdef __linux` checks.

### Background

POSIX standard for NDBM:

* http://pubs.opengroup.org/onlinepubs/9699919799/functions/dbm_open.html
* http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/ndbm.h.html

Mac OS X NDBM:

* https://developer.apple.com/library/mac/documentation/Darwin/Reference/ManPages/man3/dbm_open.3.html
  (Note that this is a BSD man page, and the datum struct described is wrong for OS X.)
* http://www.opensource.apple.com/source/Libc/Libc-997.90.3/include/ndbm.h

FreeBSD NDBM:

* https://www.freebsd.org/cgi/man.cgi?query=dbm&apropos=0&sektion=3&manpath=FreeBSD+10.0-RELEASE&format=html
* https://github.com/freebsd/freebsd/blob/master/lib/libc/db/hash/ndbm.c
* https://github.com/freebsd/freebsd/blob/master/include/ndbm.h

GDBM:

* http://www.gnu.org.ua/software/gdbm/manual/html_node/ndbm.html
