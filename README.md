# kube-sqlite3-vfs
sqlite3 using kubernetes as the backing storage

## How it works

Creates a set of configmaps per file requested, which is named a base32 encoding of the filename
the namespace contains
a configmap called "lockfile" which contains the lock information
a series of configmaps named which contain up to 64kB of data each

namespaces all labelled with "kube-sqlite3-vfs": "used" to ease cleanup

Configmap names can be 253 characters long after encoding

## WARNINGS

This is really very slow, and using an in memory journal so is very likely to corrupt your data!

## TODO

* have filesize be O(1) rather than O(n)