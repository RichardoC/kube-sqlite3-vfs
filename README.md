# kube-sqlite3-vfs
sqlite3 using kubernetes as the backing storage

## How it works

creates a namespace per file requested, which is named the base32 encoding of the filename
the namespace contains
a configmap called "lockfile" which contains the lock information
a series of configmaps named chunk-NUMBER which contain 64kB of data each

namespaces all labelled with "kube-sqlite3-vfs": "used" to ease cleanup


TODO: all locking/unlocking

Must use only one ns as k3s gets unhappy with namespaces being created/deleted so often.
Configmap names can be 253 characters long
Not convinced writing is working correctly

TODO add relevant-file label to lockfile too


CURRENT ISSUE - something about opening the journal file, despite it being turned off