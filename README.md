odbc driver written in go. Implements database driver interface as used by standard database/sql package. It calls into odbc dll on Windows, and uses cgo (unixODBC) everywhere else.

modified to user SQLExecDirect instead of SQLExecute

warning: not all functionality has been tested! Things may not work

To get started using odbc, have a look at the [wiki](../../wiki) pages.
