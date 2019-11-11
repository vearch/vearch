# Profile User Guide for Gamma Engine


Profile modules store profile index data, including fixed-length numeric types and variable-length string types

name| meaning|is fixed-length
----|-------|------
INT|32-bit integer|yes
LONG|64-bit integer|yes
FLOAT|32-bit float|yes
DOUBLE|64-bit double|yes
STRING|string type|no
---
Profile index data is stored in memory at run time, providing load and dump functions. Fixed-length field and variable-length field are stored in two memory lines, in which string field stores the beginning address and length of string in fixed-length field.

Dump to two files
name| usage
----|----|----
.prf|storage of index table structure and fixed-length data
.str.prf|storage of string and variable-length fields
