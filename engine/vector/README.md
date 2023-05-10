# Raw Vector User Guide for Gamma Engine


This module is responsible for storing raw vectors. Raw Vector is the base class, it may has different implementations according to the storage media. Currently, Only in-memory implementation is supported, it is called Memory Raw Vector.

## Memory Raw Vector

### Memory Structure


vector\_mem: stores all vectors in sequential memory space, Each vector has fixed dimension. If the dimension is 512, so v\_1's begining address is 0, v\_2's begining address is 512, as shown in the figure below. The begining address of each vector can be derived by it's id and dimension. 

source\_mem: stores all sources in sequential memory space too, but source's length is not fixed.

source\_pos: stores the begining address of each source in source_mem. Combine source\_pos and source\_mem, it can find any source of vector, just need the id of vector.
 
![memory_struct](/doc/img/vector/memory_structure.png)

### File Structure

Dump to two files

name| usage
----|----|----
.fet|storage of all vectors
.src|storage of all sources of vector

