# Gamma 
Gamma is the core vector search engine of Vearch. It is a high-performance, concurrent vector search engine, and supports real time indexing vectors and scalars without lock.

## Installation
1. `mkdir build`
2. `cd build`
3. `cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=gamma/ ..`
4. `make`
5. `make install`

## TEST
1. `mkdir build`
2. `cd build`
3. `cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_TEST=ON -DCMAKE_INSTALL_PREFIX=gamma/ ..`
4. `make -j`
5. `cd tests && cp ../../tests/profile_10k.txt .`
6. `wget ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz && tar -zxvf siftsmall.tar.gz`
7. `./test_files profile_10k.txt siftsmall/siftsmall_base.fvecs`
Currently we support gamma both on Linux and OSX of x86_64 machines. We have tested on Centos, Ubuntu and Mac os. And we all just tested with gcc on both Linux and OSX.
