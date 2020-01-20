# Gamma 
Gamma is the core vector search engine of VectorBase. It is a high-performance, concurrent vector search engine, and supports real time indexing vectors and scalars without lock. Differently from general vector search engine, Gamma can store and index a document which contains scalars and vectors, and provides the ablity of quickly indexing and filtering by numeric scalar fields. The work of design and implementation of real time indexing has been publish in [our Middleware paper](https://arxiv.org/abs/1908.07389).
As for the part of similarity search of vectors in Gamma, it is mainly implemented based on faiss which is an open source library developed by Facebook AI Research. Besides faiss, it can easily support other approximate nearest neighbor search(ANN) algorithms or libraries. 

## Requirements 
* [Faiss](https://github.com/facebookresearch/faiss)

## Installation
1. `mkdir build`
2. `cd build`
3. `cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=gamma/ ..`
4. `make`
5. `make install`

Currently we support gamma both on Linux and OSX of x86_64 machines. We have tested on Centos, Ubuntu and Mac os. And we all just tested with gcc on both Linux and OSX.

## Issue Report


## References
Please cite this paper when referencing Gamma.
Jie Li, Haifeng Liu, Chuanghua Gui, Jianyu chen, Zhenyun Ni, Ning Wang, Yuan Chen. [The Design and Implementation of a Real Time Visual Search System on JD E-commerce Platform](https://arxiv.org/abs/1908.07389). In the 19th International ACM Middleware Conference, December 10–14, 2018, Rennes, France.
 
## License
Licensed under the Apache License, Version 2.0. For detail see LICENSE and NOTICE.
