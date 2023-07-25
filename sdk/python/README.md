# Gamma Python SDK

gamma python sdk and python wheel packages.

## Overview

This repository shows gamma python sdk and provides scripts to create wheel
packages for the gamma library.

[python sdk api](./docs/APIPythonSDK.md) is the document of python sdk api.
Files in directory of python shows how the python sdk encapsulate gamma.
setup.py is written for creating wheel packages for gamma.

Of course, pip install vearch is the easiest way to use this python sdk. And
this repository helps to build your custom python sdk.

## Building source package

if thers is a custom built gamma library in the system, build source package
for the best performance.

### Prerequisite

You can build it with docker image: docker.io/dockcross/manylinux2014-x64:latest

auditwheel tool should be installed firstly. You can install it by pip.

The package can be built when gamma is already built and installed.
See the official [gamma installation
instruction](https://github.com/vearch/gamma/blob/master/README.md) for more
on how to build and install gamma. In particular, compiling wheel packages
requires additional compilation options in compiling gamma.

```bash
git clone https://github.com/vearch/vearch.git
cd vearch/engine
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DPERFORMANCE_TESTING=ON ..
make
sh build-wheels.sh
sh install-vearch.sh
```

Then the whl file will be generated into the wheelhouse directory.

For building wheel packages, swig 3.0.12 or later needs to be avaiable.

### Linux

In linux, `auditwheel` is used for creating python wheel packages ocntains
precompiled binary extensions.
Header locations and link flags can be customized by `GAMMA_INCLUDE` and
`GAMMA_LDFLAGS` environment variables for building wheel packages.
Windows and OSX are not supported yet.
