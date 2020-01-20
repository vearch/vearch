#!/bin/bash

ROOT=$(dirname "$PWD")
BUILDOUT=$ROOT/build/bin/
mkdir -p $BUILDOUT
GAMMAOUT=$ROOT/build/gamma_build
mkdir -p $GAMMAOUT

#if want to build python, set PYTHON_FLAG=ON
PYTHON_FLAG="OFF"
if [ $PYTHON_FLAG == 'ON' ]
then
    PYTHONOUT=$ROOT/build/python
    mkdir -p $PYTHONOUT
    PYTHONROOT=$ROOT/python
fi

# version value
BUILD_VERSION="0.3"

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ROOT/ps/engine/gammacb/lib/lib/

if [ ! -n "$FAISS_HOME" ]; then
  export FAISS_HOME=$ROOT/ps/engine/gammacb/lib/faiss
fi


flags="-X 'main.BuildVersion=$BUILD_VERSION' -X 'main.CommitID=$(git rev-parse HEAD)' -X 'main.BuildTime=$(date +"%Y-%m-%d %H:%M.%S")'"

echo "version info: $flags"

echo "build gamma"

#if don't want to build python, set -DBUILD_PYTHON=OFF
cd $GAMMAOUT
cmake -DPERFORMANCE_TESTING=ON -DBUILD_PYTHON=$PYTHON_FLAG -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$ROOT/ps/engine/gammacb/lib $ROOT/engine/gamma/
make gamma -j  && make install

if [ $PYTHON_FLAG == 'ON' ]
then
    cp *swigvearch* $PYTHONOUT
fi

cd ../

echo "build vearch"
go build -a -tags="vector" -ldflags "$flags" -o $BUILDOUT/vearch $ROOT/startup.go

echo "build deploy tool"
go build -a -ldflags "$flags" -o $BUILDOUT/batch_deployment $ROOT/tools/deployment/batch_deployment.go

if [ $PYTHON_FLAG == 'ON' ]
then
    echo "build python vearch wheel"
    cd $PYTHONOUT
    cp $PYTHONROOT/setup.py $PYTHONROOT/vearch.py .
    python setup.py bdist_wheel
fi
