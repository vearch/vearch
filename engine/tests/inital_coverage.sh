#!/bin/sh

echo "start initialize lcov coverage"
pushd ..
lcov -d build -z
lcov -d build -b . --no-external --initial -c -o gamma_test_init_coverage.info
popd
