#!/bin/sh

echo "start report lcov coverage"
pushd ..
COV_TOTAL=gamma_test_coverage.info
COV_FILTER=gamma_test_filtered.info

lcov -d build -b . --no-external -c -o gamma_test_coverage.info
lcov --remove ${COV_TOTAL} '*/tests/*' '*third_party/*' '*/idl/*' --output-file ${COV_FILTER}
genhtml --prefix=`pwd` ${COV_FILTER} --output-directory=build/coverage

popd
