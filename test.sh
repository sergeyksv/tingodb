#!/bin/sh

cd ./test
../node_modules/.bin/vows import-test.js search-test.js search-array-test.js --isolate
../node_modules/.bin/mocha basic-test.js
cd ./contrib
../../node_modules/.bin/mocha -t 10000 test/mocher.js
cd ../..
