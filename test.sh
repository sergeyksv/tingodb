#!/bin/sh

cd ./test
../node_modules/.bin/vows import-test.js search-test.js search-array-test.js --isolate
../node_modules/.bin/mocha basic-test.js contrib-test.js
