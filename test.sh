#!/bin/sh

cd ./test
../node_modules/.bin/mocha basic-test.js import-test.js search-test.js search-array-test.js contrib-test.js
