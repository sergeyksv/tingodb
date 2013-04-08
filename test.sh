#!/bin/sh

./node_modules/.bin/vows test/*-test.js --isolate
cd ./test/contrib
node ./test/runner.js -t functional
cd ../..