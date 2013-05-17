#!/bin/sh

TESTS="basic-test.js import-test.js search-test.js search-array-test.js contrib-test.js"

COV="coverage.html"
MOCHA="node_modules/.bin/mocha"

# get absolute paths
COV="$(readlink -f "$COV")"
MOCHA="$(readlink -f "$MOCHA")"

if [ "$(basename "$0")" = "test-cov.sh" ]; then
	if ! command -v jscoverage >/dev/null 2>&1; then
		echo "Error: JSCoverage tool not found."
		echo "Please install it from https://github.com/visionmedia/node-jscoverage"
		exit 1
	fi
	echo "Generating coverage report..."
	rm -rf lib-cov
	jscoverage lib lib-cov
	cd test
	TINGODB_COV=1 "$MOCHA" -R html-cov $TESTS > "$COV"
	echo "Done."
else
	cd test
	"$MOCHA" $TESTS
fi
