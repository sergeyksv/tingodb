#! /usr/bin/env node

var path = require('path');
var Mocha = require('mocha');
var mocha = new Mocha();

var files = [ 'basic-test.js', 'import-test.js', 'search-test.js',
    'search-array-test.js', 'sort-test.js', 'contrib-test.js' ];

files.forEach(function (file) {
	mocha.addFile(path.join(__dirname, file));
});

mocha.run();
