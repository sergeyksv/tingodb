// run integra tests using mocha

var assert = require('assert');
var config = require('./configurations/single_server').single_server_config;
var _ = require('lodash');

var dir = './tests/functional';
var files = [
	'collection_tests',
	'cursor_tests',
	'find_tests',
	'insert_tests',
	'remove_tests'
];

_(files).each(function (file) {
	var tests = require(dir + '/' + file);
	describe(file, function () {
		_(tests).each(function (fn, name) {
			if (typeof fn != 'function') return;
			it(name, function (done) {
				var configuration = new (config())();
				var test = {
					ok: function (x) { assert.ok(x); },
					equal: function (x, y) { assert.equal(x, y); },
					deepEqual: function (x, y) { assert.deepEqual(x, y); },
					throws: function (x, y) { assert.throws(x, y); },
					done: done
				};
				fn(configuration, test);
			});
		});
	});
});
