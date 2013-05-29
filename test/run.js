#! /usr/bin/env node

var path = require('path');

var Mocha = require('mocha');
var mocha = new Mocha();

var argv = require('optimist')
	.default('db', 'tingodb')
	.describe('db', 'tingodb | mongodb')
	.check(function (argv) {
		return argv.db == 'tingodb' || argv.db == 'mongodb';
	})
	.argv;

var tutils = require('./utils.js');
tutils.setConfig({
	db: argv.db
});

var files = [
	'basic-test.js',
	'import-test.js',
	'search-test.js',
	'search-array-test.js',
	'sort-test.js',
	'contrib-test.js'
];

files.forEach(function (file) {
	mocha.addFile(path.join(__dirname, file));
});

function run(cb) {
	tutils.startDb(function (err) {
		if (err) throw err;
		mocha.run(function (failures) {
			tutils.stopDb(function (err) {
				if (err) throw err;
				if (failures) process.exit(failures);
				if (cb) cb();
			});
		});
	});
}

console.log('Using tingodb ObjectID');
run(function () {
	console.log('Using BSON ObjectID');
	tutils.setConfig({ nativeObjectID: true });
	run();
});
