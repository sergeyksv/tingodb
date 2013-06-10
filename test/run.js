#! /usr/bin/env node

var path = require('path');

var Mocha = require('mocha');
var mocha = new Mocha();

var argv = require('optimist')
	.default('db', 'tingodb')
	.describe('db', 'tingodb | mongodb')
	.boolean('quick')
	.alias('q', 'quick')
	.describe('quick', 'Run only quick tests')
	.describe('single', 'Run only single file')
	.check(function (argv) {
		return argv.db == 'tingodb' || argv.db == 'mongodb';
	})
	.argv;

var config = {
	mongo: argv.db == 'mongodb'
};

var tutils = require('./utils.js');
tutils.setConfig(config);

var files = [
	'basic-test.js',
	'delete-test.js'
];
var tingo = [
	'compact-test.js'
];
var slow = [
	'import-test.js',
	'search-test.js',
	'search-array-test.js',
	'sort-test.js',
	'contrib-test.js'
];

if (argv.single) {
	files = [argv.single]
} else {
	if (!config.mongo) files = files.concat(tingo);
	if (!argv.quick) files = files.concat(slow);
}

files.forEach(function (file) {
	mocha.addFile(path.join(__dirname, file));
});

mocha.timeout(5000);

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

console.log('Using default ObjectID');
run(function () {
	if (config.mongo) process.exit(0);
	console.log('Using BSON ObjectID');
	tutils.setConfig({ nativeObjectID: true });
	run();
});
