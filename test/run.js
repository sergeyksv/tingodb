#! /usr/bin/env node

var path = require('path');

var Mocha = require('mocha');
var mocha = new Mocha();
var async = require('async');

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
	'delete-test.js',
	'index-test.js',
	'search-test.js',
	'search-array-test.js',
	'sort-test.js',	
	'crud-test.js'
];
var tingo = [
	'compact-test.js'
];
var slow = [
	'import-test.js',
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

var sessions = [
	function (cb) {
		console.log('Using defaults');
		run(cb);
	}
]

if (!config.mongo) {
	sessions.push(function (cb) {
		console.log('Using global searchInArray');
		tutils.setConfig({ searchInArray: true , nativeObjectID: false });
		run(cb);
	})
	sessions.push(function (cb) {
		console.log('Using BSON ObjectID');
		tutils.setConfig({ searchInArray: false , nativeObjectID: true });
		run(cb);
	})	
}

async.series(sessions, function () { process.exit(0)});
