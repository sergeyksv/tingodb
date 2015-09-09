#! /usr/bin/env node

var path = require('path');

var Mocha = require('mocha');
var mocha = new Mocha();
var safe = require('safe');

var optimist = require('optimist')
	.default('reporter','dot')
	.alias('R',"reporter")
	.default('db', 'tingodb')
	.describe('db', 'tingodb | mongodb')
    .describe('h', 'Display the usage')
    .alias('h', 'help')
	.boolean('quick')
	.alias('q', 'quick')
	.describe('quick', 'Run only quick tests')
	.describe('single', 'Run only single file')
	.describe('default', 'Run only default configuration check')
	.check(function (argv) {
		return argv.db == 'tingodb' || argv.db == 'mongodb';
	})
	.usage();
var argv = optimist.argv;

if (argv.help) {
    optimist.showHelp();
    process.exit(0);
}

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
	'crud-test.js',
	'misc-test.js',
	"update-test.js"
];
var tingo = [
	'compact-test.js',
	'integrity-test.js'
];

var slow = [
	'import-test.js',
	'contrib-test.js'
];

if (argv.single) {
	files = [argv.single];
} else {
	if (!config.mongo) files = files.concat(tingo);
	if (!argv.quick) files = files.concat(slow);
}

files.forEach(function (file) {
	mocha.addFile(path.join(__dirname, file));
});

mocha.timeout(5000);
mocha.reporter(argv.reporter);

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

if (!config.mongo && !argv.default) {
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
	sessions.push(function (cb) {
		mocha.grep(/(FS)/);
		mocha.invert();
		console.log('InMemory using defaults');
		tutils.setConfig({ memStore:true });
		run(cb);
	})
	sessions.push(function (cb) {
		console.log('InMemory using global searchInArray');
		tutils.setConfig({ memStore:true, searchInArray: true , nativeObjectID: false });
		run(cb);
	})
	sessions.push(function (cb) {
		console.log('InMemory using BSON ObjectID');
		tutils.setConfig({ memStore:true, searchInArray: false , nativeObjectID: true });
		run(cb);
	})
}

safe.series(sessions, function (err) { if (err) console.log(err); process.exit(0)});
