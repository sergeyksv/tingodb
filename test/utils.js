var main = require('../lib/main')({_tiar:0});
var temp = require('temp');
var _ = require('lodash');
var async = require('async');
var Db = require('mongodb').Db,
	Server = require('mongodb').Server;
var safe = require('safe');

var cfg = { db: "tingodb" };
var mongo = false;
module.exports.setConfig = function (cfg_) {
	_.defaults(cfg_, cfg);
	cfg = cfg_;
	mongo = cfg.db == 'mongodb';
};

var paths = {};

module.exports.getDb = function getDb(tag, drop, cb) {
	if (mongo) {
		var dbs = new Db(tag, new Server('localhost', 27017),{w:1});
		dbs.open(safe.sure(cb, function (db) {
			if (drop) {
				db.dropDatabase(safe.sure(cb, function () {
					var dbs = new Db(tag, new Server('localhost', 27017),{w:1});					
					dbs.open(cb)
				}))
			} else
				cb(null,db)
		}))
	}
	else {
		if (drop)
			delete paths[tag];
		if (!paths[tag]) {
			paths[tag] = temp.mkdirSync(tag);
		} 
		var db = new main.Db(paths[tag], {});
		db.open(cb);
	}
};

module.exports.getDbSync = function (tag, db_options, server_options) {
	if (mongo) {
		return new Db(tag, new Server('localhost', 27017, server_options), db_options);
	} else {
		if (!paths[tag]) {
			paths[tag] = temp.mkdirSync(tag);
		} 
		return new main.Db(paths[tag], {name:tag});
	}
};

module.exports.openEmpty = function (db, cb) {
	db.open(safe.sure(cb, function () {
		if (mongo) {
			db.dropDatabase(cb);
		} else {
			db.collectionNames({namesOnly: true}, safe.sure(cb, function (names) {
				async.forEachSeries(names, function (name, cb) {
					db.dropCollection(name, cb);
				}, cb);
			}));
		}
	}));
};

module.exports.getDbPackage = function () {
	return mongo ? require('mongodb') : main;
};
