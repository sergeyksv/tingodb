var main = require('../lib/main')({_tiar:0});
var temp = require('temp');
var _ = require('lodash');
var async = require('async');
var Db = require('mongodb').Db,
	Server = require('mongodb').Server;
var safe = require('safe');

var mongo = false;
var paths = {};

function getDb(tag,drop,cb) {
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
}

module.exports.getDbSync = function (tag,drop) {
	if (drop)
		delete paths[tag];
	if (!paths[tag]) {
		paths[tag] = temp.mkdirSync(tag);
	} 
	return new main.Db(paths[tag], {name:tag})
}

module.exports.getDb = getDb;
