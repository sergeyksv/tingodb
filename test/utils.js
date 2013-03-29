var main = require('../lib/main');
var temp = require('temp');
var _ = require('underscore');
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
			temp.mkdir(tag, function (err, path) {
				paths[tag] = path;
				main.open(path, {}, cb)
			})		
		} else
			main.open(paths[tag], {}, cb)
	}
}

module.exports.getDb = getDb;
