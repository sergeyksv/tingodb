var vows = require('vows');
var assert = require('assert');
var main = require('../lib/main');
var temp = require('temp');
var _ = require('underscore');
var async = require('async');
var Db = require('mongodb').Db,
	Server = require('mongodb').Server;
var safe = require('safe');
var loremIpsum = require('lorem-ipsum');
var Benchmark = require('benchmark');

function dummylt(how) {
	var c1 = {
		topic: function (coll) {
			return coll;
		}
	}
	for (i=0; i<how; i++) {
		c1["lt-"+i] = {
				topic:function (coll) {
					var cb = this.callback;
					coll.find({sin:{$gt:0,$lt:0.5}}).count(cb);
				},
				"ok":function (err, count) {
					assert.equal(count, gt0sin);
				}
			}
	}
	return c1;
}

var mongo = false;
var num = 10000;
var paths = {};
var gt0sin = 0;

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

var suite = new Benchmark.Suite();	
// add listeners
suite.on('cycle', function(event) {
  console.log(String(event.target));
})
.on('complete', function() {
  console.log('Fastest is ' + this.filter('fastest').pluck('name'));
})

function cb(err, coll) {
	suite.add({name:"index", defer:true, fn:function(next) {
		coll.find({sin:{$gt:0,$lt:0.5}}).count(function () {
			next.resolve();
		})
	}});	
	suite.add({name:"mix", defer:true, fn:function(next) {
		coll.find({sin:{$gt:0,$lt:0.5},t:15}).count(function () {
			next.resolve();
		})
	}});
	suite.add({name:"single", defer:true, fn:function(next) {
		coll.find({num:10}).count(function () {
			next.resolve();
		})
	}});	
	suite.run({async:true,maxTime:240});	
}

getDb('bench', true, function (err, db) {
	db.collection("test", {}, safe.sure(cb, function (coll) {
		coll.ensureIndex({sin:1}, safe.sure(cb, function () {
			coll.ensureIndex({num:1}, safe.sure(cb, function () {			
		var i=0;
		async.whilst(function () { return i<num}, 
			function (cb) {
				var obj = {num:i,sin:Math.sin(i),cos:Math.cos(i),t:15,junk:loremIpsum({count:5,units:"paragraphs"})};
				coll.insert(obj, cb);
				if (obj.sin>0 && obj.sin<0.5)
				   gt0sin++;
				i++;
			},
			function (err) {
				cb(err,coll);
			}
		)
	}))
	}))
	}))
})
