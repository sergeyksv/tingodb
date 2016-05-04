var vows = require('vows');
var assert = require('assert');
var Benchmark = require('benchmark');
var tutils = require('./utils');
var safe = require('safe');
var async = require('async');

var suite = new Benchmark.Suite();
// add listeners
suite.on('cycle', function(event) {
  console.log(String(event.target));
})
.on('complete', function() {
  console.log('Fastest is ' + this.filter('fastest').map('name'));
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

tutils.getDb('bench', true, function (err, db) {
	db.collection("test", {}, safe.sure(cb, function (coll) {
//		coll.ensureIndex({sin:1}, safe.sure(cb, function () {
//			coll.ensureIndex({num:1}, safe.sure(cb, function () {
		var i=0;
		safe.whilst(function () { return i<1000},
			function (cb) {
				var obj = {num:i,sin:Math.sin(i),cos:Math.cos(i),t:15};
				coll.insert(obj, cb);
				i++;
			},
			function (err) {
				cb(err,coll);
			}
		)
//	}))
//	}))
	}))
})
