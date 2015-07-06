var assert = require('assert');
var _ = require('lodash');
var async = require('async');
var safe = require('safe');
var tutils = require("./utils");

describe('Misc', function () {
	var db,coll;
	before(function (done) {
		tutils.getDb('misc', true, safe.sure(done, function (_db) {
			db = _db;
			done();
		}))
	})
	it('GH-20,GH-17 toArray should not fail/close with one record or count', function (done) {
		db.collection("GH2017", {}, safe.sure(done,function (_coll) {
			_coll.insert({}, safe.sure(done, function () {
				var cursor = _coll.find();
				cursor.count(safe.sure(done, function (res) {
					assert.equal(res,1);
					cursor.toArray(safe.sure(done, function (res) {
						done();
					}))
				}))
			}))
		}))
	})
	it('GH-19 Unset must clean key from object', function (done) {
		db.collection("GH19", {}, safe.sure(done,function (_coll) {
			_coll.insert({name:'Tony',age:'37'}, safe.sure(done, function () {
				_coll.findAndModify({},{age:1},{$set: {name: 'Tony'}, $unset: { age: true }},{new:true},safe.sure(done, function (doc) {
					assert(!_.contains(_.keys(doc),'age'));
					_coll.findOne({},{age:1},safe.sure(done, function (obj) {
						assert(!_.contains(_.keys(obj),'age'));
						done();
					}))
				}))
			}))
		}))
	})
	it('GH-14 Exclude projection for _id can be mixed with include projections', function (done) {
		db.collection("GH14", {}, safe.sure(done,function (_coll) {
			_coll.insert({name:'Tony',age:'37'}, safe.sure(done, function () {
				_coll.findOne({},{_id:0,age:1}, safe.sure(done, function (obj) {
					assert(!_.contains(_.keys(obj),'_id'));
					assert(_.contains(_.keys(obj),'age'));
					assert(!_.contains(_.keys(obj),'name'));
					_coll.findOne({},{age:1}, safe.sure(done, function (obj) {
						assert(_.contains(_.keys(obj),'_id'));
						assert(_.contains(_.keys(obj),'age'));
						assert(!_.contains(_.keys(obj),'name'));
						_coll.findOne({},{age:0}, safe.sure(done, function (obj) {
							assert(_.contains(_.keys(obj),'_id'));
							assert(!_.contains(_.keys(obj),'age'));
							assert(_.contains(_.keys(obj),'name'));
							_coll.findOne({},{_id:0,age:0}, safe.sure(done, function (obj) {
								assert(!_.contains(_.keys(obj),'_id'));
								assert(!_.contains(_.keys(obj),'age'));
								assert(_.contains(_.keys(obj),'name'));
								_coll.findOne({},{_id:1,age:0}, function (err) {
									assert(err!=null)
									done();
								})
							}))
						}))
					}))
				}))
			}))
		}))
	})
	it('GH-26 sort order can also be optional for findAndRemove', function (done) {
		db.collection("GH26", {}, safe.sure(done,function (_coll) {
			_coll.insert({}, safe.sure(done, function () {
				_coll.findAndRemove({},safe.sure(done, function (res) {
					done();
				}))
			}))
		}))
	})
	it('GH-21 $in should work as the intersection between the property array and the parameter array', function(done) {
		db.collection("GH21", {}, safe.sure(done,function (_coll) {
			_coll.insert({item: "abc", qty: 10, tags: [ "school", "clothing" ], sale: false }, safe.sure(done, function () {
				_coll.find({tags: { $in: ["appliances", "school"] }},{"_tiar.tags":0}).toArray(safe.sure(done, function (docs) {
					assert(docs.length,1);
					done()
				}))
			}))
		}))
	})
	it('GH-65 $regex modifier does not work', function (done) {
		db.collection("GH65", {}, safe.sure(done,function (_coll) {
			_coll.insert([{hello:'world_safe1'}, {hello:'world_safe2'}], {w:1}, safe.sure(done, function(result) {
				_coll.findOne({hello: {$regex: /^world_safe2$/}}, safe.sure(done, function(item) {
					assert.equal('world_safe2', item.hello);
					done()
				}))
			}))
		}))
	})
	it('GH-56 escaping regex characters in regex search', function (done) {
		db.collection("GH56", {}, safe.sure(done,function (_coll) {
			_coll.insert([{path: '/aaa/bbb/ccc/'}, {path:'/aaa/bbb'}, {path:'/aaa/bbb/qqq/'}], {w:1}, safe.sure(done, function(result) {
				_coll.find({path: {$regex: '\/aaa\/'}}).toArray( safe.sure(done, function(docs) {
					assert(docs.length,1);
					done()
				}))
			}))
		}))
	})
});
