var assert = require('assert');
var _ = require('lodash');
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
					assert(!_.includes(_.keys(doc),'age'));
					_coll.findOne({},{age:1},safe.sure(done, function (obj) {
						assert(!_.includes(_.keys(obj),'age'));
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
					assert(!_.includes(_.keys(obj),'_id'));
					assert(_.includes(_.keys(obj),'age'));
					assert(!_.includes(_.keys(obj),'name'));
					_coll.findOne({},{age:1}, safe.sure(done, function (obj) {
						assert(_.includes(_.keys(obj),'_id'));
						assert(_.includes(_.keys(obj),'age'));
						assert(!_.includes(_.keys(obj),'name'));
						_coll.findOne({},{age:0}, safe.sure(done, function (obj) {
							assert(_.includes(_.keys(obj),'_id'));
							assert(!_.includes(_.keys(obj),'age'));
							assert(_.includes(_.keys(obj),'name'));
							_coll.findOne({},{_id:0,age:0}, safe.sure(done, function (obj) {
								assert(!_.includes(_.keys(obj),'_id'));
								assert(!_.includes(_.keys(obj),'age'));
								assert(_.includes(_.keys(obj),'name'));
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

	it('GH-26-1 sort order and opts can be undefined for findAndRemove', function (done) {
		db.collection("GH26-1", {}, safe.sure(done,function (_coll) {
			_coll.insert({}, safe.sure(done, function () {
				_coll.findAndRemove({},undefined,undefined,safe.sure(done, function (res) {
					done();
				}))
			}))
		}))
	})

	it('GH-26-2 sort order can also be optional and undefined for findAndRemove', function (done) {
		db.collection("GH26-2", {}, safe.sure(done,function (_coll) {
			_coll.insert({name:'Tony',age:'37'}, safe.sure(done, function () {
				_coll.findAndModify({},{age:1},{$set: {name: 'Tony'}, $unset: { age: true }},undefined,safe.sure(done, function (doc) {
					assert(doc.age);
					done();
				}))
			}))
		}))
	})

	it('GH-26-3 doc can be undefined if remote is true for findAndModify', function (done) {
		db.collection("GH26-3", {}, safe.sure(done,function (_coll) {
			_coll.insert({name:'Tony',age:'37'}, safe.sure(done, function () {
				_coll.findAndModify({},{age:1},undefined,{remove:true},safe.sure(done, function (doc) {
					assert(doc.age);
					done();
				}))
			}))
		}))
	})

	it('GH-26-3 optional find paramater can be undefined', function (done) {
		db.collection("GH26-3", {}, safe.sure(done,function (_coll) {
			_coll.insert({name:'Tony',age:'37'}, safe.sure(done, function () {
				_coll.find({},undefined,undefined).toArray(safe.sure(done, function (docs) {
					assert(docs.length==1);
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
	it('GH-84 double escape for . can not be found in tingodb but in mongodb', function (done) {
		db.collection("GH84", {}, safe.sure(done,function (_coll) {
			_coll.insert([{hello:'.'}, {hello:'s'},{hello:'smething else'}], {w:1}, safe.sure(done, function(result) {
				_coll.find({hello: new RegExp('^\.$')}).toArray(safe.sure(done, function(items) {
					assert.equal(items.length,2);
					assert.equal('.', items[0].hello); //normally escaped works
					_coll.find({hello: /^\.$/}).toArray(safe.sure(done, function(items) {
						assert.equal(items.length,1);
						assert.equal('.', items[0].hello); //normally escaped works
						done();
					}));
				}));
			}));
		}));
	});

	it('regexp with double quotes', function (done) {
		db.collection("doublequotes", {}, safe.sure(done,function (_coll) {
			_coll.insert([{hello:'"quote"'}, {hello:'""'}], {w:1}, safe.sure(done, function(result) {
				_coll.find({hello: new RegExp('^".*"$')}).toArray(safe.sure(done, function(items) {
					assert.equal(items.length,2);
					_coll.find({hello: new RegExp('^""$')}).toArray(safe.sure(done, function(items) {
						assert.equal(items.length,1);
						done();
					}));
				}));
			}));
		}));
	});

	it('GH-85 numberic properties should be supported', function (done) {
		db.collection("GH85", {}, safe.sure(done,function (_coll) {
			_coll.insert({1234:"test",nested:{level:{123:"test"}}}, safe.sure(done, function () {
				_coll.findOne({1234:"test"},safe.sure(done, function (res) {
					assert(res);
					assert.equal(res[1234],"test");
					_coll.findOne({"nested.level.123":"test"},safe.sure(done, function (res) {
						assert(res);
						assert.equal(res.nested.level[123],"test");
						done();
					}));
				}));
			}));
		}));
	});

	it('GH-88 multiple fields search by range', function (done) {

		var samples = [
			{"date2": "2008-12-29T23:00:00.000Z", "n_length": 16},
			{"date2": "2009-12-29T23:00:00.000Z", "n_length": 5},
			{"date2": "2009-12-30T23:00:00.000Z", "n_length": 10},
			{"date2": "2009-12-28T23:00:00.000Z", "n_length": 11},
			{"date2": "2008-12-29T23:00:00.000Z", "n_length": 16},
			{"date2": "2008-12-29T23:00:00.000Z", "n_length": 16}
		]

		var q = {
			date2: {$gte: '2009-01-01T00:00:00.000Z', $lt: '2010-01-02T00:00:00.000Z'},
			n_length: {$gte: 8, $lte: 10}
		}

		db.collection("GH88", {}, safe.sure(done, function (_coll) {
			_coll.insert(samples, safe.sure(done, function () {

				_coll.createIndex({"date2": 1}, safe.sure(done, function (indexname) {
					_coll.find(q).toArray(safe.sure(done, function (res) {
						assert(res)
						assert.equal(res.length,1)
						assert(res[0].date2 === '2009-12-30T23:00:00.000Z')
						done()
					}));
				}));
			}));
		}));
	});

});
