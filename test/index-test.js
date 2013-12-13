var assert = require('assert');
var _ = require('lodash');
var async = require('async');
var safe = require('safe');
var tutils = require("./utils");

function eq(docs1, docs2) {
	assert.equal(docs1.length, docs2.length);
	_(docs1).each(function (a, i) {
		var b = docs2[i];
		assert.equal(a.a, b.a);
		assert.equal(a.b, b.b);
		assert.equal(a.s, b.s);
	});
}

describe('Index Test', function () {
	describe('New store', function () {
		var db, coll, docs;
		before(function (done) {
			tutils.getDb('test', true, safe.sure(done, function (_db) {
				db = _db;
				done();
			}));
		});
		it("Create new collection", function (done) {
			db.collection("test", {}, safe.sure(done, function (_coll) {
				coll = _coll;
				done();
			}));
		});
		it("Create single field index", function (done) {
			coll.ensureIndex({ a: 1 }, safe.sure(done, function (indexName) {
				assert.equal(indexName, 'a_1');
				done();
			}));
		});
		it("Create compound index on 2 fields", function (done) {
			coll.ensureIndex({ a: 1, b: -1 }, safe.sure(done, function (indexName) {
				assert.equal(indexName, 'a_1_b_-1');
				done();
			}));
		});
		it("Create compound index on 3 fields", function (done) {
			coll.ensureIndex({ a: -1, b: 1, s: -1 }, safe.sure(done, function (indexName) {
				assert.equal(indexName, 'a_-1_b_1_s_-1');
				done();
			}));
		});
		it("Create another compound index on 2 fields", function (done) {
			coll.ensureIndex([ [ 'a', 'desc' ], ['s', 'desc' ] ], function (err, indexName) {
				assert.equal(err, null);
				assert.equal(indexName, 'a_-1_s_-1');
				done();
			});
		});
		it("Populate with test data", function (done) {
			docs = [
				{ a: 1, b: 1, s: 'Hello' },
				{ a: 1, b: 2, s: 'World' },
				{ a: 1, b: 3, s: 'this' },
				{ a: 2, b: 1, s: 'is' },
				{ a: 2, b: 2, s: 'test' },
				{ a: 3, b: 1, s: 'index' },
				{ a: 2, b: 1, s: 'cat' },
				{ a: 4, b: -1, s: 'dog' }
			];
			async.forEachSeries(docs, function (doc, cb) {
				coll.insert(doc, cb);
			}, done);
		});
		it("Sort by index", function (done) {
			coll.find().sort({ a: 1 }).toArray(safe.sure(done, function (docs) {
				var exp = docs.slice().sort(function (l, r) { return l.a - r.a; });
				eq(docs, exp);
				done();
			}));
		});
		it("Sort by 2-field index", function (done) {
			coll.find().sort([ [ 'a', 1 ], [ 'b', -1 ] ]).toArray(safe.sure(done, function (docs) {
				var exp = docs.slice().sort(function (l, r) {
					var d = l.a - r.a;
					if (d !== 0) return d;
					return r.b - l.b;
				});
				eq(docs, exp);
				done();
			}));
		});
		it("Sort by 3-field index", function (done) {
			coll.find().sort([ [ 'a', -1 ], [ 'b', 1 ], [ 's', -1 ] ]).toArray(safe.sure(done, function (docs) {
				var exp = docs.slice().sort(function (l, r) {
					var d = r.a - l.a;
					if (d !== 0) return d;
					d = l.b - r.b;
					if (d !== 0) return d;
					if (l.s < r.s) return 1;
					else if (l.s > r.s) return -1;
					else return 0;
				});
				eq(docs, exp);
				done();
			}));
		});
		it("Search and sort by index", function (done) {
			coll.find({ a: 1 }).sort([ [ 'a', 1 ], [ 'b', -1 ] ]).toArray(safe.sure(done, function (docs) {
				var exp = docs.filter(function (v) {
					return v.a == 1;
				}).sort(function (l, r) {
					var d = l.a - r.a;
					if (d !== 0) return d;
					return r.b - l.b;
				});
				eq(docs, exp);
				done();
			}));
		});
	});
});
