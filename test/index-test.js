var _ = require('lodash');
var assert = require('assert');
var safe = require('safe');
var tutils = require("./utils");

var dataset = [
	{ a: 1, b: 1, s: 'Hello', n: 0 },
	{ a: 1, b: 2, s: 'World', n: 1 },
	{ a: 1, b: 3, s: 'this', n: 2 },
	{ a: 2, b: 1, s: 'is', n: 3 },
	{ a: 2, b: 2, s: 'test', n: 4 },
	{ a: 3, b: 1, s: 'index', n: 5 },
	{ a: 2, b: 1, s: 'cat', n: 6 },
	{ a: 4, b: -1, s: 'dog', n: 7 }
];

var indexes = [
	{
		name: 'single field',
		value: { a: 1 }
	},
	{
		name: 'another single field',
		value: { b: -1 }
	},
	{
		name: '2-field',
		value: [ [ 'a', 1 ], [ 'b', -1 ] ]
	},
/*	{
		name: 'another 2-field',
		value: [ { 's': 'desc' }, { 'a': 'desc' } ]
	}, */
	{
		name: '3-field',
		value: [ [ 'a', -1 ], [ 'b', 1 ], [ 's', -1 ] ]
	},
	{
		name: '2-field n',
		value: [ [ 'b', 1 ], [ 'n', 1 ] ]
	},
];

var queries = [
	{
		name: 'empty',
		value: { }
	},
	{
		name: 'by index single field',
		value: { a: 1 }
	},
	{
		name: 'by index another single field',
		value: { b: 1 }
	},
	{
		name: 'by index another single field $lt',
		value: { b: { $lt: 2 } }
	},
	{
		name: 'by index another single field $range',
		value: { b: { $lt: 2, $gt: 0 } }
	},
	{
		name: 'no index single field',
		value: { n: 3 }
	},
	{
		name: 'by index 2-field',
		value: { a: 2, b: 1 }
	},
	{
		name: 'by index another 2-field',
		value: { a: 2, s: 'test' }
	},
	{
		name: 'by index 2-field $gt',
		value: { a: 1, b: { $gt: 1 } }
	},
	{
		name: 'partially by index 2-field',
		value: { a: 2, n: 4 }
	},
	{
		name: 'partially by index another 2-field',
		value: { s: 'index', b: 1 }
	},
	{
		name: 'by index 3-field',
		value: { a: 2, b: 1, s: 'is' }
	},
	{
		name: '4-field',
		value: { a: 2, b: 1, s: 'cat', n: 6 }
	},
	{
		name: '2-field-no-result',
		value: { a: 2, b: 7 }
	},
	{
		name: 'by index but! only if selected the right one',
		value: { a: 2, b: 1, n: 6 }
	},
	{
		name: 'by index 2-field $in',
		value: { a: { $in: [ 1, 2, 3, 2 ] }, b: 2 }
	},
	{
		name: 'by index 2-field $nin',
		value: { a: { $nin: [ 2, 3, 2 ] }, b: 2 }
	},
];

var sorting = [
	{
		name: 'empty',
		value: [ ]
	},
	{
		name: 'by index',
		value: [ [ 'a', 1 ] ]
	},
	{
		name: 'by another index',
		value: [ [ 'b', -1 ] ]
	},
	{
		name: 'by 2-field index',
		value: [ [ 'a', 1 ], [ 'b', -1 ] ]
	},
	{
		name: 'by 3-field index',
		value: [ [ 'a', -1 ], [ 'b', 1 ], [ 's', -1 ] ]
	},
	{
		name: 'by index and another field',
		value: [ [ 'a', 1 ], [ 'n', -1 ] ]
	},
	{
		name: 'by 2-field index and another field',
		value: [ [ 'a', 1 ], [ 'b', -1 ], [ 'n', -1 ] ]
	}
];

function check(query, sort, docs) {
	// apply query to dataset
	if (!_.isArray(query)) query = [ query ];
	var sample = _.uniq(_.flatten(_.map(query, function (keys) {
		return _.filter(dataset, function (doc) {
			return _.every(keys, function (v, k) {
				var vv = doc[k];
				if (!_.isObject(v)) return vv === v;
				return _.every(v, function (ov, ok) {
					if (ok == '$gt') return vv > ov;
					else if (ok == '$gte') return vv >= ov;
					else if (ok == '$lt') return vv < ov;
					else if (ok == '$lte') return vv <= ov;
					else if (ok == '$in') return _.includes(ov, vv);
					else if (ok == '$nin') return !_.includes(ov, vv);
					else return false;
				});
			});
		});
	})));
	// check count
	assert.equal(sample.length, docs.length);
	// check order
	for (var i = 0; i < docs.length - 1; i++) {
		var doc1 = docs[i];
		var doc2 = docs[i + 1];
		for (var j in sort) {
			var o = sort[j];
			var k = o[0];
			var d = o[1];
			var v1 = doc1[k];
			var v2 = doc2[k];
			if (v1 < v2) {
				assert.equal(d, 1);
				break;
			} else if (v1 > v2) {
				assert.equal(d, -1);
				break;
			}
		}
	}
}

describe('Index Test', function () {
	var db, coll;
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
	_.each(indexes, function (index) {
		it("Create " + index.name + " index", function (done) {
			coll.ensureIndex(index.value, safe.sure(done, function (indexName) {
				done();
			}));
		});
	});
	it("Populate with test data", function (done) {
		safe.forEachSeries(dataset, function (doc, cb) {
			coll.insert(_.clone(doc), cb);
		}, done);
	});
	_.each(queries, function (query) {
		_.each(sorting, function (sort) {
			var name = "Search " + query.name + " query, sort " + sort.name;
			it(name, function (done) {
				coll.find(query.value).sort(sort.value).toArray(safe.sure(done, function (docs) {
					check(query.value, sort.value, docs);
					done();
				}));
			});
			var rsort = _.map(sort.value, function (v) { return [ v[0], -v[1] ]; });
			it(name + ", reverse", function (done) {
				coll.find(query.value).sort(rsort).toArray(safe.sure(done, function (docs) {
					check(query.value, rsort, docs);
					done();
				}));
			});
		});
	});
});
