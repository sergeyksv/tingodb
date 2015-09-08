var assert = require('assert');
var _ = require('lodash');
var safe = require('safe');
var tutils = require("./utils");

function eq(docs1, docs2) {
	assert.equal(docs1.length, docs2.length);
	_.each(docs1,function (a, i) {
		var b = docs2[i];
		assert.equal(a.num, b.num);
		assert.equal(a.num2, b.num2);
		assert.equal(a.val, b.val);
	});
}

describe('Sort Test', function () {
	describe('New store', function () {
		var db, coll, asc, desc, gt, gtr;
		before(function (done) {
			tutils.getDb('test', true, safe.sure(done, function (_db) {
				db = _db;
				done();
			}));
		});
		it("Create new collection", function (done) {
			db.collection("test", {}, safe.sure(done, function (_coll) {
				coll = _coll;
				coll.ensureIndex({ num: 1 }, function () {
					coll.ensureIndex({ num2: 1 }, function () {
						coll.ensureIndex({ inul: 1 }, function () {
							done();
						});
					});
				});
			}));
		});
		it("Populated with test data", function (done) {
			this.timeout(10000);
			//var nums = [ 4, 8, 3, 2, 7, 78, 3, 7, 32, 67, 3, 6, 2, 67, 8, 3, 7, 3, 7, 3, 67, 7, 8, 3, 1, 8, 9 ];
			var nums = _.times(10000, function (n) {
				return _.random(100);
			});
			var docs = nums.map(function (num) {
				var doc = { num: num, num2: 100 - num, val: num, nul:num,inul:num };
				if (num%33==0) {
					delete doc.nul;
					delete doc.inul;
				}
				if (num%21==0) {
					doc.nul = doc.inul = null;
				}
				return doc;
			});
			asc = docs.slice().sort(function (a, b) {
				return a.num - b.num;
			});
			desc = asc.slice().reverse();
			gt = _.filter(asc, function (x) {
				return x.num > 25;
			});
			gtr = gt.slice().reverse();
			safe.forEachSeries(docs, function (doc, cb) {
				coll.insert(doc, cb);
			}, done);
		});
		it("Sort asc index", function (done) {
			coll.find().sort({ num: 1 }).toArray(safe.sure(done, function (docs) {
				eq(docs, asc);
				done();
			}));
		});
		it("Sort with absent/null fields no index", function (done) {
			this.timeout(10000);
			coll.find().sort({ nul: 1 }).toArray(safe.sure(done, function (docs) {
				var state = "nulls_start";
				var prev_value;
				_.each(docs, function (doc) {
					if (state=="nulls_start") {
						assert(!doc.nul);
						state="nulls";
					} else if (state=="nulls") {
						if (doc.nul) {
							state = "ordered";
							prev_value = doc.nul;
						}
					} else {
						assert(prev_value<=doc.nul)
						prev_value = doc.nul;
					}
				})
				done();
			}));
		});
		it("Sort with absent/null fields with index", function (done) {
			coll.find().sort({ inul: 1 }).toArray(safe.sure(done, function (docs) {
				var state = "nulls_start";
				var prev_value;
				_.each(docs, function (doc) {
					if (state=="nulls_start") {
						assert(!doc.inul);
						state="nulls";
					} else if (state=="nulls") {
						if (doc.inul) {
							state = "ordered";
							prev_value = doc.inul;
						}
					} else {
						assert(prev_value<=doc.inul)
						prev_value = doc.inul;
					}
				})
				done();
			}));
		});
		it("Sort asc index 2", function (done) {
			coll.find().sort({ num2: 1 }).toArray(safe.sure(done, function (docs) {
				eq(docs, desc);
				done();
			}));
		});
		it("Search index sort desc index 2 skip", function (done) {
			coll.find({ num: { $gt: 25 } }).sort({ num2: -1 }).skip(3000).toArray(safe.sure(done, function (docs) {
				eq(docs, gt.slice(3000));
				done();
			}));
		});
		it("Search index sort desc index 2 limit", function (done) {
			coll.find({ num: { $gt: 25 } }).sort({ num2: -1 }).limit(3000).toArray(safe.sure(done, function (docs) {
				eq(docs, gt.slice(0, 3000));
				done();
			}));
		});
		it("Search index 2 sort desc index 2", function (done) {
			coll.find({ num2: { $lt: 75 } }).sort({ num2: -1 }).skip(3000).toArray(safe.sure(done, function (docs) {
				eq(docs, gt.slice(3000));
				done();
			}));
		});
	});
});
