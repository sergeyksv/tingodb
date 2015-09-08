var assert = require('assert');
var safe = require('safe');
var tutils = require("./utils");

describe('Delete', function () {
	var db, coll, items, length;
	function checkCount(done) {
		coll.find().count(safe.sure(done, function (count) {
			assert.equal(count, length);
			done();
		}));
	}
	function checkData(done) {
		safe.forEach(items, function (item, cb) {
			coll.findOne({ k: item.k }, safe.sure(cb, function (doc) {
				if (item.x) {
					assert.equal(doc, null);
				} else {
					assert.equal(doc.k, item.k);
					assert.equal(doc.v, item.v);
				}
				cb();
			}));
		}, done);
	}
	it('Open database', function (done) {
		tutils.getDb('test', true, safe.sure(done, function (_db) {
			db = _db;
			done();
		}));
	});
	it('Create new collection', function (done) {
		db.collection('test', {}, safe.sure(done, function (_coll) {
			coll = _coll;
			done();
		}));
	});
	it('Add test data', function (done) {
		items = [ { k: 1, v: 123 }, { k: 2, v: 456 }, { k: 3, v: 789 }, { k: 4, v: 111 } ];
		length = items.length;
		coll.insert(items, { w: 1 }, done);
	});
	it('Check count', checkCount);
	it('Check data', checkData);
	it('Delete items', function (done) {
		items[1].x = true;
		items[3].x = true;
		var keys = items.filter(function (x) {
			return x.x;
		}).map(function (x) {
			return x.k;
		});
		length -= keys.length;
		coll.remove({ k: { $in: keys } }, { w: 1 }, done);
	});
	it('Check count after remove', checkCount);
	it('Check data after remove', checkData);
	it('Close database', function (done) {
		db.close(done);
	});
	it('Reopen database', function (done) {
		tutils.getDb('test', false, safe.sure(done, function (_db) {
			db = _db;
			done();
		}));
	});
	it('Get test collection', function (done) {
		db.collection('test', {}, safe.sure(done, function (_coll) {
			coll = _coll;
			done();
		}));
	});
	it('Check count after reopening db', checkCount);
	it('Check data after reopening db', checkData);
});
