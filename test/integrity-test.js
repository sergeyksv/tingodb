var assert = require('assert');
var fs = require('fs');
var safe = require('safe');
var tutils = require('./utils');
var _ = require('lodash');
var tingodb = require('../lib/main')({});

describe('(FS) Corrupted DB Load', function () {
	var db, coll, items, length, fsize;
	function checkCount(done) {
		coll.find().count(safe.sure(done, function (count) {
			assert.equal(count, length);
			done();
		}));
	}
	function checkData(done) {
		safe.forEachSeries(items, function (item, cb) {
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
	it('Create new collection with data', function (done) {
		db.collection('test', {}, safe.sure(done, function (_coll) {
			coll = _coll;
			items = _.times(100, function (n) {
				return { k: n, v: _.random(100) };
			});
			length = items.length;
			coll.insert(items, { w: 1 }, done);
		}));
	});
	it('Check count', checkCount);
	it('Check data', checkData);
	it('Close database and trunkate collection file', function (done) {
		db.close(safe.sure(done, function () {
			fs.stat(coll._filename, safe.sure(done, function (stats) {
				fs.open(coll._filename, 'r+', safe.sure(done, function (fd) {
					items = items.slice(0,99);
					length--;
					fs.truncate(fd, stats.size-100, done);
				}));
			}));
		}));
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
	it('Check count', checkCount);
	it('Check data', checkData);
});
