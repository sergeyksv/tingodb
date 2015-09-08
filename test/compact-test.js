var assert = require('assert');
var fs = require('fs');
var safe = require('safe');
var tutils = require('./utils');
var _ = require('lodash');
var tingodb = require('../lib/main')({});

describe('(FS) Compact', function () {
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
	it('Create new collection', function (done) {
		db.collection('test', {}, safe.sure(done, function (_coll) {
			coll = _coll;
			done();
		}));
	});
	it('Add test data', function (done) {
		items = _.times(100, function (n) {
			return { k: n, v: _.random(100) };
		});
		length = items.length;
		coll.insert(items, { w: 1 }, done);
	});
	it('Update some items', function (done) {
		var docs = _.times(30, function () {
			var idx = _.random(items.length - 1);
			var doc = items[idx];
			doc.v = _.random(101, 200);
			return doc;
		});
		safe.forEachSeries(docs, function (doc, cb) {
			coll.update({ k: doc.k }, doc, { w: 1 }, cb);
		}, done);
	});
	it('Delete some items', function (done) {
		var count = 50;
		var keys = _.times(count, function () {
			for (;;) {
				var idx = _.random(items.length - 1);
				var doc = items[idx];
				if (!doc.x) {
					doc.x = true;
					return doc.k;
				}
			}
		});
		length -= count;
		coll.remove({ k: { $in: keys } }, { w: 1 }, done);
	});
	it('Update some items again', function (done) {
		var docs = _.times(30, function () {
			var idx = _.random(items.length - 1);
			var doc = items[idx];
			if (doc.x) {
				delete doc.x;
				length++;
			}
			doc.v = _.random(201, 300);
			return doc;
		});
		safe.forEachSeries(docs, function (doc, cb) {
			coll.update({ k: doc.k }, doc, { upsert: true, w: 1 }, cb);
		}, done);
	});
	it('Check count', checkCount);
	it('Check data', checkData);
	it('Close database', function (done) {
		db.close(done);
	});
	it('Remember collection size', function (done) {
		fs.stat(coll._filename, safe.sure(done, function (stats) {
			fsize = stats.size;
			done();
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
	it('Check count after reopening db', checkCount);
	it('Check data after reopening db', checkData);
	it('Check collection size', function (done) {
		fs.stat(coll._filename, safe.sure(done, function (stats) {
			assert(stats.size < fsize);
			done();
		}));
	});
});

describe('(FS) Hot Compact', function () {
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
	it('Create new collection', function (done) {
		db.collection('test', {}, safe.sure(done, function (_coll) {
			coll = _coll;
			done();
		}));
	});
	it('Add test data', function (done) {
		items = _.times(100, function (n) {
			return { k: n, v: _.random(100) };
		});
		length = items.length;
		coll.insert(items, { w: 1 }, done);
	});
	it('Update some items', function (done) {
		var docs = _.times(30, function () {
			var idx = _.random(items.length - 1);
			var doc = items[idx];
			doc.v = _.random(101, 200);
			return doc;
		});
		safe.forEachSeries(docs, function (doc, cb) {
			coll.update({ k: doc.k }, doc, { w: 1 }, cb);
		}, done);
	});
	it('Delete some items', function (done) {
		var count = 50;
		var keys = _.times(count, function () {
			for (;;) {
				var idx = _.random(items.length - 1);
				var doc = items[idx];
				if (!doc.x) {
					doc.x = true;
					return doc.k;
				}
			}
		});
		length -= count;
		coll.remove({ k: { $in: keys } }, { w: 1 }, done);
	});
	it('Update some items again', function (done) {
		var docs = _.times(30, function () {
			var idx = _.random(items.length - 1);
			var doc = items[idx];
			if (doc.x) {
				delete doc.x;
				length++;
			}
			doc.v = _.random(201, 300);
			return doc;
		});
		safe.forEachSeries(docs, function (doc, cb) {
			coll.update({ k: doc.k }, doc, { upsert: true, w: 1 }, cb);
		}, done);
	});
	it('Check count', checkCount);
	it('Check data', checkData);
	it('Remember collection size', function (done) {
		fs.stat(coll._filename, safe.sure(done, function (stats) {
			fsize = stats.size;
			done();
		}));
	});
	it('Compact database', function (done) {
		db.compactDatabase(done);
	});
	it('Check count after reopening db', checkCount);
	it('Check data after reopening db', checkData);
	it('Check collection size', function (done) {
		fs.stat(coll._filename, safe.sure(done, function (stats) {
			assert(stats.size < fsize);
			done();
		}));
	});
});

describe('(FS) Update+Hash', function () {
	var db, coll, fsize;
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
		coll.insert({ k: 1, v: 123 }, { w: 1 }, done);
	});
	it('Remember collection size', function (done) {
		fs.stat(coll._filename, safe.sure(done, function (stats) {
			fsize = stats.size;
			done();
		}));
	});
	it('Update data', function (done) {
		coll.update({ k: 1 }, { k: 1, v: 456 }, { w: 1 }, done);
	});
	it('Collection should grow', function (done) {
		fs.stat(coll._filename, safe.sure(done, function (stats) {
			assert(stats.size > fsize);
			fsize = stats.size;
			done();
		}));
	});
	it('Update with the same value', function (done) {
		coll.update({ k: 1 }, { k: 1, v: 456 }, { w: 1 }, done);
	});
	it('Collection should not change', function (done) {
		fs.stat(coll._filename, safe.sure(done, function (stats) {
			assert.equal(stats.size, fsize);
			done();
		}));
	});
	it('Update data again', function (done) {
		coll.update({ k: 1 }, { k: 1, v: 789 }, { upsert: true, w: 1 }, done);
	});
	it('Collection should grow again', function (done) {
		fs.stat(coll._filename, safe.sure(done, function (stats) {
			assert(stats.size > fsize);
			fsize = stats.size;
			done();
		}));
	});
	it('Ensure data is correct', function (done) {
		coll.find({ k: 1 }).toArray(safe.sure(done, function (docs) {
			assert.equal(docs.length, 1);
			assert.equal(docs[0].v, 789);
			done();
		}));
	});
});

describe('(FS) Store', function () {
	var db, coll, fsize;
	it('Operations must fail if db is linked to not existent path', function (done) {
		var Db = tingodb.Db;
		var db = new Db('/tmp/some_unexistant_path_667676qwe', {});
		var c = db.collection( 'test' );
		c.remove( {}, function (err) {
			assert(err);
			c.insert( {  name: 'Chiara',    surname: 'Mobily',     age: 22 } , function( err ){
				assert(err);
				done();
			});
		});
	});
});
