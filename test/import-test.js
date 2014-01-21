var assert = require('assert');
var async = require('async');
var csv = require('csv');
var fs = require('fs');
var zlib = require('zlib');
var _ = require('lodash');
var safe = require('safe');
var tutils = require("./utils");

function load(file, iterator, callback) {
	var schema;
	var queue = async.queue(function (task, cb) {
		if (task.value) iterator(task.value, task.index, cb);
		else callback();
	}, 1);
	var gunzip = zlib.createGunzip();
	fs.createReadStream(file).pipe(gunzip);
	csv().from.stream(gunzip).on('record', function (row, index) {
		if (index === 0) schema = row;
		else {
			var value = { id: index };
			row.forEach(function (item, i) {
				value[schema[i]] = item;
			});
			queue.push({ value: value, index: index });
		}
	}).on('end', function (rowcount) {
		queue.push({});
	});
}

var rowcount = 500;

describe('Import', function () {
	describe('New store', function () {
		var db, coll;
		var sample = __dirname + '/sample-data/' + rowcount + '.csv.gz';
		before(function (done) {
			tutils.getDb('test', true, safe.sure(done, function (_db) {
				db = _db;
				done();
			}));
		});
		it("Create new collection", function (done) {
			db.collection('c' + rowcount, {}, safe.sure(done, function (_coll) {
				coll = _coll;
				done();
			}));
		});
		it("Populated with test data", function (done) {
			load(sample, function (value, index, callback) {
				coll.insert(value, callback);
			}, done);
		});
		it("Has right size", function (done) {
			coll.count(safe.sure(done, function (count) {
				assert.equal(count, rowcount);
				done();
			}));
		});
		it("test find $eq", function (done) {
			load(sample, function (value, index, callback) {
				if (Math.random() > 10 / rowcount) return process.nextTick(callback); // ~10 rows
				coll.find({ id: value.id }, function (err, docs) {
					if (err) return callback(err);
					docs.toArray(function (err, rows) {
						if (err) return callback(err);
						assert.equal(rows.length, 1);
						_.each(value, function (v, k) {
							assert.equal(v, rows[0][k]);
						});
						callback();
					});
				});
			}, done);
		});
	});
});
