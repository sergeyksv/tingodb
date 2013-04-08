#! /usr/bin/env node

var assert = require('assert');
var async = require('async');
var csv = require('csv');
var fs = require('fs');
var temp = require('temp');
var vows = require('vows');
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

function import_context(rowcount) {
	var sample = __dirname+'/sample-data/' + rowcount + '.csv.gz';
	return {
		topic: function (db) {
			var cb = this.callback;
			db.collection('c' + rowcount, {}, safe.sure(cb, function (coll) {
				coll.ensureIndex({id:1}, safe.sure(cb, function () {
					cb(null,coll);
				}))
			}))
		},
		'can be created': function (coll) {
			assert.notEqual(coll, null);
		},
		'populated with test data': {
			topic: function (coll) {
				load(sample, function (value, index, callback) {
					coll.insert(value, callback);
				}, this.callback);
			},
			'ok': function () {},
			'has proper size': {
				topic: function (coll) {
					coll.count(this.callback);
				},
				'ok': function (err, size) {
					assert.equal(size, rowcount);
				}
			},
			'test find $eq': {
				topic: function (coll) {
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
					}, this.callback);
				},
				'ok': function () {}
			}
		}
	};
}

vows.describe('Import').addBatch({
		'New store': {
			topic: function () {
				tutils.getDb('test', true, this.callback);
			},
			'can be created by path': function (db) {
				assert.notEqual(db, null);
			},
			'collection 500' : import_context(500)
//			'collection 50000' : import_context(50000)
		}
}).export(module);
