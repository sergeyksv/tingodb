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
});
