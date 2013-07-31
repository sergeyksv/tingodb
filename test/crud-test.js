var assert = require('assert');
var _ = require('lodash');
var async = require('async');
var safe = require('safe');
var tutils = require("./utils");

describe('CRUD', function () {
	var db,coll;
	before(function (done) {
		tutils.getDb('test', true, safe.sure(done, function (_db) {
			db = _db;
			done();
		}))
	})
	before(function (done) {
		db.collection("test", {}, safe.sure(done,function (_coll) {
			coll = _coll;
			coll.ensureIndex({i:1}, safe.sure(done, function () {
				done();
			}))
		}))
	})	
	describe('save', function () {
		var obj;
		it('create new', function (done) {
			obj = {i:1, j:1}
			coll.save(obj, done)
		})
		it('id is assigned', function () {
			assert(obj._id)
		})
		it('modify it', function (done) {
			obj.i++;
			coll.save(obj, safe.sure(done, function () {
				coll.findOne({_id:obj._id}, safe.sure(done, function (obj1) {
					assert.deepEqual(obj,obj1)
					done()
				}))
			}))
		})	
		it('delete it', function (done) {
			coll.remove({_id:obj._id}, safe.sure(done, function () {
				coll.findOne({_id:obj._id}, {sort:{i:1}}, safe.sure(done, function (obj1) {
					assert(!obj1)
					done()
				}))
			}))
		})	
	})
	describe('update', function () {
		it('create with upsert')
		it('modify it')
		it('insert one more')
		it('modify multi')
		it('modify did not touch _id')
	})
});
