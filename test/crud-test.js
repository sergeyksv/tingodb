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
		var obj;
		it('create with upsert and $set apply $set to query', function (done) {
			obj = {j:3, c:"multi", a:[1,2,3,4,5]}
			coll.update({i:2}, {$set:obj}, {upsert:true}, safe.sure(done, function (n,r) {
				assert.equal(n,1);
				assert.equal(r.updatedExisting,false)
				assert(r.upserted)
				coll.findOne({i:2}, safe.sure(done, function (obj1) {
					assert.equal(obj1.i,2);
					done()
				}))
			}))
		})
		it('update array field is possible', function (done) {
			coll.update({i:2}, {$set:{a:[1,2]}},safe.sure(done, function (n,r) {
				assert.equal(n,1);
				assert.equal(r.updatedExisting,true)
				coll.findOne({i:2}, safe.sure(done, function (obj1) {
					assert.deepEqual([1,2],obj1.a);
					done()
				}))
			}))
		})
		it('upsert one more did not touch initial object', function (done) {
			obj = {j:4, i:3, c:"multi", a:[1,2,3,4,5]}
			var clone = _.cloneDeep(obj);
			coll.update({i:3}, obj, {upsert:true}, safe.sure(done, function (n,r) {
				assert.equal(n,1);
				assert.equal(r.updatedExisting,false)
				assert(r.upserted)
				coll.findOne({i:3}, safe.sure(done, function (obj1) {
					assert.deepEqual(obj,clone);
					clone._id = obj1._id;
					assert.deepEqual(obj1,clone);
					done()
				}))
			}))
		})
		it('modify multi changes only specific field for many documents', function (done) {
			coll.update({c:"multi"}, {$set:{a:[]}}, {multi:true}, safe.sure(done, function (n,r) {
				assert.equal(n,2);
				assert.equal(r.updatedExisting,true)
				coll.find({c:"multi"}).toArray(safe.sure(done, function (docs) {
					assert(docs[0].j,docs[1].j)
					_.each(docs, function (doc) {
						assert.deepEqual(doc.a,[])
					})
					done()
				}))
			}))
		})
		it('update with setting of _id field is not possible', function (done) {
			coll.update({c:"multi"}, {$set:{_id:"newId"}}, {multi:true}, function (err) {
				assert(err);
				done();
			})
		})
	})
	describe("insert", function () {
		it("works with String id", function (done) {
			coll.insert({_id:"some@email.goes.here.com",data:"some data"}, safe.sure(done, function () {
				coll.findOne({_id:"some@email.goes.here.com"}, safe.sure(done,function (obj) {
					assert(obj);
					done()
				}))
			}))
		})
		it("works with Date id", function (done) {
			var _id = new Date();
			coll.insert({_id:_id,data:"some data"}, safe.sure(done, function () {
				coll.findOne({_id:_id}, safe.sure(done,function (obj) {
					assert(obj);
					done()
				}))
			}))
		})
		it("works with Number id", function (done) {
			var _id = 1976;
			coll.insert({_id:_id,data:"some data"}, safe.sure(done, function () {
				coll.findOne({_id:_id}, safe.sure(done,function (obj) {
					assert(obj);
					done()
				}))
			}))
		})
	})
});
