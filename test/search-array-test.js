var assert = require('assert');
var _ = require('lodash');
var safe = require('safe');
var tutils = require("./utils");

var num = 100;

describe('Search Array', function () {
	describe('New store', function () {
		var db, coll;
		before(function (done) {
			tutils.getDb('test', true, safe.sure(done, function (_db) {
				db = _db;
				done();
			}));
		});
		it("Create new collection", function (done) {
			db.collection("test1", {}, safe.sure(done, function (_coll) {
				coll = _coll;
				coll.ensureIndex({"arr.num":1}, {sparse:false,unique:false,_tiarr:true}, safe.sure(done, function (name) {
					coll.ensureIndex({"tags":1}, {sparse:false,unique:false,_tiarr:true}, safe.sure(done, function (name) {
						coll.ensureIndex({"nested.tags":1}, {sparse:false,unique:false,_tiarr:true}, safe.sure(done, function (name) {
							assert.ok(name);
							done();
						}));
					}));
				}));
			}));
		});
		it("Populated with test data", function (done) {
			var i=1;
			safe.whilst(function () { return i<=num; },
				function (cb) {
					var arr = [], arr2=[], j, obj;
					for (j=i; j<i+10; j++) {
						obj = {num:j,pum:j,sub:{num:j,pum:j}};
						if (i%7===0) {
							delete obj.num;
							delete obj.pum;
						}
						arr.push(obj);
						arr2.push(JSON.parse(JSON.stringify(obj)));
					}
					for (j=0; j<10; j++) {
						arr[j].sub.arr = arr2;
					}
					obj = {num:i, pum:i, arr:arr, nags:["tag"+i,"tag"+(i+1)], tags:["tag"+i,"tag"+(i+1)], nested:{tags:["tag"+i,"tag"+(i+1)]}};
					coll.insert(obj, cb);
					i++;
				},
				done
			);
		});
		it("has proper size", function (done) {
			coll.count(safe.sure(done, function (size) {
				assert.equal(size, num);
				done();
			}));
		});
		it("find {'arr.num':10} (index)", function (done) {
			coll.find({'arr.num':10},{"_tiar.arr.num":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 9);
				_.each(docs, function (doc) {
					var found = false;
					_.each(doc.arr, function (obj) {
						if (obj.num==10)
							found = true;
					});
					assert.ok(found);
				});
				done();
			}));
		});
		it("find {'arr.pum':10} (no index)", function (done) {
			coll.find({'arr.pum':10},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 9);
				_.each(docs, function (doc) {
					var found = false;
					_.each(doc.arr, function (obj) {
						if (obj.pum==10)
							found = true;
					});
					assert.ok(found);
				});
				done();
			}));
		});
		it("find {'arr.num':{$ne:10}} (index)", function (done) {
			coll.find({'arr.num':{$ne:10}},{"_tiar.arr.num":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 91);
				_.each(docs, function (doc) {
					var found = false;
					_.each(doc.arr, function (obj) {
						if (obj.num==10)
							found = true;
					});
					assert.ok(!found);
				});
				done();
			}));
		});
		it("find {'arr.pum':{$ne:10}} (no index)", function (done) {
			coll.find({'arr.pum':{$ne:10}},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 91);
				_.each(docs, function (doc) {
					var found = false;
					_.each(doc.arr, function (obj) {
						if (obj.pum==10)
							found = true;
					});
					assert.ok(!found);
				});
				done();
			}));
		});
		it("find {'arr.num':{$gt:10}} (index)", function (done) {
			coll.find({'arr.num':{$gt:10}},{"_tiar.arr.num":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 85);
				done();
			}));
		});
		it("find {'arr.pum':{$gt:10}} (no index)", function (done) {
			coll.find({'arr.pum':{$gt:10}},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 85);
				done();
			}));
		});
		it("find {'arr.num':{$gte:10}} (index)", function (done) {
			coll.find({'arr.num':{$gte:10}},{"_tiar.arr.num":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 86);
				done();
			}));
		});
		it("find {'arr.pum':{$gte:10}} (no index)", function (done) {
			coll.find({'arr.pum':{$gte:10}},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 86);
				done();
			}));
		});
		it("find {'arr.num':{$lt:10}} (index)", function (done) {
			coll.find({'arr.num':{$lt:10}},{"_tiar.arr.num":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 8);
				done();
			}));
		});
		it("find {'arr.pum':{$lt:10}} (no index)", function (done) {
			coll.find({'arr.pum':{$lt:10}},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 8);
				done();
			}));
		});
		it("find {'arr.num':{$lte:10}} (index)", function (done) {
			coll.find({'arr.num':{$lte:10}},{"_tiar.arr.num":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 9);
				done();
			}));
		});
		it("find {'arr.pum':{$lte:10}} (no index)", function (done) {
			coll.find({'arr.pum':{$lte:10}},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 9);
				done();
			}));
		});
		it("find {'arr.num':{$in:[10,20,30,40]}} (index)", function (done) {
			coll.find({'arr.num':{$in:[10,20,30,40]}},{"_tiar.arr.num":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 35);
				done();
			}));
		});
		it("find {'arr.pum':{$in:[10,20,30,40]}} (no index)", function (done) {
			coll.find({'arr.pum':{$in:[10,20,30,40]}},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 35);
				done();
			}));
		});
		it("find {'arr.num':{$nin:[10,20,30,40]}} (index)", function (done) {
			coll.find({'arr.num':{$nin:[10,20,30,40]}},{"_tiar.arr.num":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 65);
				done();
			}));
		});
		it("find {'arr.pum':{$nin:[10,20,30,40]}} (no index)", function (done) {
			coll.find({'arr.pum':{$nin:[10,20,30,40]}},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 65);
				done();
			}));
		});
		it("find {'arr.num':{$not:{$lt:10}}} (index)", function (done) {
			coll.find({'arr.num':{$not:{$lt:10}}},{"_tiar.arr.num":0}).toArray(safe.sure(done, function (docs) {
				assert.ok(docs.length==92||docs.length==78);	// Mongo BUG, 78 is wrong
				done();
			}));
		});
		it("find {'arr.pum':{$not:{$lt:10}}} (no index)", function (done) {
			coll.find({'arr.pum':{$not:{$lt:10}}},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 92);
				done();
			}));
		});
		it("find {'arr.num':{$lt:10},$or:[{'arr.pum':3},{'arr.pum':5},{'arr.pum':7}]}", function (done) {
			coll.find({'arr.num':{$lt:10},$or:[{'arr.pum':3},{'arr.pum':5},{'arr.pum':7}]},
				{"_tiar.arr.num":0,"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 6);
				done();
			}));
		});
		it("find {'arr.pum':{$lt:10},$or:[{'arr.num':3},{'arr.num':5},{'arr.num':7}]}", function (done) {
			coll.find({'arr.pum':{$lt:10},$or:[{'arr.num':3},{'arr.num':5},{'arr.num':7}]},
				{"_tiar.arr.num":0,"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 6);
				done();
			}));
		});
		it("find {'arr.num':{$lt:10},$nor:[{'arr.pum':3},{'arr.pum':5},{'arr.pum':7}]}", function (done) {
			coll.find({'arr.num':{$lt:10},$nor:[{'arr.pum':3},{'arr.pum':5},{'arr.pum':7}]},
				{"_tiar.arr.num":0,"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 2);
				done();
			}));
		});
		it("find {'arr.pum':{$lt:10},$nor:[{'arr.num':3},{'arr.num':5},{'arr.num':7}]}", function (done) {
			coll.find({'arr.pum':{$lt:10},$nor:[{'arr.num':3},{'arr.num':5},{'arr.num':7}]},
				{"_tiar.arr.num":0,"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 2);
				done();
			}));
		});
		it("find {'arr.num':{$all:[1,2,3,4,5,6,7,8,9,10]}} (index)", function (done) {
			coll.find({'arr.num':{$all:[1,2,3,4,5,6,7,8,9,10]}},{"_tiar.arr.num":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 1);
				done();
			}));
		});
		it("find {'arr.pum':{$all:[1,2,3,4,5,6,7,8,9,10]}} (no index)", function (done) {
			coll.find({'arr.pum':{$all:[1,2,3,4,5,6,7,8,9,10]}},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 1);
				done();
			}));
		});
		it("find {'arr.pum':{$exists:false}} (no index)", function (done) {
			coll.find({'arr.pum':{$exists:false}},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 14);
				done();
			}));
		});
		it("find {'arr.num':{$exists:false}} (index)", function (done) {
			coll.find({'arr.num':{$exists:false}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 14);
				done();
			}));
		});
		it("find {'arr.pum':{$exists:true}} (no index)", function (done) {
			coll.find({'arr.pum':{$exists:true}},{"_tiar.arr.pum":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 86);
				done();
			}));
		});
		it("find {'arr.num':{$exists:true}} (index)", function (done) {
			coll.find({'arr.num':{$exists:true}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 86);
				done();
			}));
		});
		it("find flat array {'tags':'tag2'} (no index)", function (done) {
			coll.find({'tags':'tag2'},{"_tiar.tags":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 2);
				done();
			}));
		});
		it("find nested flat array {'nested.tags':'tag2'} (no index)", function (done) {
			coll.find({'nested.tags':'tag2'},{"_tiar.nested.tags":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 2);
				done();
			}));
		});
		it("find flat array {'tags':'tag2'} (index)", function (done) {
			coll.find({'tags':'tag2'}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 2);
				done();
			}));
		});
		it("find flat array {'tags':{'$regex':'tag1.'}} (index)", function (done) {
			coll.find({'tags':{'$regex':'tag1.'}},{"_tiar.tags":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 13);
				done();
			}));
		});
		it("find flat array {'nags':{'$regex':'tag'}} (no index)", function (done) {
			coll.find({'nags':{'$regex':'tag1.'}},{"_tiar.nags":0}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 13);
				done();
			}));
		});
		it("find nested flat array {'nested.tags':'tag2'} (index)", function (done) {
			coll.find({'nested.tags':'tag2'}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 2);
				done();
			}));
		});
	});
});
