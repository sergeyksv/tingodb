var assert = require('assert');
var _ = require('lodash');
var async = require('async');
var safe = require('safe');
var loremIpsum = require('lorem-ipsum');
var tutils = require("./utils");


var num = 1000;
var gt0sin = 0;
var _dt = null;

describe('Search', function () {
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
				coll.ensureIndex({num:1}, {sparse:false,unique:false}, safe.sure(done, function (name) {
					assert.ok(name);
					done();
				}));
			}));
		});
		it("Populated with test data", function (done) {
			var i=1;
			async.whilst(function () { return i<=num; }, 
				function (cb) {
					var d = new Date();
					if (_dt === null)
						_dt=d;
					var obj = { _dt: d, anum: [i,i+1,i+2], apum: [i,i+1,i+2], num: i, pum: i,
						sub: { num: i }, sin: Math.sin(i), cos: Math.cos(i), t: 15,
						junk: loremIpsum({ count: 1, units: "paragraphs" }) };
					if (i % 7 === 0) {
						delete obj.num;
						delete obj.pum;
					}
					coll.insert(obj, cb);
					if (obj.sin>0 && obj.sin<0.5)
						gt0sin++;
					i++;
				},
				safe.sure(done, done)
			);
		});
		it("Has right size", function (done) {
			coll.count(safe.sure(done, function (count) {
				safe.trap(done, function () {
					assert.equal(count, num);
					done();
				})();
			}));
		});
		it("find {num:10} (index)", function (done) {
			coll.find({num:10}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 1);						
				assert.equal(docs[0].num, 10);
				done();
			}));
		});
		it("find {num:{$not:{$ne:10}}} (index)", function (done) {
			coll.find({num:{$not:{$ne:10}}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 1);
				assert.equal(docs[0].num, 10);
				done();
			}));
		});
		it("find {pum:10} (no index)", function (done) {
			coll.find({pum:10}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 1);						
				assert.equal(docs[0].pum, 10);
				done();
			}));
		});
		it("find {pum:{eq:10}} (no index)", function (done) {
			coll.find({num:10}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 1);						
				assert.equal(docs[0].pum, 10);
				done();
			}));
		});
		it("find {num:{$lt:10}} (index)", function (done) {
			coll.find({num:{$lt:10}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 8);			
				_.each(docs, function (doc) {
					assert.ok(doc.num<10);
				});
				done();
			}));
		});
		it("find {pum:{$lt:10}} (no index)", function (done) {
			coll.find({pum:{$lt:10}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 8);						
				_.each(docs, function (doc) {
					assert.ok(doc.pum<10);
				});
				done();
			}));
		});
		it("find {num:{$lte:10}} (index)", function (done) {
			coll.find({num:{$lte:10}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 9);						
				_.each(docs, function (doc) {
					assert.ok(doc.num<=10);
				});
				done();
			}));
		});
		it("find {pum:{$lte:10}} (no index)", function (done) {
			coll.find({pum:{$lte:10}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 9);						
				_.each(docs, function (doc) {
					assert.ok(doc.pum<=10);
				});
				done();
			}));
		});
		it("find {num:{$gt:10}} (index)", function (done) {
			coll.find({num:{$gt:10}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 849);			
				_.each(docs, function (doc) {
					assert.ok(doc.num>10);
				});
				done();
			}));
		});
		it("find {pum:{$gt:10}} (no index)", function (done) {
			coll.find({pum:{$gt:10}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 849);						
				_.each(docs, function (doc) {
					assert.ok(doc.pum>10);
				});
				done();
			}));
		});
		it("find {num:{$gte:10}} (index)", function (done) {
			coll.find({num:{$gte:10}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 850);			
				_.each(docs, function (doc) {
					assert.ok(doc.num>=10);
				});
				done();
			}));
		});
		it("find {pum:{$gte:10}} (no index)", function (done) {
			coll.find({pum:{$gte:10}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 850);						
				_.each(docs, function (doc) {
					assert.ok(doc.pum>=10);
				});
				done();
			}));
		});
		it("find {num:{$ne:10}} (index)", function (done) {
			coll.find({num:{$ne:10}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, num-1);			
				_.each(docs, function (doc) {
					assert.ok(doc.num!=10);
				});
				done();
			}));
		});
		it("find {num:{$not:{$eq:10}}} (index)", function (done) {
			coll.find({num:{$ne:10}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, num-1);			
				_.each(docs, function (doc) {
					assert.ok(doc.num!=10);
				});
				done();
			}));
		});
		it("find {pum:{$ne:10}} (no index)", function (done) {
			coll.find({pum:{$ne:10}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, num-1);			
				_.each(docs, function (doc) {
					assert.ok(doc.pum!=10);
				});
				done();
			}));
		});
		it("find {num:{$in:[10,20,30,40]}} (index)", function (done) {
			coll.find({num:{$in:[10,20,30,40]}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 4);			
				_.each(docs, function (doc) {
					assert.ok(doc.num%10===0);
				});
				done();
			}));
		});
		it("find {pum:{$in:[10,20,30,40]}} (no index)", function (done) {
			coll.find({pum:{$in:[10,20,30,40]}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 4);			
				_.each(docs, function (doc) {
					assert.ok(doc.num%10===0);
				});
				done();
			}));
		});
		it("find {num:{$nin:[10,20,30,40]}} (index)", function (done) {
			coll.find({num:{$nin:[10,20,30,40]}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, num-4);			
				_.each(docs, function (doc) {
					assert.ok(doc.num!=10 && doc.num!=20 && doc.num!=30 && doc.num!=40);
				});
				done();
			}));
		});
		it("find {pum:{$nin:[10,20,30,40]}} (no index)", function (done) {
			coll.find({pum:{$nin:[10,20,30,40]}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, num-4);			
				_.each(docs, function (doc) {
					assert.ok(doc.pum!=10 && doc.pum!=20 && doc.pum!=30 && doc.pum!=40);
				});
				done();
			}));
		});
		it("find {num:{$not:{$lt:10}}} (index)", function (done) {
			coll.find({num:{$not:{$lt:10}}}).toArray(safe.sure(done, function (docs) {
				assert.ok(docs.length==992||docs.length==850);	// Mongo BUG, 850 is wrong
				_.each(docs, function (doc) {
					assert.ok(_.isUndefined(doc.num) || doc.num>=10);
				});
				done();
			}));
		});
		it("find {pum:{$not:{$lt:10}}} (no index)", function (done) {
			coll.find({pum:{$not:{$lt:10}}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 992);			
				_.each(docs, function (doc) {
					assert.ok(_.isUndefined(doc.pum) || doc.pum>=10);
				});
				done();
			}));
		});
		it("find {num:{$lt:10},$or:[{num:5},{num:6},{num:11}]}", function (done) {
			coll.find({num:{$lt:10},$or:[{num:5},{num:6},{num:11}]}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 2);			
				_.each(docs, function (doc) {
					assert.ok(doc.num<10);
				});
				done();
			}));
		});
		it("find {num:{$lt:10},$nor:[{num:5},{num:6},{num:7}", function (done) {
			coll.find({num:{$lt:10},$nor:[{num:5},{num:6},{num:7}]}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 6);			
				_.each(docs, function (doc) {
					assert.ok(doc.num<10);
				});
				done();
			}));
		});
		it("find {'anum':{$all:[1,2,3]}} (index)", function (done) {
			coll.find({'anum':{$all:[1,2,3]}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 1);
				done();
			}));
		});
		it("find {'apum':{$all:[1,2,3]}} (no index)", function (done) {
			coll.find({'apum':{$all:[1,2,3]}}).toArray(safe.sure(done, function (docs) {
				assert.equal(docs.length, 1);
				done();
			}));
		});
	});
});
