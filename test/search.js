var vows = require('vows');
var assert = require('assert');
var _ = require('underscore');
var async = require('async');
var safe = require('safe');
var loremIpsum = require('lorem-ipsum');
var tutils = require("./utils");


var num = 1000;
var gt0sin = 0;
var _dt = null;

vows.describe('Basic').addBatch({
	'New store':{
		topic: function () {
			tutils.getDb('test', true, this.callback);
		},
		"ok":function (db) {
			assert.notEqual(db,null);
		},
		"collection":{
			topic:function (db) {
				var cb = this.callback;
				db.collection("test1", {}, safe.sure(cb,function (coll) {
					coll.ensureIndex({num:1}, {sparse:false,unique:false}, safe.sure(cb, function () {
						cb(null, coll);
					}))
				}))
			},
			"ok":function (coll) {
				assert.notEqual(coll,null);
			},			
			"with test data":{
				topic:function (coll) {
					var i=1;
					async.whilst(function () { return i<=num}, 
						function (cb) {
							var d = new Date();
							if (_dt==null)
								_dt=d;
							var obj = {_dt:d, num:i, pum:i, sub:{num:i}, sin:Math.sin(i),cos:Math.cos(i),t:15,junk:loremIpsum({count:1,units:"paragraphs"})};
							if (i%7==0) {
								delete obj.num;
								delete obj.pum;
							}
							coll.insert(obj, cb);
							if (obj.sin>0 && obj.sin<0.5)
							   gt0sin++;
							i++;
						},
						this.callback
					)
				},
				"ok":function() {
				},
				"has proper size":{
					topic:function (coll) {
						coll.count(this.callback);
					},
					"ok":function (err, size) {
						assert.equal(err, null);						
						assert.equal(size, num);
					}
				},
				"find {num:10} (index)":{
					topic:function (coll) {
						coll.find({num:10}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 1);						
						assert.equal(docs[0].num, 10);
					}
				},
				"find {num:{$not:{$ne:10}}} (index)":{
					topic:function (coll) {
						coll.find({num:{$not:{$ne:10}}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 1);						
						assert.equal(docs[0].num, 10);
					}
				},				
				"find {pum:10} (no index)":{
					topic:function (coll) {
						coll.find({pum:10}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 1);						
						assert.equal(docs[0].pum, 10);
					}
				},
				"find {pum:{eq:10}} (no index)":{
					topic:function (coll) {
						coll.find({num:10}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 1);						
						assert.equal(docs[0].pum, 10);
					}
				},				
				"find {num:{$lt:10}} (index)":{
					topic:function (coll) {
						coll.find({num:{$lt:10}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 8);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.num<10)
						})			
					}
				},
				"find {pum:{$lt:10}} (no index)":{
					topic:function (coll) {
						coll.find({pum:{$lt:10}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 8);						
						_.each(docs, function (doc) {
							assert.isTrue(doc.pum<10)
						})			
					}
				},	
				"find {num:{$lte:10}} (index)":{
					topic:function (coll) {
						coll.find({num:{$lte:10}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 9);						
						_.each(docs, function (doc) {
							assert.isTrue(doc.num<=10)
						})			
					}
				},
				"find {pum:{$lte:10}} (no index)":{
					topic:function (coll) {
						coll.find({pum:{$lte:10}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 9);						
						_.each(docs, function (doc) {
							assert.isTrue(doc.pum<=10)
						})			
					}
				},
				"find {num:{$gt:10}} (index)":{
					topic:function (coll) {
						coll.find({num:{$gt:10}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 849);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.num>10)
						})			
					}
				},
				"find {pum:{$gt:10}} (no index)":{
					topic:function (coll) {
						coll.find({pum:{$gt:10}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 849);						
						_.each(docs, function (doc) {
							assert.isTrue(doc.pum>10)
						})			
					}
				},					
				"find {num:{$gte:10}} (index)":{
					topic:function (coll) {
						coll.find({num:{$gte:10}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 850);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.num>=10)
						})			
					}
				},
				"find {pum:{$gte:10}} (no index)":{
					topic:function (coll) {
						coll.find({pum:{$gte:10}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 850);						
						_.each(docs, function (doc) {
							assert.isTrue(doc.pum>=10)
						})			
					}
				},		
				"find {num:{$ne:10}} (index)":{
					topic:function (coll) {
						coll.find({num:{$ne:10}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, num-1);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.num!=10)
						})			
					}
				},
				"find {num:{$not:{$eq:10}}} (index)":{
					topic:function (coll) {
						coll.find({num:{$ne:10}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, num-1);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.num!=10)
						})			
					}
				},				
				"find {pum:{$ne:10}} (no index)":{
					topic:function (coll) {
						coll.find({pum:{$ne:10}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, num-1);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.pum!=10)
						})			
					}
				},				
				"find {num:{$in:[10,20,30,40]}} (index)":{
					topic:function (coll) {
						coll.find({num:{$in:[10,20,30,40]}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 4);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.num%10==0)
						})			
					}
				},				
				"find {pum:{$in:[10,20,30,40]}} (no index)":{
					topic:function (coll) {
						coll.find({pum:{$in:[10,20,30,40]}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 4);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.num%10==0)
						})			
					}
				},
				"find {num:{$nin:[10,20,30,40]}} (index)":{
					topic:function (coll) {
						coll.find({num:{$nin:[10,20,30,40]}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, num-4);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.num!=10 && doc.num!=20 && doc.num!=30 && doc.num!=40)
						})			
					}
				},				
				"find {pum:{$nin:[10,20,30,40]}} (no index)":{
					topic:function (coll) {
						coll.find({pum:{$nin:[10,20,30,40]}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, num-4);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.pum!=10 && doc.pum!=20 && doc.pum!=30 && doc.pum!=40)
						})			
					}
				},
				"find {num:{$not:{$lt:10}}} (index)":{
					topic:function (coll) {
						coll.find({num:{$not:{$lt:10}}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.isTrue(docs.length==992||docs.length==850);	// Mongo BUG, 850 is wrong
						_.each(docs, function (doc) {
							assert.isTrue(_.isUndefined(doc.num) || doc.num>=10)							
						})			
					}
				},
				"find {pum:{$not:{$lt:10}}} (no index)":{
					topic:function (coll) {
						coll.find({pum:{$not:{$lt:10}}}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 992);			
						_.each(docs, function (doc) {
							assert.isTrue(_.isUndefined(doc.pum) || doc.pum>=10)
						})			
					}
				},			
				"find {num:{$lt:10},$or:[{num:5},{num:6},{num:11}]}":{
					topic:function (coll) {
						coll.find({num:{$lt:10},$or:[{num:5},{num:6},{num:11}]}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 2);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.num<10)
						})			
					}
				},
				"find {num:{$lt:10},$nor:[{num:5},{num:6},{num:7}":{
					topic:function (coll) {
						coll.find({num:{$lt:10},$nor:[{num:5},{num:6},{num:7}]}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 6);			
						_.each(docs, function (doc) {
							assert.isTrue(doc.num<10)
						})			
					}
				}				
			}
		}
	}
}).export(module);
