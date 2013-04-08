var vows = require('vows');
var assert = require('assert');
var _ = require('underscore');
var async = require('async');
var safe = require('safe');
var tutils = require("./utils");

var num = 100;

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
					coll.ensureIndex({"arr.num":1}, {sparse:false,unique:false,_tiarr:true}, safe.sure(cb, function () {
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
							var arr = [],arr2=[];
							for (var j=i; j<i+10; j++) {
								var obj = {num:j,pum:j,sub:{num:j,pum:j}};
								if (i%7==0) {
									delete obj.num;
									delete obj.pum;
								}
								arr.push(obj)
								arr2.push(JSON.parse(JSON.stringify(obj)))
							}
							for (var j=0; j<10; j++) {
								arr[j].sub.arr = arr2;
							}							
							var obj = {num:i, pum:i, arr:arr};
							coll.insert(obj, cb);
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
				"find {'arr.num':10} (index)":{
					topic:function (coll) {
						coll.find({'arr.num':10},{"_tiar.arr.num":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 9);						
						_.each(docs, function (doc) {
							var found = false;
							_.each(doc.arr, function (obj) {
								if (obj.num==10)
									found = true;
							})
							assert.isTrue(found);
						})
					}
				},							
				"find {'arr.pum':10} (no index)":{
					topic:function (coll) {
						coll.find({'arr.pum':10},{"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 9);						
						_.each(docs, function (doc) {
							var found = false;
							_.each(doc.arr, function (obj) {
								if (obj.pum==10)
									found = true;
							})
							assert.isTrue(found);
						})
					}
				},
				"find {'arr.num':{$ne:10}} (index)":{
					topic:function (coll) {
						coll.find({'arr.num':{$ne:10}},{"_tiar.arr.num":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 91);						
						_.each(docs, function (doc) {
							var found = false;
							_.each(doc.arr, function (obj) {
								if (obj.num==10)
									found = true;
							})
							assert.isTrue(!found);
						})
					}
				},							
				"find {'arr.pum':{$ne:10}} (no index)":{
					topic:function (coll) {
						coll.find({'arr.pum':{$ne:10}},{"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 91);						
						_.each(docs, function (doc) {
							var found = false;
							_.each(doc.arr, function (obj) {
								if (obj.pum==10)
									found = true;
							})
							assert.isTrue(!found);
						})
					}
				},
				"find {'arr.num':{$gt:10}} (index)":{
					topic:function (coll) {
						coll.find({'arr.num':{$gt:10}},{"_tiar.arr.num":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 85);						
					}
				},							
				"find {'arr.pum':{$gt:10}} (no index)":{
					topic:function (coll) {
						coll.find({'arr.pum':{$gt:10}},{"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 85);						
					}
				},				
				"find {'arr.num':{$gte:10}} (index)":{
					topic:function (coll) {
						coll.find({'arr.num':{$gte:10}},{"_tiar.arr.num":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 86);						
					}
				},							
				"find {'arr.pum':{$gte:10}} (no index)":{
					topic:function (coll) {
						coll.find({'arr.pum':{$gte:10}},{"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 86);						
					}
				},						
				"find {'arr.num':{$lt:10}} (index)":{
					topic:function (coll) {
						coll.find({'arr.num':{$lt:10}},{"_tiar.arr.num":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 8);						
					}
				},							
				"find {'arr.pum':{$lt:10}} (no index)":{
					topic:function (coll) {
						coll.find({'arr.pum':{$lt:10}},{"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 8);						
					}
				},						
				"find {'arr.num':{$lte:10}} (index)":{
					topic:function (coll) {
						coll.find({'arr.num':{$lte:10}},{"_tiar.arr.num":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 9);						
					}
				},							
				"find {'arr.pum':{$lte:10}} (no index)":{
					topic:function (coll) {
						coll.find({'arr.pum':{$lte:10}},{"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 9);						
					}
				},								
				"find {'arr.num':{$in:[10,20,30,40]}} (index)":{
					topic:function (coll) {
						coll.find({'arr.num':{$in:[10,20,30,40]}},{"_tiar.arr.num":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 35);						
					}
				},							
				"find {'arr.pum':{$in:[10,20,30,40]}} (no index)":{
					topic:function (coll) {
						coll.find({'arr.pum':{$in:[10,20,30,40]}},{"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 35);						
					}
				},
				"find {'arr.num':{$nin:[10,20,30,40]}} (index)":{
					topic:function (coll) {
						coll.find({'arr.num':{$nin:[10,20,30,40]}},{"_tiar.arr.num":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 65);						
					}
				},							
				"find {'arr.pum':{$nin:[10,20,30,40]}} (no index)":{
					topic:function (coll) {
						coll.find({'arr.pum':{$nin:[10,20,30,40]}},{"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 65);						
					}
				},						
				"find {'arr.num':{$not:{$lt:10}}} (index)":{
					topic:function (coll) {
						coll.find({'arr.num':{$not:{$lt:10}}},{"_tiar.arr.num":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.isTrue(docs.length==92||docs.length==78);	// Mongo BUG, 78 is wrong				
					}
				},							
				"find {'arr.pum':{$not:{$lt:10}}} (no index)":{
					topic:function (coll) {
						coll.find({'arr.pum':{$not:{$lt:10}}},{"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 92);						
					}
				},					
				"find {'arr.num':{$lt:10},$or:[{'arr.pum':3},{'arr.pum':5},{'arr.pum':7}]}":{
					topic:function (coll) {
						coll.find({'arr.num':{$lt:10},$or:[{'arr.pum':3},{'arr.pum':5},{'arr.pum':7}]},{"_tiar.arr.num":0,"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 6);						
					}
				},
				"find {'arr.pum':{$lt:10},$or:[{'arr.num':3},{'arr.num':5},{'arr.num':7}]}":{
					topic:function (coll) {
						coll.find({'arr.pum':{$lt:10},$or:[{'arr.num':3},{'arr.num':5},{'arr.num':7}]},{"_tiar.arr.num":0,"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 6);						
					}
				},
				"find {'arr.num':{$lt:10},$nor:[{'arr.pum':3},{'arr.pum':5},{'arr.pum':7}]}":{
					topic:function (coll) {
						coll.find({'arr.num':{$lt:10},$nor:[{'arr.pum':3},{'arr.pum':5},{'arr.pum':7}]},{"_tiar.arr.num":0,"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 2);						
					}
				},
				"find {'arr.pum':{$lt:10},$nor:[{'arr.num':3},{'arr.num':5},{'arr.num':7}]}":{
					topic:function (coll) {
						coll.find({'arr.pum':{$lt:10},$nor:[{'arr.num':3},{'arr.num':5},{'arr.num':7}]},{"_tiar.arr.num":0,"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 2);						
					}
				},
				"find {'arr.num':{$all:[1,2,3,4,5,6,7,8,9,10]}} (index)":{
					topic:function (coll) {
						coll.find({'arr.num':{$all:[1,2,3,4,5,6,7,8,9,10]}},{"_tiar.arr.num":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 1);						
					}
				},				
				"find {'arr.pum':{$all:[1,2,3,4,5,6,7,8,9,10]}} (no index)":{
					topic:function (coll) {
						coll.find({'arr.pum':{$all:[1,2,3,4,5,6,7,8,9,10]}},{"_tiar.arr.pum":0}).toArray(this.callback)
					},
					"ok":function (err, docs) {
						assert.equal(err, null);
						assert.equal(docs.length, 1);						
					}
				}					
			}
		}
	}
}).export(module);
