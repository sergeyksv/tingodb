var assert = require('assert');
var _ = require('lodash');
var safe = require('safe');
var tutils = require("./utils");

describe('Incremental update', function () {
	var db,coll;
	before(function (done) {
		tutils.getDb('test', true, safe.sure(done, function (_db) {
			db = _db;
			done();
		}))
	})
	describe("$rename", function () {
		it("#1 $rename basics",function (done) {
			db.collection("rename", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:1,tcores:1,nmame:"john"}, safe.sure(done, function () {
					_coll.update({_id:1},{$rename:{tcores:"scores",nmame:"name"}}, safe.sure(done, function () {
						_coll.findOne({_id:1},safe.sure(done, function (obj) {
							assert.deepEqual(obj,{_id:1,scores:1,name:"john"});
							done();
						}))
					}))
				}))
			}))
		})
		it("#1 $rename move",function (done) {
			db.collection("rename", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:2,user:{name:"john"}}, safe.sure(done, function () {
					_coll.update({_id:2},{$rename:{"user.name":"contact.fname"}}, safe.sure(done, function () {
						_coll.findOne({_id:2},safe.sure(done, function (obj) {
							assert.deepEqual(obj,{_id:2,user:{},contact:{fname:"john"}});
							done();
						}))
					}))
				}))
			}))
		})
	})
	describe("$inc", function () {
		it("#1 $inc basics",function (done) {
			db.collection("inc", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:1,i1:1,i2:1,sub:{i1:5.35}}, safe.sure(done, function () {
					_coll.update({_id:1},{$inc:{i1:5,i2:-6,i3:1,"sub.i1":1}}, safe.sure(done, function () {
						_coll.findOne({_id:1},safe.sure(done, function (obj) {
							assert.deepEqual(obj,{_id:1,i1:6,i2:-5,i3:1,sub:{i1:6.35}});
							done();
						}))
					}))
				}))
			}))
		})
		it("#2 $inc on non number throws error",function (done) {
			db.collection("inc", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:2,i1:"help"}, safe.sure(done, function () {
					_coll.update({_id:2},{$inc:{i1:5}}, function (err) {
						assert.equal(err.message,"Cannot apply $inc modifier to non-number");
						done();
					})
				}))
			}))
		})
	})
	describe("$set", function () {
		it("#1 $set basics",function (done) {
			db.collection("set", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:1,name:"John",sub:{gender:"male"}}, safe.sure(done, function () {
					_coll.update({_id:1},{$set:{name:"Rosa","sub.gender":"female","sub.age":22}}, safe.sure(done, function () {
						_coll.findOne({_id:1},safe.sure(done, function (obj) {
							assert.deepEqual(obj,{"_id":1,"name":"Rosa","sub":{"age":22,"gender":"female"}});
							done();
						}))
					}))
				}))
			}))
		})
		it("#2 $set with subobj",function (done) {
			db.collection("set", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:2,name:"John",sub:{gender:"male"}}, safe.sure(done, function () {
					_coll.update({_id:2},{$set:{name:"Rosa",sub:{gender:"female","age":22}}}, safe.sure(done, function () {
						_coll.findOne({_id:2},safe.sure(done, function (obj) {
							assert.deepEqual(obj,{"_id":2,"name":"Rosa","sub":{"age":22,"gender":"female"}});
							done();
						}))
					}))
				}))
			}))
		})
	})
	describe("$setOnInsert", function () {
		it("#1 $setOnInsert on update",function (done) {
			db.collection("setOnInsert", {}, safe.sure(done,function (_coll) {
				_coll.update({_id:1},{$setOnInsert:{name:"John",sub:{gender:"male"}}},{upsert:true}, safe.sure(done, function () {
					_coll.findOne({_id:1},safe.sure(done, function (obj) {
						assert.deepEqual(obj,{"_id":1,"name":"John","sub":{"gender":"male"}});
						done();
					}))
				}))
			}))
		})
		it("#1 $setOnInsert on findAndUpdate",function (done) {
			db.collection("setOnInsert", {}, safe.sure(done,function (_coll) {
				_coll.findAndModify({_id:2},{},{$setOnInsert:{name:"John",sub:{gender:"male"}}},{upsert:true,new:true}, safe.sure(done, function (obj) {
					assert.deepEqual(obj,{"_id":2,"name":"John","sub":{"gender":"male"}});
					done();
				}))
			}))
		})
	})
	describe("$unset", function () {
		it("#1 $unset basics",function (done) {
			db.collection("unset", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:1,name:"John",lname:"Peter",sub:{gender:"male",age:34}}, safe.sure(done, function () {
					_coll.update({_id:1},{$unset:{lname:1,"sub.age":1}}, safe.sure(done, function () {
						_coll.findOne({_id:1},safe.sure(done, function (obj) {
							assert.deepEqual(obj,{"_id":1,"name":"John","sub":{"gender":"male"}});
							done();
						}))
					}))
				}))
			}))
		})
		it("#2 $unset on non existent field does nothing",function (done) {
			db.collection("unset", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:2,scores:25}, safe.sure(done, function () {
					_coll.update({_id:2},{$unset:{"jmores.fores":1,"pores":1}}, safe.sure(done, function () {
						_coll.findOne({_id:2},safe.sure(done, function (obj) {
							assert.deepEqual({_id:2,scores:25},obj)
							done();
						}))
					}))
				}))
			}))
		})
	})
	describe("$push", function () {
		it("#1 $push basics",function (done) {
			db.collection("push", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:1,scores:[1]}, safe.sure(done, function () {
					_coll.update({_id:1},{$push:{scores:25,age:17,"my.rating":12}}, safe.sure(done, function () {
						_coll.findOne({_id:1},safe.sure(done, function (obj) {
							assert.deepEqual(obj.age,[17]);
							assert.deepEqual(obj.scores,[1,25]);
							assert.deepEqual(obj.my.rating,[12]);
							done();
						}))
					}))
				}))
			}))
		})
		it("#2 $push with each",function (done) {
			db.collection("push", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:2,scores:[1]}, safe.sure(done, function () {
					_coll.update({_id:2},{$push:{scores:{$each:[25,26]},age:{$each:[17,18]}}}, safe.sure(done, function () {
						_coll.findOne({_id:2},safe.sure(done, function (obj) {
							assert.deepEqual(obj.age,[17,18]);
							assert.deepEqual(obj.scores,[1,25,26]);
							done();
						}))
					}))
				}))
			}))
		})
		it("#3 $push on non array throws error",function (done) {
			db.collection("push", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:3,scores:25}, safe.sure(done, function () {
					_coll.update({_id:3},{$push:{scores:2}}, function (err) {
						assert.equal(err.message,"Cannot apply $push/$pushAll modifier to non-array");
						done();
					})
				}))
			}))
		})
	})
	describe("$pushAll", function () {
		it("#1 $pushAll basics",function (done) {
			db.collection("pushAll", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:1,scores:[1]}, safe.sure(done, function () {
					_coll.update({_id:1},{$pushAll:{scores:[25,26],age:[17,18]}}, safe.sure(done, function () {
						_coll.findOne({_id:1},safe.sure(done, function (obj) {
							assert.deepEqual(obj.age,[17,18]);
							assert.deepEqual(obj.scores,[1,25,26]);
							done();
						}))
					}))
				}))
			}))
		})
		it("#2 $pushAll on non array throws error",function (done) {
			db.collection("pushAll", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:2,scores:25}, safe.sure(done, function () {
					_coll.update({_id:2},{$pushAll:{scores:[12,11]}}, function (err) {
						assert.equal(err.message,"Cannot apply $push/$pushAll modifier to non-array");
						done();
					})
				}))
			}))
		})
	})
	describe("$addToSet", function () {
		it("#1 $addToSet basics",function (done) {
			db.collection("addToSet", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:1,scores:[25]}, safe.sure(done, function () {
					_coll.update({_id:1},{$addToSet:{scores:17,age:17}}, safe.sure(done, function () {
						_coll.findOne({_id:1},safe.sure(done, function (obj) {
							assert.deepEqual(obj.scores,[25,17]);
							assert.deepEqual(obj.age,[17]);
							done();
						}))
					}))
				}))
			}))
		})
		it("#2 $addToSet with $each",function (done) {
			db.collection("addToSet", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:2,scores:[25]}, safe.sure(done, function () {
					_coll.update({_id:2},{$addToSet:{scores:{$each:[25,17]}}}, safe.sure(done, function () {
						_coll.findOne({_id:2},safe.sure(done, function (obj) {
							assert.deepEqual(obj.scores,[25,17]);
							done();
						}))
					}))
				}))
			}))
		})
		it("#3 $addToSet on non array throws error",function (done) {
			db.collection("addToSet", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:3,scores:25}, safe.sure(done, function () {
					_coll.update({_id:3},{$addToSet:{scores:-1}}, function (err) {
						assert.equal(err.message,"Cannot apply $addToSet modifier to non-array");
						done();
					})
				}))
			}))
		})
	})
	describe("$pop", function () {
		it("#1 $pop basics",function (done) {
			db.collection("pop", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:1,sub:{scores:[11,12]},scores:[25,27],age:[16,17,18],ratings:[3,4,5]}, safe.sure(done, function () {
					_coll.update({_id:1},{$pop:{scores:-1,age:1,ratings:2,"sub.scores":-1}}, safe.sure(done, function () {
						_coll.findOne({_id:1},safe.sure(done, function (obj) {
							assert.deepEqual(obj.scores,[27]);
							assert.deepEqual(obj.age,[16,17]);
							assert.deepEqual(obj.ratings,[3,4]);
							assert.deepEqual(obj.sub.scores,[12]);
							done();
						}))
					}))
				}))
			}))
		})
		it("#2 $pop on non array throws error",function (done) {
			db.collection("pop", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:2,scores:25}, safe.sure(done, function () {
					_coll.update({_id:2},{$pop:{scores:-1}}, function (err) {
						assert.equal(err.message,"Cannot apply $pop modifier to non-array");
						done();
					})
				}))
			}))
		})
		it("#3 $pop with one or zero elements",function (done) {
			db.collection("pop", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:3,scores:[],age:[16],ratings:[3]}, safe.sure(done, function () {
					_coll.update({_id:3},{$pop:{scores:-1,age:1,ratings:1}}, safe.sure(done, function () {
						_coll.findOne({_id:3},safe.sure(done, function (obj) {
							assert.deepEqual(obj.scores,[]);
							assert.deepEqual(obj.age,[]);
							assert.deepEqual(obj.ratings,[]);
							done();
						}))
					}))
				}))
			}))
		})
		it("#4 $pop on non existent field does nothing",function (done) {
			db.collection("pop", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:4,scores:25}, safe.sure(done, function () {
					_coll.update({_id:4},{$pop:{"jmores.fores":-1,"pores":1}}, safe.sure(done, function () {
						_coll.findOne({_id:4},safe.sure(done, function (obj) {
							assert.deepEqual({_id:4,scores:25},obj)
							done();
						}))
					}))
				}))
			}))
		})
	})
	describe("$pull", function () {
		it("#1 $pull basics",function (done) {
			db.collection("pull", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:1,sub:{scores:[11,12]},scores:[25,27],age:[16,17,18],ratings:[3,4,5]}, safe.sure(done, function () {
					_coll.update({_id:1},{$pull:{scores:25,age:{$gt:17},ratings:{$in:[3,4]},"sub.scores":{$gte:12}}}, safe.sure(done, function () {
						_coll.findOne({_id:1},safe.sure(done, function (obj) {
							assert.deepEqual(obj.scores,[27]);
							assert.deepEqual(obj.age,[16,17]);
							assert.deepEqual(obj.ratings,[5]);
							assert.deepEqual(obj.sub.scores,[11]);
							done();
						}))
					}))
				}))
			}))
		})
		it("#2 $pull on non array throws error",function (done) {
			db.collection("pull", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:2,scores:25}, safe.sure(done, function () {
					_coll.update({_id:2},{$pull:{scores:-1}}, function (err) {
						assert.equal(err.message,"Cannot apply $pull/$pullAll modifier to non-array");
						done();
					})
				}))
			}))
		})
		it("#3 $pull on non existent field does nothing",function (done) {
			db.collection("pull", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:3,scores:25}, safe.sure(done, function () {
					_coll.update({_id:3},{$pull:{"jmores.fores":-1,"pores":1}}, safe.sure(done, function () {
						_coll.findOne({_id:3},safe.sure(done, function (obj) {
							assert.deepEqual({_id:3,scores:25},obj)
							done();
						}))
					}))
				}))
			}))
		})
	})
	describe("$pullAll", function () {
		it("#1 $pullAll basics",function (done) {
			db.collection("pullAll", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:1,sub:{scores:[11,12,13,14,15]},age:[16,17,18]}, safe.sure(done, function () {
					_coll.update({_id:1},{$pullAll:{"sub.scores":[12,14],age:[16,18,20]}}, safe.sure(done, function () {
						_coll.findOne({_id:1},safe.sure(done, function (obj) {
							assert.deepEqual(obj.age,[17]);
							assert.deepEqual(obj.sub.scores,[11,13,15]);
							done();
						}))
					}))
				}))
			}))
		})
		it("#2 $pull on non array throws error",function (done) {
			db.collection("pullAll", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:2,scores:25}, safe.sure(done, function () {
					_coll.update({_id:2},{$pullAll:{scores:[20]}}, function (err) {
						assert.equal(err.message,"Cannot apply $pull/$pullAll modifier to non-array");
						done();
					})
				}))
			}))
		})
		it("#3 $pull on non existent field does nothing",function (done) {
			db.collection("pullAll", {}, safe.sure(done,function (_coll) {
				_coll.insert({_id:3,scores:25}, safe.sure(done, function () {
					_coll.update({_id:3},{$pullAll:{"jmores.fores":[12],"pores":[11]}}, safe.sure(done, function () {
						_coll.findOne({_id:3},safe.sure(done, function (obj) {
							assert.deepEqual({_id:3,scores:25},obj)
							done();
						}))
					}))
				}))
			}))
		})
	})

});
