var vows = require('vows');
var assert = require('assert');
var main = require('../lib/main');
var temp = require('temp');
var _ = require('underscore');
var async = require('async');

function dummyDataCheck(index) {
    var context = {
        topic: function (coll) {
			coll.get(index, this.callback);
        }
    };
    context['ok'] = function (err, v) {
		assert.equal(v.id,index);
	}
    return context;
}

function randomRead(max,size) {
	var context = {
		topic: function (coll) {
			return coll;
		}
	}

	context['at ' + 0] = dummyDataCheck(0);
	context['at ' + (max-1)] = dummyDataCheck(max-1);
	
	for (i=0; i<size;i++) {
		var index = Math.floor(Math.random() * max);
		context['at ' + index] = dummyDataCheck(index)
	}
	
	return context;
}
	
var path = "./data";
vows.describe('Basic').addBatch({
	'New store':{
		topic: function () {
			var self = this;
			temp.mkdir('test', function (err, path_) {
//				path = path_;
				main.open(path, {}, self.callback)
			})
		},
		"can be created by path":function (db) {
			assert.notEqual(db,null);
		},
		"collection":{
			topic:function (db) {
				db.ensure("test", {}, this.callback)
			},
			"can be created":function (coll) {
				assert.notEqual(coll,null);
			},			
			"populated with test data 1":{
				topic:function (coll) {
					var i=0;
					async.whilst(function () { return i<1000}, 
						function (cb) {
							coll.put(i, {id:i,sin:Math.sin(i),cos:Math.cos(i)}, cb);
							i++;
						},
						this.callback
					)
				},
				"ok":function() {},				
			},
			"populated with test data":{
				topic:function (coll) {
					var i=0;
					async.whilst(function () { return i<1000}, 
						function (cb) {
							coll.put(i, {id:i,sin:Math.sin(i),cos:Math.cos(i)}, cb);
							i++;
						},
						this.callback
					)
				},
				"ok":function() {},
				"has proper size":{
					topic:function (coll) {
						coll.size(this.callback);
					},
					"ok":function (err, size) {
						assert.equal(size, 1000);
					}
				},
				"random read":randomRead(1000,1),
			}
		}
	}
}).addBatch({
	'Existing store':{
		topic: function () {
			main.open(path, {}, this.callback)
		},
		"can be created by path":function (db) {
			assert.notEqual(db,null);
		},
		"test collection":{
			topic:function (db) {
				db.ensure("test", {}, this.callback)
			},
			"exists":function (coll) {
				assert.notEqual(coll,null);
			},			
			"has proper size":{
				topic:function (coll) {
					coll.size(this.callback);
				},
				"ok":function (err, size) {
					assert.equal(size, 1000);
				}
			},
			"random read":randomRead(1000,1),
		}
	}
}).export(module);
