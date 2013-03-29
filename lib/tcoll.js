var safe = require('safe');
var _ = require('underscore');
var fs = require('fs');
var path = require('path');
var async = require('async');
var finder = require('./finder');
var tcursor = require('./tcursor');
var wqueue = require('./wqueue');
var tindex = require('./tindex');
var tcache = require("./tcache");

function tcoll() {
	var self = this;
	this._tdb = null;
	this._name = null;
	this._store = {};
	this._fd = null;
	this._fsize = null;
	this._id = 1;
	this._wq = new wqueue();
	this._tq = null;
	this._idx = {};
	this._cache = new tcache();
	this._mc = {};
}

module.exports = tcoll;

tcoll.prototype.init = function (tdb, name, options, cb) {
	var self= this;
	this._tdb = tdb;
	this._name = name;
	var pos = 0;
	this._tq = new wqueue(100, function (cb) {
		fs.open(path.join(self._tdb._path,self._name), "a+", safe.sure(cb, function (fd) {
			self._fd = fd;
			var b1 = new Buffer(45);
			async.whilst(function () { return self._fsize==null; }, function(cb) {
				fs.read(fd, b1, 0, 45, pos, safe.sure(cb, function (bytes, data) {
					if (bytes==0) {
						self._fsize = pos;
						return cb();
					}
					var h1 = JSON.parse(data.toString());
					h1.o = parseInt(h1.o,10);
					h1.k = parseInt(h1.k,10);
					var b2 = new Buffer(h1.o);
					fs.read(fd,b2,0,h1.o,pos+45+2+h1.k, safe.sure(cb, function (bytes, data) {
						var obj = unwrapTypes(JSON.parse(data.toString()));
						var id = parseInt(obj._id);
						if ( !isNaN(id) )
							self._id = Math.max(id,self._id)
						self._store[obj._id]=pos;
//						self._cache.set(pos,obj);
						// update indexes
						_(self._idx).forEach(function(v,k) {
							v.set(obj[k],obj._id);
						})						
						pos+=45+3+h1.o+h1.k;
						cb();
					}))
				}))
			}, cb)
		}));
	})
	self.ensureIndex({_id:1},{unique:true},cb);
}

tcoll.prototype.ensureIndex = function (obj, options, cb) {
	var self = this;
	if (_.isFunction(options) && cb == null) {
		cb = options;
		options = {};
	}
	if (_.size(obj)!=1)
		return cb(new Error("Compound indexes are not supported yet"));
	var key = _.keys(obj)[0];
	if (self._idx[key])
		return cb(null,self._idx[key]);
	var index = new tindex(self, options)
	
	if (self._tq._tc==-1) {
		// if no operation is pending just register index
		self._idx[key] = index;
		cb()
	}
	else {
		// overwise register index operation
		this._tq.add(function (cb) {	
			var range = _(self._store).values();	
			async.forEachSeries(range, function (pos, cb) {
				self._get(pos, safe.sure(cb, function (obj) {
					index.set(obj[key],obj._id);
					cb()
				}))
			}, safe.sure(cb, function () {
				self._idx[key] = index;
				cb()
			}))
		},true,cb);
	}
}

tcoll.prototype.get = function (id, cb) {
	var self = this;
	this._tq.add(function (cb) {	
		var pos = self._store[id]; 
		if (pos == null) return cb(null,null);
		self._get(pos, cb);
	},false, cb)
}

tcoll.prototype._get = function (pos, cb) {
	var self = this;
	var cached = self._cache.get(pos);
	if (cached)
		return process.nextTick(function () {cb(null, cached)});
	var b1 = new Buffer(45);
	fs.read(self._fd, b1, 0, 45, pos, safe.sure(cb, function (bytes, data) {
		var h1 = JSON.parse(data.toString());
		h1.o = parseInt(h1.o,10);
		h1.k = parseInt(h1.k,10);
		var b2 = new Buffer(h1.o);
		fs.read(self._fd,b2,0,h1.o,pos+45+2+h1.k, safe.sure(cb, function (bytes, data) {
			var obj = unwrapTypes(JSON.parse(data.toString()));
			self._cache.set(pos,obj);
			cb(null,obj);
		}))
	}))
}

tcoll.prototype.scan = function (worker) {
	worker(null, null, null)
}

tcoll.prototype.insert = function (docs, options, cb ) {
	var self = this;
	if (_.isFunction(options) && cb == null) {
		cb = options;
		options = {};
	}
	if (_.isObject(docs))
		docs = [docs];	
	this._tq.add(function (cb) {		
		async.forEachSeries(docs, function (doc, cb) {
			if (_.isUndefined(doc._id)) {
				doc._id = self._id;
				self._id++;
			}
			self._put(doc, false, cb);
		}, safe.sure(cb, function () {
			cb(null, docs);
		}))
	}, true, cb)
}

function wrapTypes(obj) {
	_.each(obj, function (v,k) {
		if (_.isDate(v))
			obj[k] = {$wrap:1,$date:v}
		else if (_.isObject(v))
			wrapTypes(v)
	})
	return obj;
}

function unwrapTypes(obj) {
	_.each(obj, function (v,k) {
		if (_.isObject(v)) {
			if (v.$wrap && v.$date)
				obj[k]=new Date(v.$date);
			else unwrapTypes(v);
		}
	})
	return obj;
}
	
tcoll.prototype._put = function (item, remove, cb) {
	var self = this;
	self._wq.add(function (cb) {	
		item = wrapTypes(item);
		var key = {_id:item._id};
		var sobj = JSON.stringify(remove?null:item);
		var skey = JSON.stringify(key);		
		var zeros = "0000000000";
		var lobj = sobj.length.toString();
		var lkey = skey.length.toString();
		lobj = zeros.substr(0,zeros.length - lobj.length)+lobj;
		lkey = zeros.substr(0,zeros.length - lkey.length)+lkey;
		var h1={k:lkey,o:lobj,v:"001"};
		var buf = new Buffer(JSON.stringify(h1)+"\n"+skey+"\n"+sobj+"\n");

		fs.write(self._fd,buf, 0, buf.length, self._fsize, safe.sure( cb, function (written) {
			if (remove)
				delete self._store[item._id];
			else			
				self._store[item._id]=self._fsize;
			item = unwrapTypes(item);		
			if (remove)	
				self._cache.unset(self._fsize)			
			else
				self._cache.set(self._fsize,item);
			self._fsize+=written;
			// update indexes
			_(self._idx).forEach(function(v,k) {
				if (item)
					v.set(item[k],item._id);
				else
					v.del(item[k],item._id);
			})
			cb(null);		
		}))
	}, true, cb);
}

tcoll.prototype.count = function (cb) {
	var self = this;
	this._tq.add(function (cb) {	
		cb(null, _.size(self._store));
	},false,cb);
}

tcoll.prototype.findOne = function (query, cb) {
	this.find(query).limit(1).nextObject(cb);
}

tcoll.prototype.find = function (query, cb) {
	var c = new tcursor(this,query);
	if (cb)
		cb(null, c)
	else
		return c;
}

function applySet(obj,$set) {
	_.each($set, function (v,k) {
		var path = k.split(".")
		var t = null;
		if (path.length==1)
			t = obj
		else {
			var l = obj;
			for (var i=0; i<path.length-1; i++) {
				var p = path[i];
				if (!l[p]) 
					l[p] = {};
				l = l[p];
			}
			t = l;
			k = path[i];
		}
		if (_.isObject(v)) {
			if (!t[k])
				t[k]={};
			applySet(t[k],v);
		}
		else
			t[k] = v;
	})
}

function applyUnset(obj,$set) {
	_.each($set, function (v,k) {
		var path = k.split(".")
		var t = null;
		if (path.length==1)
			t = obj
		else {
			var l = obj;
			for (var i=0; i<path.length-1; i++) {
				var p = path[i];
				if (!l[p]) 
					l[p] = {};
				l = l[p];
			}
			t = l;
			k = path[i];
		}
		delete t[k];
	})
}

tcoll.prototype.update = function (query, doc, opts, cb) {
	var self = this;
	if (_.isFunction(opts) && cb == null) {
		cb = opts;
		opts = {};
	}	
	var multi = opts.multi || false;
	var $set = doc.$set;
	var $unset = doc.$unset;
	var $push = doc.$push;
	var $doc = ($set || $unset || $push)?null:doc;
	this._tq.add(function (cb) {	
		self.__find(query,null,0,multi?null:1,null,null, safe.sure(cb, function(res) {
			async.forEachSeries(res, function (pos, cb) {
				self._get(pos, safe.sure(cb, function (obj) {
					// remove current version of doc from indexes
					_(self._idx).forEach(function(v,k) {
						v.del(obj[k],obj._id);
					})					
					if (!$doc) {
						$doc = obj;
						if ($set)
							applySet($doc,$set);
						if ($unset)
							applyUnset($doc,$unset);
					}
					$doc._id = obj.id;
					// put will add it back to indexes
					self._put($doc, false, cb);
				}))
			}, cb)
		}))
	},true,cb);	
}

tcoll.prototype.remove = function (query, opts, cb) {
	var self = this;
	if (_.isFunction(opts) && cb == null) {
		cb = opts;
		opts = {};
	}	
	var single = opts.single || false;
	this._tq.add(function (cb) {	
		self.__find(query,null,0,single?1:null,null,null, safe.sure(cb, function(res) {
			async.forEachSeries(res, function (pos, cb) {
				self._get(pos, safe.sure(cb, function (obj) {
					self._put(obj,true,cb);
				}))
			}, cb)
		}))
	},true,cb);	
}

function sget(obj, k) {
	var i=0; var p = obj;
	var path = k.split(".");
	for (;i<path.length;i++) {
		p=p[path[i]];
		if (!p)
			break;
	}
	if (p != undefined && i==path.length)
		return p;
	else
		return null;
}

tcoll.prototype.__find = function (query, fields, skip, limit, sort_, order, cb) {
	var sort = sort_;
	var self = this;
	var res = [];
	var range = [];
	// now simple non-index search
	var found = 0;
	var qt = finder.matcher(query);
	var pi = [];
	var io = {};
	// for non empty query check indexes that we can use
	if (_.size(qt)>0) {
		_(self._idx).forEach(function (v,k) {
			if (qt._ex(k)==1)
				pi.push(k)
		});
	}
	// if possible indexes found split the query and process
	// indexes separately
	if (pi.length>0) {
		_(pi).forEach(function (v) {
			io[v]=qt.split(v);
		})
		var p = [];
		_(io).forEach(function (v,k) {
			var r = v._index(self._idx[k]);
			p.push(r);
		})
		if (pi.length==1) {
			p=p[0];
			if (pi[0]==sort) {
				sort = null;
				if (order==-1)
					p.reverse();
			}
		} else
			p = _.intersection.apply(_,p);
		_(p).forEach(function (_id) {
			range.push(self._store[_id])
		})
	} else {
		if (self._idx[sort]) {
			_.each(self._idx._id.all(), function (_id) {
				range.push(self._store[_id])
			})
			if (order==-1)
				range.reverse();
			sort = null;
		} else
			range = _.values(self._store);
	}
	
	// no sort, no query then return right away
	if (sort==null && _.size(qt)==0 || qt._args.length==0) {
		if (skip!=0 || limit!=null) {
			var c = Math.min(range.length-skip,limit?limit:range.length-skip);
			range = range.splice(skip,c)
		}
		return cb(null,range);				
	}

	var matcher = null;
	eval("matcher = function (obj) { return "+ qt.native() + " }");
	
	// create sort index
	var si = null;
	if (sort) {
		si = new tindex();
	}
		
	async.forEachSeries(range, function (pos, cb) {
		if (sort==null && limit && res.length>=limit) return process.nextTick(cb);
		self._get(pos, safe.sure(cb, function (obj) {
			if (matcher(obj)) {
				if (sort!=null || found>=skip) {
					if (sort==null) 
						res.push(pos);
					else
						si.set(obj[sort],pos);
				}
				found++;
			}
			cb()
		}))
	}, safe.sure(cb, function () {
		if (sort) {
			res = si.all();
			if (order==-1)
				res.reverse();
			if (skip!=0 || limit!=null) {
				var c = Math.min(res.length-skip,limit?limit:res.length-skip);
				res = res.splice(skip,c)
			}			
		}	
		cb(null, res);
	}))
}

tcoll.prototype._find = function (query, fields, skip, limit, sort_, order, cb) {
	var self = this;
	this._tq.add(function (cb) {	
		self.__find(query, fields, skip, limit,sort_,order, cb);
	}, false, cb);
}
