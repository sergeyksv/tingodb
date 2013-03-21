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
					fs.read(fd,b2,0,h1.o,pos+45+1, safe.sure(cb, function (bytes, data) {
						var obj = JSON.parse(data.toString());
						var id = parseInt(obj._id);
						if ( !isNaN(id) )
							self._id = Math.max(id,self._id)
						self._store[obj._id]=pos;
						// update indexes
						_(self._idx).forEach(function(v,k) {
							v.set(obj[k],obj._id);
						})						
						pos+=45+2+h1.o;
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
	var b1 = new Buffer(45);
	var cached = self._cache.get(pos);
	if (cached)
		return process.nextTick(function () {cb(null, cached)});
	fs.read(self._fd, b1, 0, 45, pos, safe.sure(cb, function (bytes, data) {
		var h1 = JSON.parse(data.toString());
		h1.o = parseInt(h1.o,10);
		h1.k = parseInt(h1.k,10);
		var b2 = new Buffer(h1.o);
		fs.read(self._fd,b2,0,h1.o,pos+45+1, safe.sure(cb, function (bytes, data) {
			var obj = JSON.parse(data.toString());
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
			self._put(doc, cb);
		}, safe.sure(cb, function () {
			cb(null, docs);
		}))
	}, true, cb)
}
	
tcoll.prototype._put = function (item, cb) {
	var self = this;
	self._wq.add(function (cb) {	
		var sobj = JSON.stringify(item);
		var zeros = "0000000000";
		var lobj = sobj.length.toString();
		lobj = zeros.substr(0,zeros.length - lobj.length)+lobj;
		var h1={k:zeros,o:lobj,v:"001"};
		var buf = new Buffer(JSON.stringify(h1)+"\n"+sobj+"\n");

		fs.write(self._fd,buf, 0, buf.length, self._fsize, safe.sure( cb, function (written) {
			self._store[item._id]=self._fsize;
			self._fsize+=written;
			// update indexes
			_(self._idx).forEach(function(v,k) {
				v.set(item[k],item._id);
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

tcoll.prototype.find = function (query, cb) {
	var c = new tcursor(this,query);
	if (cb)
		cb(null, c)
	else
		return c;
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

tcoll.prototype._find = function (query, fields, skip, limit, cb) {
	var self = this;
	this._tq.add(function (cb) {	
		var res = [];
		var range = [];
		// special case for getting all data with offset and/or limit
		if (_(query).size()==0) {
			range = _(self._store).values();				
			if (skip!=0 || limit!=null) {
				var c = Math.min(range.length-skip,limit?limit:range.length-skip);
				range = range.splice(skip,c)
			}
			cb(null,range);
		} else {
			// now simple non-index search
			var found = 0;
			var qt = finder.matcher(query);
			var pi = [];
			var io = {};
			_(self._idx).forEach(function (v,k) {
				if (qt._ex(k)==1)
					pi.push(k)
			});
			if (pi.length>0) {
				_(pi).forEach(function (v) {
					io[v]=qt.split(v);
				})
				var p = [];
				_(io).forEach(function (v,k) {
					var r = v._index(self._idx[k]);
					p.push(r);
				})
				p = _.intersection.apply(_,p);
				_(p).forEach(function (_id) {
					range.push(self._store[_id])
				})
			} else
				range = _(self._store).values();
			
			if (qt._args.length==0)
				return cb(null,range);

			var matcher = null;
			eval("matcher = function (obj) { return "+ qt.native() + " }");
				
			async.forEach(range, function (pos, cb) {
				if (limit && res.length>=limit) return process.nextTick(cb);
				self._get(pos, safe.sure(cb, function (obj) {
					if (matcher(obj)) {
						if (found>=skip)
							res.push(pos);
						found++;
					}
					cb()
				}))
			}, safe.sure(cb, function () {
				cb(null, res);
			}))
		}
	}, false, cb);
}
