var safe = require('safe');
var _ = require('underscore');
var fs = require('fs');
var path = require('path');
var async = require('async');
var finder = require('./finder');
var tcursor = require('./tcursor');
var wqueue = require('./wqueue');

function tcoll() {
	var self = this;
	this._tdb = null;
	this._name = null;
	this._store = {};
	this._fd = null;
	this._fsize = null;
	this._id = 1;
	this._wq = new wqueue();
	this._tq = new wqueue();
}

module.exports = tcoll;

tcoll.prototype.init = function (tdb, name, options, cb) {
	var self= this;
	this._tdb = tdb;
	this._name = name;
	var pos = 0;
	fs.open(path.join(this._tdb._path,this._name), "a+", safe.sure(cb, function (fd) {
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
				var b2 = new Buffer(h1.k);
				fs.read(fd,b2,0,h1.k,pos+45+1, safe.sure(cb, function (bytes, data) {
					var obj = JSON.parse(data.toString());
					var id = parseInt(obj._id);
					if ( !isNaN(id) )
						self._id = Math.max(id,self._id)
					self._store[obj._id]=pos;
					pos+=45+3+h1.o+h1.k;
					cb();
				}))
			}))
		}, cb)
	}));
}

tcoll.prototype.addIndex = function () {
	console.log(arguments);
	_(arguments).last()(null);
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
	fs.read(self._fd, b1, 0, 45, pos, safe.sure(cb, function (bytes, data) {
		var h1 = JSON.parse(data.toString());
		h1.o = parseInt(h1.o,10);
		h1.k = parseInt(h1.k,10);
		var b2 = new Buffer(h1.o);
		fs.read(self._fd,b2,0,h1.o,pos+45+2+h1.k, safe.sure(cb, function (bytes, data) {
			var obj = JSON.parse(data.toString());
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
		async.forEach(docs, function (doc, cb) {
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
		var kitem = {"_id":item._id};
		var sobj = JSON.stringify(item);
		var skey = JSON.stringify(kitem);
		var zeros = "0000000000";
		var lobj = sobj.length.toString();
		var lkey = skey.length.toString();
		lobj = zeros.substr(0,zeros.length - lobj.length)+lobj;
		lkey = zeros.substr(0,zeros.length - lkey.length)+lkey;
		var h1={k:lkey,o:lobj,v:"001"};
		var buf = new Buffer(JSON.stringify(h1)+"\n"+skey+"\n"+sobj+"\n");
		
		fs.write(self._fd,buf, 0, buf.length, self._fsize, safe.sure( cb, function (written) {
			self._store[item._id]=self._fsize;
			self._fsize+=written;
			cb(null);		
		}))
	}, true, cb);
}

tcoll.prototype.count = function (cb) {
	cb(null, _.size(this._store));
}

tcoll.prototype.find = function (query, cb) {
	var c = new tcursor(this,query);
	if (cb)
		cb(null, c)
	else
		return c;
}

tcoll.prototype._find = function (query, fields, skip, limit, cb) {
	var self = this;
	this._tq.add(function (cb) {	
		var res = [];
		var range = _(self._store).values();	
		// special case for getting all data with offset and/or limit
		if (_(query).size()==0 && (skip!=0 || limit!=null)) {
			var c = Math.min(range.length-skip,limit?limit:range.length-skip);
			range = range.splice(skip,c)
			skip =0; limit=null;
		}
		// now simple non-index search
		var found = 0;
		var matcher = finder.matcher(query);
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
	}, false, cb);
}
