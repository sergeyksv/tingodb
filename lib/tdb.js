var safe = require('safe');
var tcoll = require('./tcoll.js');
var path = require('path');
var _ = require('lodash');

function tdb(path_, options) {
	this._path = path.resolve(path_);	
	this._cols = {};
	this._name = options.name || path.basename(path_);
}

module.exports = tdb;

tdb.prototype.open = function (options, cb) {
	if (cb==null) cb = options;
	cb = cb || function () {};
	cb(null,this)
}

tdb.prototype.close = function (forceClose, cb) {
	if (cb==null) cb = forceClose;
	cb = cb || function () {};		
	cb(null,this)
}

tdb.prototype.createIndex = function () {
	var c = this._cols[arguments[0]];
	var cb = arguments[arguments.length-1];
	if (!c) 
		return cb (new Error("Collection doesn't exists"));
	var nargs = Array.prototype.slice.call(arguments,1,arguments.length-1);
	c.createIndex.apply(c, nargs);
}

tdb.prototype.collection = function (cname, opts, cb) {
	return this._collection(cname, opts,false, cb)
}

tdb.prototype.createCollection = function (cname, opts, cb) {
	return this._collection(cname, opts,true, cb)
}

tdb.prototype._collection = function (cname, opts, create, cb) {
	var err = null;
	if (!_.isString(cname))
		err = new Error("collection name must be a String");
	if (!err && cname.length==0)
		err = new Error("collection names cannot be empty");
	if (!err && cname.indexOf("$")!=-1)
		err = new Error("collection names must not contain '$'");
	if (!err) { 
		var di = cname.indexOf(".");		
		if (di==0 || di==cname.length-1)
			err = new Error("collection names must not start or end with '.'");
	}
	if (!err && cname.indexOf("..")!=-1)		
		err = new Error("collection names cannot be empty");
		
	if (cb==null) {
		cb = opts;
		opts = {};
	}
	cb = cb || function () {};
	if (err) return cb(err);
	var self = this;
	var c = self._cols[cname];
	if (c) {
		cb((opts.strict && create)?new Error("Collection test_strict_create_collection already exists. Currently in safe mode."):null, c);
		return c;
	} else if (!create && opts.strict) {
		return cb(new Error("Collection does-not-exist does not exist. Currently in safe mode."));
	}
	c = new tcoll();
	c.init(this, cname, opts, safe.sure(cb, function () {
		self._cols[cname] = c;
		cb(null, c);
	}))
	return c;
}

tdb.prototype.collectionNames = function (opts, cb) {
	if (cb==null) {
		cb = opts;
		opts = {};
	}	
	// TODO: Dummy implementation, folder scan is required
	var self = this;
	cb(null,_(this._cols).keys().map(function (e) { return opts.namesOnly?e:{name:self._name+"."+e};}).value())
}

tdb.prototype.collections = function (cb) {
	cb(null, _.values(this._cols))
}

tdb.prototype.dropCollection = function (cname, cb) {
	// TODO: Dummy, must be removed from store as well
	if (!this._cols[cname])
		return cb(new Error("ns not found"));
	delete this._cols[cname];
	cb()
}

tdb.prototype.renameCollection = function (on,nn,cb) {
	// TODO: Dummy non persistant and without name check
	var old = this._cols[on];
	if (old) {
		delete this._cols[on];
		this._cols[nn] = old;
	}
}
