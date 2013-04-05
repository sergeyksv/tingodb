var safe = require('safe');
var tcoll = require('./tcoll.js');
var path = require('path');

function tdb(path_, options) {
	this._path = path.resolve(path_);	
	this._cols = {};
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

tdb.prototype.collection = tdb.prototype.createCollection = function (cname, options, cb) {
	if (cb==null) {
		cb = options;
		options = {};
	}
	cb = cb || function () {};
	var self = this;
	var c = self._cols[cname];
	if (c) {
		cb(null, c);
		return c;
	}
	c = new tcoll();
	c.init(this, cname, options, safe.sure(cb, function () {
		self._cols[cname] = c;
		cb(null, c);
	}))
	return c;
}
