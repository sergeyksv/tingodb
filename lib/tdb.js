var safe = require('safe');
var tcoll = require('./tcoll.js');
var path = require('path');

function tdb() {
	this._path = "";
	this._cols = {};
}

module.exports = tdb;

tdb.prototype.init = function (path_, options, cb) {
	this._path = path.resolve(path_);
	cb(null)
}

tdb.prototype.ensure = function (cname, options, cb) {
	var self = this;
	var c = self._cols[cname];
	if (c) return cb(null, c);
	c = new tcoll();
	c.init(this, cname, options, safe.sure(cb, function () {
		self._cols[cname] = c;
		cb(null, c);
	}))
}
