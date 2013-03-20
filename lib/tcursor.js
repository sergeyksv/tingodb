var safe = require('safe');
var _ = require('underscore');
var async = require('async');

function tcursor (tcoll,query) {
	this._query = query;
	this._c = tcoll;
	this._i = 0;
	this._skip = 0;
	this._limit = null;
	this._items = null;
}

tcursor.prototype.skip = function (v, cb) {
	this._skip = v;
	if (cb) cb()
	return this;
}

tcursor.prototype.limit = function (v, cb) {
	this._limit = v;
	if (cb) cb();
	return this;
}

tcursor.prototype.nextObject = function (cb) {
	var self = this;
	self._ensure(safe.sure(cb, function () {
		if (self._i>=self._items.length)
			return cb(null, null);
		self._c._get(self._items[self._i], cb)
		self._i++;
	}))
}
tcursor.prototype.count = function (applySkipLimit, cb) {
	var self = this;
	if (!cb) cb = applySkipLimit;
	self._ensure(safe.sure(cb, function () {	
		cb(null, self._items.length);
	}))
}

tcursor.prototype.toArray = function (cb) {
	var self = this;
	self._ensure(safe.sure(cb, function () {
		var res = [];
		async.forEach(self._items, function (pos,cb) {
			self._c._get(pos, safe.sure(cb, function (obj) {
				res.push(obj)
				cb();
			}))
		}, function () {
			cb(null, res);
		})
	}))
}

tcursor.prototype._ensure = function (cb) {
	var self = this;	
	if (self._items!=null)
		return cb();
	self._c._find(self._query, {}, self._skip, self._limit, safe.sure_result(cb, function (data) {
		self._items = data;
		self._i=0;
	}))
}

module.exports = tcursor;
