var safe = require('safe');
var _ = require('underscore');
var async = require('async');

function tcursor (tcoll,query) {
	this._query = query;
	this._c = tcoll;
	this._items = null;
}

tcursor.prototype.count = function (applySkipLimit, cb) {
	if (!cb) cb = applySkipLimit;
	self._ensure(safe.sure(cb, function () {	
		cb(null, this._items.length);
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
	self._c._find(this._query, safe.sure_result(cb, function (data) {
		self._items = data;
	}))
}

module.exports = tcursor;
