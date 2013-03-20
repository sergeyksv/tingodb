var safe = require('safe');
var _ = require('underscore');
var async = require('async');

function tcursor (tcoll) {
	this._coll = tcoll;
	this._items = [];
}

tcursor.prototype.count = function (applySkipLimit, cb) {
	if (!cb) cb = applySkipLimit;
	cb(null, this._items.length);
}

tcursor.prototype.toArray = function (cb) {
	var self = this;
	var res = [];
	async.forEach(this._items, function (pos,cb) {
		self._coll._get(pos, safe.sure(cb, function (obj) {
			res.push(obj)
			cb();
		}))
	}, function () {
		cb(null, res);
	})
}

module.exports = tcursor;
