var _ = require('underscore');
var BPlusTree = require('./bplustree');
var finder = require('../lib/finder');

function tindex (key,tcoll,options) {
	options = options || {};
	this._unique = options.unique || false;
	this._c = tcoll;
	this._bp = BPlusTree.create({order:100});
	this._nuls = {};
	this._array = options._tiarr || false;
	this._key = key;
	var getter = new finder.field(key);
	eval("this._get = function (obj) { return "+ (this._array?getter.native3():getter.native()) + " }");
	
}

tindex.prototype.set = function (k_,v) {
	var self = this;
	var k = this._get(k_);
	if (_.isArray(k)) {
		_.each(k, function (k1) {
			self._set(k1,v);
		})
	}
	else
		return this._set(k,v)
}

tindex.prototype._set = function (k,v) {
	if (k==null) {
		this._nuls[v]=1;
		return;
	}
	if (this._unique)
		return this._bp.set(k,v);
	else {
		var l = this._bp.get(k);
		var n = l || [];
		n = _.union(n,[v]);
		this._bp.set(k,n);
	}
}

tindex.prototype.del = function (k_,v) {
	var k = this._get(k_);
	if (_.isArray(k)) {
		_.each(k, function (k1) {
			self._del(k1,v);
		})
	}
	else
		return this._del(k,v)
}

tindex.prototype._del = function (k,v) {
	delete
		this._nuls[v];
	if (this.unique) 
		this._bp.del(k) 
	else {
		var l = this._bp.get(k);
		if (l) {
			var n = _.without(l,v);
			if (n.length>0)
				this._bp.set(k,n)
			else
				this._bp.del(k)
		}
	}
}

tindex.prototype.match = function (k) {
	return (this._unique?[this._bp.get(k)]:this._bp.get(k)) || [];
}

tindex.prototype.range = function (s, e, si, ei) {
	var r = this._bp.rangeSync(s,e,si,ei);
	return this._unique?r:_.flatten(r);
}

tindex.prototype.all = function () {
	var r = _.union(this._bp.all(), _.keys(this._nuls));
	return this._unique?r:_.flatten(r);
}

tindex.prototype.count = function () {
	var c = 0;
	this._bp.each(function (k,v) {
		c+=v.length;
	});
	return c;
}

module.exports = tindex;
