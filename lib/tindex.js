var _ = require('underscore');
var BPlusTree = require('./bplustree');

function tindex (tcoll,options) {
	options = options || {};
	this._unique = options._unique || false;
	this._c = tcoll;
	this._bp = BPlusTree.create({order:100});
}

tindex.prototype.set = function (k,v) {
	if (this._unique)
		return this._bp.set(k,v);
	else {
		var l = this._bp.get(k);
		var n = l || [];
		n = _.union(n,[v]);
		if (!l)
			this._bp.set(k,n);
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
	var r = this._bp.all()
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
