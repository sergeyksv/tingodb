var _ = require('lodash');
var BPlusTree = require('./bplustree');

function tcache (tdb, size) {
	this._tdb = tdb;
	this.size = size || 1000;
	this._cache = [];
	this._cache.length = this.size;
	for (var i=0; i<this._cache.length; i++) {
		this._cache[i]={k:null};
	}
}

tcache.prototype.set = function (k,v) {
	this._cache[k%this.size] = {k:k,v:v};
}

tcache.prototype.unset = function (k) {
	var c = this._cache[k%this.size];
	if (c.k==k)
		this._cache[k%this.size]={k:null}
}

tcache.prototype.get = function (k) {
	var self = this;
	var c = this._cache[k%this.size];
	return c.k==k?_.cloneDeep(c.v,function (c) {
		if (c instanceof self._tdb.ObjectID)
			return new c.constructor(c.$oid)
		else if (c instanceof self._tdb.Binary)
			return new c.constructor(new Buffer(c.value(true)));
		else return;
	}):null;
}

module.exports = tcache;
