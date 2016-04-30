// var BPlusTree = require('./bplustree');

function tcache (tdb, size) {
	this._tdb = tdb;
	this.size = size || 1000;
	this._cache = [];
	this._cache.length = this.size;
	this.clear();
}

tcache.prototype.clear = function () {
	for (var i=0; i<this._cache.length; i++) {
		this._cache[i]={k:null};
	}
};

tcache.prototype.set = function (k,v) {
	this._cache[k%this.size] = {k:k,v:this._tdb._cloneDeep(v)};
};

tcache.prototype.unset = function (k) {
	var c = this._cache[k%this.size];
	if (c.k==k)
		this._cache[k%this.size]={k:null};
};

tcache.prototype.get = function (k, unsafe) {
	var c = this._cache[k%this.size];
	return c.k == k ? (unsafe?c.v:this._tdb._cloneDeep(c.v)) : null;
};

module.exports = tcache;
