var _ = require('underscore');
var BPlusTree = require('./bplustree');

function tcache (size) {
	this.size = size || 20000;
	this._cache = [];
	this._cache.length = this.size;
	for (var i=0; i<this._cache.length; i++) {
		this._cache[i]={k:null};
	}
}

tcache.prototype.set = function (k,v) {
	this._cache[k%this.size] = {k:k,v:v};
}

tcache.prototype.get = function (k) {
	var c = this._cache[k%this.size];
	return c.k==k?c.v:null;
}

module.exports = tcache;
