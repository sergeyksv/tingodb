var _ = require('lodash');
var BPlusTree = require('./bplustree');

function tindex (key,tcoll,options,name) {
	this.options = options || {};
	this.name = name || key+'_';
	this._unique = this.options.unique || false;
	this._c = tcoll;
	this._nuls = Object.create ? Object.create(null) : {};
	this._array = this.options._tiarr || false;
	this.key = key[0][0];
	this.order = key[0][1];
	if (key.length > 1) this._sub = key.slice(1);
	this._bp = BPlusTree.create({ sort: this.order, order: 100 });
	var getter = new tcoll._tdb.Finder.field(this.key);
	this._get = new Function("obj", "return " + (this._array ? getter.native3() : getter.native()));
}
tindex.prototype.clear = function () {
	if (this.count())
		this._bp = BPlusTree.create({ sort: this.order, order: 100 });
};

tindex.prototype.set = function (k_,v, check) {
	var self = this;
	var k = this._get(k_);

	if (check) {
		if (!this._sub && this._unique && this._bp.get(k) !== null)
			throw new Error("duplicate key error index");
	} else {
		if (_.isArray(k)) {
			_.each(k, function (k1) {
				self._set(k1, v, k_);
			});
		}
		else
			return this._set(k, v, k_);
	}
};

tindex.prototype._set = function (k, v, o) {
	if (this._sub) {
		var s = (_.isNull(k) || _.isUndefined(k)) ? this._nuls[v] : this._bp.get(k);
		if (!s) {
			s = new tindex(this._sub, this._c, this.options, this.name + '_' + k);
			if (_.isNull(k) || _.isUndefined(k)) this._nuls[v] = s;
				else this._bp.set(k, s);
		}
		s.set(o, v);
		return;
	}
	if (_.isNull(k) || _.isUndefined(k)) {
		this._nuls[v]=v;
		return;
	}
	if (this._unique)
		return this._bp.set(k,v);
	else {
		var l = this._bp.get(k);
		var n = l || [];
		n.push(v);
		if (!l) this._bp.set(k,n);
	}
};

tindex.prototype.del = function (k_,v) {
	var self = this;
	var k = this._get(k_);
	if (_.isArray(k)) {
		_.each(k, function (k1) {
			self._del(k1, v, k_);
		});
	}
	else
		return this._del(k, v, k_);
};

tindex.prototype._del = function (k, v, o) {
	if (this._sub) {
		var s = (_.isNull(k) || _.isUndefined(k)) ? this._nuls[v] : this._bp.get(k);
		if (s) s.del(o, v);
		return;
	}
	delete this._nuls[v];
	if (this._unique) {
		this._bp.del(k);
	}
	else {
		var l = this._bp.get(k);
		if (l) {
			var i = l.indexOf(v);
			if (i != -1)
				l.splice(i, 1);
			if (l.length===0)
				this._bp.del(k);
		}
	}
};

tindex.prototype.match = function (k) {
	var m = this._bp.get(k);
	if (!m) return [];
	return this._unique || this._sub ? [ m ] : m;
};

tindex.prototype.range = function (s, e, si, ei) {
	var r = this._bp.rangeSync(s,e,si,ei);
	return this._unique || this._sub ? r : _.flatten(r);
};

tindex.prototype.all = function (order, shallow) {
	var a = this._bp.all();
	var n = _.values(this._nuls);
	var r = this.order > 0 ? _.union(n, a) : _.union(a, n);
	if (order && order.length > 0) {
		if (order[0] != this.order) r = r.reverse();
		order = order.slice(1);
	}
	if (this._sub) return shallow ? r : _(r).map(function (i) { return i.all(order); }).flattenDeep().value();
	return this._unique?r:_.flatten(r);
};

tindex.prototype.nuls = function () {
	return _.values(this._nuls);
};

tindex.prototype.values = function () {
	var r = this._bp.all();
	return this._unique || this._sub ? r : _.flatten(r);
};

tindex.prototype.count = function () {
	var c = 0;
	this._bp.each(function (k,v) {
		c += this._sub ? v.count() : v.length;
	});
	return c;
};

tindex.prototype.fields = function () {
	var result = [ [ this.key, this.order ] ];
	if (this._sub) result = result.concat(this._sub);
	return result;
};

tindex.prototype.depth = function () {
	return this._sub ? this._sub.length + 1 : 1;
};

tindex.prototype.inspect = function (depth) {
	return '[Index ' + this.name + ']';
};

module.exports = tindex;

/* IDEAS
 *
 * - Keep some stats about index to allow of making decisions about which index to use in query
 *
 */
