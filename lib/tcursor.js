var safe = require('safe');
var _ = require('lodash');
var CursorStream = require('./tstream');

function tcursor (tcoll,query,fields,opts) {
	var self = this;
	this.INIT = 0;
	this.OPEN = 1;
	this.CLOSED = 2;
	this.GET_MORE = 3;
	this._query = query;
	this._c = tcoll;
	this._i = 0;
	this._skip = 0;
	this._limit = null;
	this._count = null;
	this._items = null;
	this._sort = null;
	this._hint = opts.hint;
	this._arFields = Object.create ? Object.create(null) : {};
	this._fieldsType = null;
	this._fieldsExcludeId = false;
	this._fields = Object.create ? Object.create(null) : {};
	this.timeout = _.isUndefined(opts.timeout)?true:opts.timeout;

	_.each(fields, function (v,k) {
		if (!k && _.isString(v)) {
			k=v; v = 1;
		}
		if (v == 0 || v==1) {
			// _id treated specially
			if (k=="_id" && v==0) {
				self._fieldsExcludeId = true;
				return;
			}

			if (self._fieldsType==null)
				self._fieldsType = v;
			if (self._fieldsType==v) {
				if (k.indexOf("_tiar.")==0)
					self._arFields[k.substr(6)]=1;
				else
					self._fields[k]=v;
			} else if (!self._err)
				self._err = new Error("Mixed set of projection options (0,1) is not valid");
		} else if (!self._err)
			self._err = new Error("Unsupported projection option: "+JSON.stringify(v));
	})
	// _id treated specially
	if ((self._fieldsType===0 || self._fieldsType===null) && self._fieldsExcludeId) {
		self._fieldsType = 0;
		self._fields['_id']=0;
	}
}

function applyProjectionDel(obj,$set) {
	_.each($set, function (v,k) {
		var path = k.split(".")
		var t = null;
		if (path.length==1)
			t = obj
		else {
			var l = obj;
			for (var i=0; i<path.length-1; i++) {
				var p = path[i];
				if (l[p]==null)
					break;
				l = l[p];
			}
			t = i==path.length-1?l:null;
			k = path[i];
		}
		if (t)
			delete t[k];
	})
}

function applyProjectionGet(obj,$set,nobj) {
	_.each($set, function (v,k) {
		var path = k.split(".")
		var t = null,n=null;
		if (path.length==1) {
			t = obj;
			n = nobj;
		}
		else {
			var l = obj, nl = nobj;
			for (var i=0; i<path.length-1; i++) {
				var p = path[i];
				if (l[p]==null) break; l = l[p];
				if (nl[p]==null) nl[p]={}; nl = nl[p];
			}
			if (i==path.length-1) {
				t = l;
				n = nl;
			}
			k = path[i];
		}
		if (!_.isUndefined(t[k])) {
			n[k] = t[k];
		}
	})
	return nobj;
}



tcursor.prototype.isClosed  = function () {
	if (!this._items)
		return false;
	return this._i==-1 || this._i>=this._items.length;
}

tcursor.prototype.skip = function (v, cb) {
	var self = this;
	if (!_.isFinite(v)) {
		self._err = new Error("skip requires an integer");
		if (!cb) throw self._err;
	}
	if (self._i!=0) {
		self._err = new Error('Cursor is closed');
		if (!cb) throw self._err;
	}
	if (!self._err)
		this._skip = v;
	if (cb)
		process.nextTick(function () {cb(self._err,self)})
	return this;
}

function parseSortList(l, d) {
	var message = "Illegal sort clause, " +
		"must be of the form [['field1', '(ascending|descending)'], " +
		"['field2', '(ascending|descending)']]";
	// null or empty string
	if (!l) return null;
	// sanity check
	if (!_.isObject(l) && !_.isString(l)) throw new Error(message);
	// 'a' => [ [ 'a', 1 ] ]
	if (!d) d = 1;
	// 'a', 1 => [ [ 'a', 1 ] ]
	if (_.isString(l)) l = [ [ l, d ] ];
	// { a: 1, b: -1 } => [ [ 'a', 1 ], [ 'b', -1 ] ]
	else if (!_.isArray(l)) l = _.map(l, function (v, k) { return [ k, v ]; });
	// [ 'a', 1 ], [ 'a', 'asc' ] => [ [ 'a', 1 ] ]
	else if (_.isString(l[0]) && (l[1] == 1 || l[1] == -1 ||
				l[1] == 'asc' || l[1] == 'ascending' ||
				l[1] == 'desc' || l[1] == 'descending')) l = [ l ];
	// [ 'a', 'b' ] => [ [ 'a', 1 ], [ 'b', 1 ] ]
	else if (_.every(l, _.isString)) l = _.map(l, function (v) { return [ v, d ]; });
	// empty array or object
	if (_.isEmpty(l)) return null;
	// 'asc', 'ascending' => 1; 'desc', 'descending' => -1
	return _.map(l, function (v) {
		var d = v[1];
		if (d == 'asc' || d == 'ascending') d = 1;
		else if (d == 'desc' || d == 'descending') d = -1;
		if (d != 1 && d != -1) throw new Error(message);
		return [ v[0], d ];
	});
}

tcursor.prototype.sort = function (v, d, cb) {
	var self = this;
	if (_.isFunction(d)) {
		cb = d;
		d = null;
	}
	if (self._i!=0)
		this._err = new Error('Cursor is closed');
	if (!this._err) {
		this._err = _.attempt(function () {
			self.sortValue = v; // just to pass contrib test
			self._sort = parseSortList(v, d);
		});
	}
	if (cb)
		process.nextTick(function () {cb(self._err, self)})
	return this;
}

tcursor.prototype.limit = function (v, cb) {
	var self = this;
	if (!_.isFinite(v)) {
		self._err = new Error("limit requires an integer");
		if (!cb) throw self._err;
	}
	if (self._i!=0) {
		self._err = new Error('Cursor is closed');
		if (!cb) throw self._err;
	}
	if (!self._err) {
		this._limit = v==0?null:Math.abs(v);
	}
	if (cb)
		process.nextTick(function () {cb(self._err,self)})
	return this;
}

tcursor.prototype.nextObject = function (cb) {
	var self = this;
	if (self._err) {
		if (cb) process.nextTick(function () {cb(self._err)})
		return;
	}
	self._ensure(safe.sure(cb, function () {
		if (self._i>=self._items.length)
			return cb(null, null);
		self._get(self._items[self._i], cb)
		self._i++;
	}))
}

tcursor.prototype.count = function (applySkipLimit, cb) {
	var self = this;
	if (!cb) {
		cb = applySkipLimit;
		applySkipLimit = false;
	}
	if (self._err) {
		if (cb) process.nextTick(function () {cb(self._err)})
		return;
	}
	if ((!self._skip && self._limit === null) || applySkipLimit) {
		self._ensure(safe.sure(cb, function () {
			cb(null, self._items.length);
		}));
		return;
	}
	if (self._count !== null) {
		process.nextTick(function () {
			cb(null, self._count);
		});
		return;
	}
	self._c._find(self._query, {}, 0, null, null, self._hint, self._arFields, safe.sure(cb, function (data) {
		self._count = data.length;
		cb(null, self._count);
	}));
}

tcursor.prototype.setReadPreference = function (the, cb) {
	var self = this;
	if (self._err) {
		if (cb) process.nextTick(function () {cb(self._err)})
		return;
	}
	return this;
}

tcursor.prototype.batchSize = function (v, cb) {
	var self = this;
	if (!_.isFinite(v)) {
		self._err = new Error("batchSize requires an integer");
		if (!cb) throw self._err;
	}
	if (self._i!=0) {
		self._err = new Error('Cursor is closed');
		if (!cb) throw self._err;
	}
	if (cb) process.nextTick(function () {cb(self._err,self)})
	return this;
}

tcursor.prototype.close = function (cb) {
	var self = this;
	this._items = [];
	this._i=-1;
	this._err = null;
	if (cb)
		process.nextTick(function () {cb(self._err,self)})
	return this;
}

tcursor.prototype.rewind = function () {
	this._i=0;
	return this;
}

tcursor.prototype.toArray = function (cb) {
	if (!_.isFunction(cb))
		throw new Error('Callback is required');
	var self = this;

	if (self.isClosed())
		self._err = new Error("Cursor is closed");

	if (self._err) {
		if (cb) process.nextTick(function () {cb(self._err)})
		return;
	}

	self._ensure(safe.sure(cb, function () {
		safe.mapSeries(self._i!=0?self._items.slice(self._i,self._items.length):self._items, function (pos,cb) {
			self._get(pos, safe.sure(cb, function (obj) {
				cb(null, obj);
			}));
		}, safe.sure(cb, function (res) {
			self._i=self._items.length;
			cb(null, res);
		}))
	}))
}

tcursor.prototype.each = function (cb) {
	if (!_.isFunction(cb))
		throw new Error('Callback is required');

	var self = this;

	if (self.isClosed())
		self._err = new Error("Cursor is closed");

	if (self._err) {
		if (cb) process.nextTick(function () {cb(self._err)})
		return;
	}
	self._ensure(safe.sure(cb, function () {
		safe.forEachSeries(self._i!=0?self._items.slice(self._i,self._items.length):self._items, function (pos,cb1) {
			self._get(pos, safe.sure(cb, function (obj) {
				cb(null,obj)
				cb1();
			}))
		}, safe.sure(cb, function () {
			self._i=self._items.length;
			cb(null, null);
		}))
	}))
}

tcursor.prototype.stream = function (options) {
	return new CursorStream(this, options);
};

tcursor.prototype._ensure = function (cb) {
	var self = this;
	if (self._items!=null)
		return process.nextTick(cb);
	self._c._find(self._query, {}, self._skip, self._limit, self._sort, self._hint, self._arFields, safe.sure_result(cb, function (data) {
		self._items = data;
		self._i=0;
	}))
}

tcursor.prototype._projectFields = function (obj) {
	var self = this;
	if (self._fieldsType!=null) {
		if (self._fieldsType==0) {
			applyProjectionDel(obj,self._fields)
		}
		else
			obj = applyProjectionGet(obj, self._fields,self._fieldsExcludeId?{}:{_id:obj._id})
	}
	return obj;
}

tcursor.prototype._get = function (pos,cb) {
	var self = this;
	self._c._get(pos, false, safe.sure(cb, function (obj) {
		cb(null,self._projectFields(obj))
	}))
}

module.exports = tcursor;
