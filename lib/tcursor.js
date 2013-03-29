var safe = require('safe');
var _ = require('underscore');
var async = require('async');

function tcursor (tcoll,query) {
	this.INIT = 0;
	this.OPEN = 1;
	this.CLOSED = 2;
	this.GET_MORE = 3;
	this._query = query;
	this._c = tcoll;
	this._i = 0;
	this._skip = 0;
	this._limit = null;
	this._items = null;
	this._sort = null;
	this._order = null;
}

tcursor.prototype.isClosed  = function () {
	if (this._items==null)
		return this.INIT;
	else if (this._i==this._items.length-1)
		return this.CLOSED;
	else
		return this.OPEN;
}

tcursor.prototype.skip = function (v, cb) {
	var self = this;
	this._skip = v;
	if (cb)
		process.nextTick(function () {cb(self._err)})
	return this;
}

tcursor.prototype.sort = function (v, cb) {
	var self = this;
	var key = null;
	if (!this._err) {
		if (_.isObject(v)) {
			var size = _.isArray(v)?v.length/2:_.size(v);
			if (size==1) {
				if (_.isArray(v) && v.length==2) {
					this._sort = v[0];
					this._order = v[1];
				} else {
					this._sort = _.keys(v)[0];
					this._order = v[this._sort];
				}
				if (this._sort) {
					if (this._order == 'ascending')
						this._order = 1;
					if (this._order == 'descending')
						this._order = -1;
					if (!(this._order==1 || this._order==-1))
						this._err = new Error("Attempting to use index type '"+self._order+"' where index types are not allowed (1 or -1 only).");
				}
			} else this._err = new Error("Multi field sort is not supported");
		} else
			this._err = new Error("Illegal sort clause, must be of the form [['field1', '(ascending|descending)'], ['field2', '(ascending|descending)']");
	}
	if (cb)
		process.nextTick(function () {cb(self._err)})
	return this;
}

tcursor.prototype.limit = function (v, cb) {
	var self = this;
	this._limit = v;
	if (cb)
		process.nextTick(function () {cb(self._err)})
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
		self._c._get(self._items[self._i], cb)
		self._i++;
	}))
}

tcursor.prototype.count = function (applySkipLimit, cb) {
	var self = this;
	if (!cb) cb = applySkipLimit;	
	if (self._err) {	
		if (cb) process.nextTick(function () {cb(self._err)})
		return;
	}	
	self._ensure(safe.sure(cb, function () {	
		cb(null, self._items.length);
	}))
}

tcursor.prototype.setReadPreference = function (the, cb) {
	var self = this;
	if (self._err) {	
		if (cb) process.nextTick(function () {cb(self._err)})
		return;
	}	
	return this;
}

tcursor.prototype.batchSize = function (batchSize, cb) {
	var self = this;
	if (self._err) {	
		if (cb) process.nextTick(function () {cb(self._err)})
		return;
	}	
	return this;
}

tcursor.prototype.close = function (cb) {
	var self = this;
	this._items=null;
	this._i=0;
	this._err = null;
	if (self._err) {	
		if (cb) process.nextTick(function () {cb(self._err)})
		return;
	}	
	return this;
}

tcursor.prototype.rewind = function () {
	this._i=0;
	return this;
}

tcursor.prototype.toArray = function (cb) {
	var self = this;
	
	if (self.isClosed() == this.CLOSED)
		self._err = new Error("Cursor is closed");
		
	if (self._err) {	
		if (cb) process.nextTick(function () {cb(self._err)})
		return;
	}
		
	self._ensure(safe.sure(cb, function () {
		var res = [];
		async.forEachSeries(self._i!=0?self._items.slice(self._i,self._items.length):self._items, function (pos,cb) {
			self._c._get(pos, safe.sure(cb, function (obj) {
				res.push(obj)
				cb();
			}))
		}, safe.sure(cb, function () {
			self._i=self._items.length-1;
			cb(null, res);
		}))
	}))
}

tcursor.prototype.each = function (cb) {
	var self = this;
	
	if (self.isClosed() == this.CLOSED)
		self._err = new Error("Cursor is closed");
	
	if (self._err) {	
		if (cb) process.nextTick(function () {cb(self._err)})
		return;
	}
	self._ensure(safe.sure(cb, function () {
		var res = [];
		async.forEachSeries(self._i!=0?self._items.slice(self._i,self._items.length):self._items, function (pos,cb1) {
			self._c._get(pos, safe.sure(cb, function (obj) {
				cb(null,obj)
				cb1();
			}))
		}, safe.sure(cb, function () {
			self._i=self._items.length-1;			
			cb(null, null);
		}))
	}))
}

tcursor.prototype.stream = function () {
	throw new Error("Cursor.stream is not implemented");
}

tcursor.prototype._ensure = function (cb) {
	var self = this;	
	if (self._items!=null)
		return process.nextTick(cb);
	self._c._find(self._query, {}, self._skip, self._limit, self._sort, self._order, safe.sure_result(cb, function (data) {
		self._items = data;
		self._i=0;
	}))
}

module.exports = tcursor;
