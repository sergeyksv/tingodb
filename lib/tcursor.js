var safe = require('safe');
var _ = require('underscore');
var async = require('async');

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
	this._items = null;
	this._sort = null;
	this._order = null;
	this._arFields = {};
	this._fieldsType = null;
	this._fields = fields;
	
	_.each(fields, function (v,k) {
		if (v == 0 || v==1) {
			if (self._fieldsType==null)
				self._fieldsType = v;
			if (self._fieldsType==v) {
				if (k.indexOf("_tiar.")==0)
					self._arFields[k.substr(6)]=1;
			} else if (!self._err)
				self._err = new Error("Mixed set of projection options (0,1) is not valid");
		} else if (!self._err)
			self._err = new Error("Unsupported projection option: "+JSON.stringify(v));
	})
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
		if (t) {
			n[k] = t[k];
		}
	})
	return nobj;
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
			self._get(pos, safe.sure(cb, function (obj) {
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
	self._c._find(self._query, {}, self._skip, self._limit, self._sort, self._order, self._arFields, safe.sure_result(cb, function (data) {
		self._items = data;
		self._i=0;
	}))
}

tcursor.prototype._get = function (pos,cb) {
	var self = this;
	self._c._get(pos, safe.sure(cb, function (obj) {
		if (self._fieldsType!=null) {
			if (self._fieldsType==0)
				applyProjectionDel(obj,self._fields)
			else
				obj = applyProjectionGet(obj, self._fields,{_id:obj._id})
		}
		cb(null,obj)
	}))
}
		


module.exports = tcursor;
