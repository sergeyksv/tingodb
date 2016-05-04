var safe = require('safe');
var tcoll = require('./tcoll.js');
var fs = require('fs');
var path = require('path');
var _ = require('lodash');
var EventEmitter = require('events').EventEmitter;

var mstore = Object.create ? Object.create(null) : {};
function tdb(path_, opts, gopts) {
	this._gopts = gopts;
	this._path = path.resolve(path_);
	this._cols = Object.create ? Object.create(null) : {};
	this._name = opts.name || path.basename(path_);
	this._stype = gopts.memStore?"mem":"fs";
	if (this._stype=="mem")
		mstore[path_] = this._mstore = mstore[path_] || Object.create ? Object.create(null) : {};
	// mongodb compat variables
	this.openCalled = false;
}

tdb.prototype.__proto__ = EventEmitter.prototype;

module.exports = tdb;

tdb.prototype.open = function (options, cb) {
	// actually do nothing for now, we are inproc
	// so nothing to open/close... collection will keep going on their own
	if (cb==null) cb = options;
	cb = cb || function () {};
	this.openCalled = true;
	safe.back(cb,null,this)
}

tdb.prototype.close = function (forceClose, cb) {
	var self = this;
	if (cb==null) cb = forceClose;
	cb = cb || function () {};
	// stop any further operations on current collections
	safe.eachOf(self._cols, function (c, k, cb) {
		c._stop(cb)
	}, safe.sure(cb, function () {
		// and clean list
		self._cols = Object.create ? Object.create(null) : {};
		this.openCalled = false;
		safe.back(cb,null,this);
	}))
}

tdb.prototype.createIndex = _.rest(function (c, args) {
	c = this._cols[c];

	if (!c)
		return safe.back(args[args.length - 1], new Error("Collection doesn't exists"));

	c.createIndex.apply(c, args);
});

tdb.prototype.collection = function (cname, opts, cb) {
	return this._collection(cname, opts,false, cb)
}

tdb.prototype.createCollection = function (cname, opts, cb) {
	return this._collection(cname, opts,true, cb)
}

tdb.prototype._nameCheck = function (cname) {
	var err = null;
	if (!_.isString(cname))
		err = new Error("collection name must be a String");
	if (!err && cname.length==0)
		err = new Error("collection names cannot be empty");
	if (!err && cname.indexOf("$")!=-1)
		err = new Error("collection names must not contain '$'");
	if (!err) {
		var di = cname.indexOf(".");
		if (di==0 || di==cname.length-1)
			err = new Error("collection names must not start or end with '.'");
	}
	if (!err && cname.indexOf("..")!=-1)
		err = new Error("collection names cannot be empty");
	return err;
}

tdb.prototype._collection = function (cname, opts, create, cb) {
	var err = this._nameCheck(cname);

	if (!cb) {
		cb = opts;
		opts = {};
	}
	cb = cb || function () {};
	if (err)
		return safe.back(cb, err);
	var self = this;
	var c = self._cols[cname];
	if (c) {
		if (opts.strict && create) safe.back(cb, new Error("Collection " + cname + " already exists. Currently in safe mode."));
		else safe.back(cb, null, c);
		return c;
	}
	c = new tcoll(this);
	self._cols[cname] = c;
	c.init(this, cname, opts, create, function (err) {
		if (err) {
			delete self._cols[cname];
			cb(err);
		} else
			cb(null, c);
	});
	return c;
};

tdb.prototype.collectionNames = function (opts, cb) {
	var self = this;
	if (_.isUndefined(cb)) {
		cb = opts;
		opts = {};
	}
	if (this._stype=="mem") {
		cb(null,_(self._mstore).keys().map(function (e) { return opts.namesOnly?e:{name:self._name+"."+e};}).value());
	} else {
		fs.readdir(self._path, safe.sure(cb,function(files) {
			// some collections ca be on disk and some only in memory, we need both
			files = _.union(files,_.keys(self._cols));
			cb(null,_(files)
				.reject(function (e) {return /^\./.test(e);}) // ignore hidden linux alike files
				.map(function (e) { return opts.namesOnly?e:{name:self._name+"."+e};})
				.value());
		}));
	}
};

tdb.prototype.collections = function (cb) {
	var self = this;
	self.collectionNames({namesOnly:1},safe.sure(cb, function (names) {
		safe.forEach(names, function (cname, cb) {
			self.collection(cname, cb);
		},safe.sure(cb, function () {
			cb(null, _.values(self._cols));
		}));
	}));
};

tdb.prototype.dropCollection = function (cname, cb) {
	var self = this;
	var c = this._cols[cname];
	if (!c) {
		var err = new Error("ns not found");
		if (cb) return safe.back(cb, err)
			else throw new err;
	}
	c._stop(safe.sure(cb, function (ondisk) {
		delete self._cols[cname];
		if (ondisk)
			fs.unlink(path.join(self._path,cname),safe.sure(cb, function () {
				cb(null, true)
			}))
		else {
			if (self._stype=="mem")
				delete self._mstore[cname];
			cb(null,true);
		}
	}))
}

tdb.prototype.dropDatabase = function (cb) {
	var self = this;
	self.collections(safe.sure(cb, function(collections) {
		safe.forEach(collections, function (c, cb) {
			self.dropCollection(c.collectionName,cb);
		},cb);
	}));
};

tdb.prototype.compactDatabase = function (cb) {
	var self = this;
	self.collections(safe.sure(cb, function(collections) {
		safe.forEach(collections, function (c, cb) {
			c.compactCollection(cb);
		},cb);
	}));
};

tdb.prototype.renameCollection = function (on,nn,opts,cb) {
	if (cb==null) {
		cb = opts;
		opts = {};
	}
	cb = cb || safe.noop;
	var old = this._cols[on];
	if (old)
		old.rename(nn, {}, cb)
	else
		safe.back(cb);
}

tdb.prototype._cloneDeep = function (obj) {
	var self = this;
	return _.cloneDeepWith(obj, function (c) {
		if (c instanceof self.ObjectID)
			return new c.constructor(c.toString());
		if (c instanceof self.Binary)
			return new c.constructor(new Buffer(c.value(true)));
	});
};
