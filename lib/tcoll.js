var safe = require('safe');
var _ = require('lodash');
var crypto = require('crypto');
var fs = require('fs');
var path = require('path');
var tcursor = require('./tcursor');
var wqueue = require('./wqueue');
var tindex = require('./tindex');
var tcache = require("./tcache");
var Code = require('./tcode').Code;
var tutils = require('./utils');
var Updater = require('./updater');

function tcoll(tdb) {
	this._tdb = null;
	this._name = null;
	this._store = Object.create ? Object.create(null) : {};
	this._fd = null;
	this._fsize = null;
	this._id = 1;
	this._wq = new wqueue();
	this._tq = null;
	this._idx = Object.create ? Object.create(null) : {};
	this._cache = null;
	// this._mc = {};
	this._check1 = Math.random()*100+1;
	// native mongo db compatibility attrs
	this.collectionName = null;
	if (tdb._stype=="mem") {
		this.init = this.initM;
		this._put = this._putM;
		this._get = this._getM;
	} else {
		this.init = this.initFS;
		this._put = this._putFS;
		this._get = this._getFS;
	}
}

module.exports = tcoll;

tcoll.prototype.initM = function (tdb, name, options, create, cb) {
	var self= this;
	this._tdb = tdb;
	tdb._mstore = tdb._mstore || Object.create ? Object.create(null) : {};
	this.collectionName = this._name = name;
	if (options.strict) {
		var exists = tdb._mstore[name];
		if (exists && create) return cb(new Error("Collection " + self._name + " already exists. Currently in safe mode."));
		else if (!exists && !create) return cb(new Error("Collection " + self._name + " does not exist. Currently in safe mode."));
	}
	tdb._mstore[name] = this._mstore = tdb._mstore[name] || [];
	for (var k=0; k< this._mstore.length; k++) {
		var o = this._mstore[k];
		if (o) {
			self._store[simplifyKey(o._id)]={pos:k+1};
		}
	}
	this._tq = new wqueue(100, function (cb) {
		// update indexes
		safe.eachOfSeries(self._store, function (rec, k, cb) {
			self._get(rec.pos, false, safe.sure(cb, function (obj) {
				var id = simplifyKey(obj._id);
				_.each(self._idx,function(v, k) {
					v.set(obj, id);
				});
				cb();
			}));
		}, cb);
	});
	self.ensureIndex({_id: 1}, {name: '_id_', unique: true}, cb);
};

tcoll.prototype.initFS = function (tdb, name, options, create, cb) {
	var self= this;
	this._tdb = tdb;
	this._cache = new tcache(tdb, tdb._gopts.cacheSize);
	this._cmaxobj = tdb._gopts.cacheMaxObjSize || 1024;
	this.collectionName = this._name = name;
	this._filename = path.join(this._tdb._path, this._name);
	if (options.strict) {
		var exists = fs.existsSync(self._filename);
		if (exists && create) return cb(new Error("Collection " + self._name + " already exists. Currently in safe mode."));
		else if (!exists && !create) return cb(new Error("Collection " + self._name + " does not exist. Currently in safe mode."));
	}
	var pos = 0;
	var deleted = 0;
	var found = 0;
	this._tq = new wqueue(100, function (cb) {
		safe.run(function (cb) {
			fs.open(self._filename, "a+", safe.sure(cb, function (fd) {
			self._fd = fd;
			var b1 = new Buffer(45);
			safe.whilst(function () { return self._fsize === null; }, function(cb) {
				safe.run(function (cb) {
					fs.read(fd, b1, 0, 45, pos, safe.trap_sure(cb, function (bytes, data) {
						if (bytes===0) {
							self._fsize = pos;
							return cb();
						}
						var h1 = JSON.parse(data.toString());
						h1.o = parseInt(h1.o,10);
						h1.k = parseInt(h1.k,10);
						var b2 = new Buffer(h1.k);
						fs.read(fd,b2,0,h1.k,pos+45+1, safe.sure(cb, function (bytes, data) {
							var k = JSON.parse(data.toString());
							self._id = k._uid;
							if (k._a=='del') {
								delete self._store[k._id];
								deleted++;
							} else {
								if (self._store[k._id]) deleted++;
								self._store[k._id] = { pos: pos, sum: k._s };
							}
							pos+=45+3+h1.o+h1.k;
							found++;
							cb();
						}));
					}));
				}, function (err) {
					if (err)
						cb(new Error(self._name+": Error during load - "+err.toString()));
					else
						cb();
				});
			}, cb);
			}));
		}, function (err) {
			if (!found && err)
				return cb(err); // nothing read and error, just rise it

			safe.run(function (cb) {
				var size = _.size(self._store);
				// autocompact on certain ratio or err
				if (deleted > size || err) {
					self._compact(function (errCompact) {
						if (errCompact && err)
							cb(errCompact);
						else {
							if (errCompact) console.log(err);
							cb();
						}
					});
				} else cb();
			}, function () {
				self._refreshIndexes(cb);
			});
		});
	});
	self.ensureIndex({_id: 1}, {name: '_id_', unique: true}, cb);
};

tcoll.prototype.compactCollection = function (cb) {
	var self = this;
	self._tq.add(function (cb) {
		self._compact(safe.sure(cb, function () {
			self._cache.clear();
			self._refreshIndexes(cb);
		}));
	}, true, cb);
};

tcoll.prototype._refreshIndexes = function (cb) {
	var self = this;
	_.each(self._idx,function(v, k) {
		v.clear();
	});
	safe.eachOfSeries(self._store, function (rec, k, cb) {
		self._get(rec.pos, false, safe.sure(cb, function (obj) {
			var id = simplifyKey(obj._id);
			_.each(self._idx,function(v, k) {
				v.set(obj, id);
			});
			cb();
		}));
	}, cb);
};

tcoll.prototype._compact = function (cb) {
	var self = this;
	var filename = self._filename + '.compact';
	fs.open(filename, 'w+', safe.sure(cb, function (fd) {
		var b1 = new Buffer(45);
		function get(pos, cb) {
			fs.read(self._fd, b1, 0, 45, pos, safe.trap_sure(cb, function (bytes, data) {
				var h1 = JSON.parse(data.toString());
				h1.o = parseInt(h1.o, 10);
				h1.k = parseInt(h1.k, 10);
				var b2 = new Buffer(h1.k + h1.o + 3);
				fs.read(self._fd, b2, 0, b2.length, pos + 45, safe.sure(cb, function (bytes, data) {
					cb(null, Buffer.concat([ b1, b2 ]));
				}));
			}));
		}
		var wpos = 0;
		var store = Object.create ? Object.create(null) : {};
		safe.eachOfSeries(self._store, function (rec, k, cb) {
			get(rec.pos, safe.sure(cb, function (data) {
				fs.write(fd, data, 0, data.length, wpos, safe.sure(cb, function (written) {
					if (written != data.length) return cb(new Error('Insufficient disk space'));
					store[k] = { pos: wpos, sum: rec.sum };
					wpos += data.length;
					cb();
				}));
			}));
		}, function (err) {
			if (err) {
				fs.close(fd, function () {
					fs.unlink(filename, function () {
						cb(err);
					});
				});
				return;
			}
			if (!!process.platform.match(/^win/)) {
				// WINDOWS: unsafe because if something fail while renaming file it will not
				// restore automatically
				fs.close(self._fd, safe.sure(cb,function() {
					fs.close(fd, safe.sure(cb,function() {
						fs.unlink(self._filename, safe.sure(cb,function () {
							fs.rename(filename, self._filename, safe.sure(cb, function () {
								fs.open(self._filename, 'a+', safe.sure(cb, function (fd) {
									self._fd = fd;
									self._fsize = wpos;
									self._store = store;
									cb();
								}));
							}));
						}));
					}));
				}));
		    } else {
				// safe way
				fs.rename(filename, self._filename, safe.sure(cb, function () {
					fs.close(self._fd);
					self._fd = fd;
					self._fsize = wpos;
					self._store = store;
					cb();
				}));
			}
		});
	}));
};

tcoll.prototype.drop = function (cb) {
	this._tdb.dropCollection(this._name,cb);
};

tcoll.prototype.rename = function (nname, opts, cb) {
	var self = this;
	if (_.isFunction(opts)) {
		cb = opts;
		opts = {};
	}
	var err = self._tdb._nameCheck(nname);
	if (err)
		return safe.back(cb,err);
	if (self._tdb._stype=="mem") {
		delete self._tdb._cols[self._name];
		self._tdb._cols[nname] = self;
		delete self._tdb._mstore[self._name];
		self._tdb._mstore[nname] = self._mstore;
		safe.back(cb,null);
	} else {
		self._tq.add(function (cb) {
			fs.rename(path.join(self._tdb._path,self._name),path.join(self._tdb._path,nname),safe.sure(cb, function () {
				delete self._tdb._cols[self._name];
				self._tdb._cols[nname] = self;
				self.collectionName = self._name = nname;
				cb();
			}));
		},true,cb);
	}
};

tcoll.prototype._stop = function (cb) {
	var self = this;
	self._tq.add(function (cb) {
		// this will prevent any tasks processed on this instance
		self._tq._stoped = true;
		if (self._fd) {
			fs.close(self._fd,safe.sure(cb, function () {
				cb(null,true);
			}));
		} else
			cb(null,false);
	},true,cb);
};

tcoll.prototype.createIndex = tcoll.prototype.ensureIndex = function (obj, options, cb) {
	var self = this;
	if (_.isFunction(options)) {
		cb = options;
		options = {};
	}
	cb = cb || function () {};
	options = options || {};

	var c = new tcursor(this,{},{},{});
	c.sort(obj);
	if (c._err)
		return safe.back(cb,c._err);
	var key = c._sort;

	if (key===null)
		return safe.back(cb,new Error("No fields are specified"));

	var index = self._idx[key];
	if (index)
		return safe.back(cb,null, index.name);

	// force array support when global option is set
	if (_.isUndefined(options._tiarr) && self._tdb._gopts.searchInArray)
		options._tiarr = true;

	var name = options.name || _.map(key, function (v) { return v[0] + '_' + v[1]; }).join('_');
	index = new tindex(key, self, options, name);

	if (self._tq._tc==-1) {
		// if no operation is pending just register index
		self._idx[key] = index;
		safe.back(cb, null, index.name);
	}
	else {
		// overwise register index operation
		this._tq.add(function (cb) {
			var range = _.values(self._store);
			safe.forEachSeries(range, function (rec, cb) {
				self._get(rec.pos, false, safe.sure(cb, function (obj) {
					index.set(obj,simplifyKey(obj._id));
					cb();
				}));
			}, safe.sure(cb, function () {
				self._idx[key] = index;
				cb();
			}));
		}, true, function (err) {
			if (err) cb(err);
			else cb(null, index.name);
		});
	}
};

tcoll.prototype.indexExists = function (idx, cb) {
	if (!_.isArray(idx))
		idx = [idx];
	var i = _.intersection(idx,_(this._idx).values().map('name').value());
	cb(null,i.length == idx.length);
};

tcoll.prototype.indexes = function (cb) {
	var self = this;
	this._tq.add(function (cb) {
		cb(null, _.values(self._idx));
	},false,cb);
};

tcoll.prototype._getM = function (pos, unsafe, cb) {
	safe.back(cb,null,unsafe?this._mstore[pos-1]:this._tdb._cloneDeep(this._mstore[pos-1]));
};

tcoll.prototype._getFS = function (pos, unsafe, cb) {
	var self = this;
	var cached = self._cache.get(pos,unsafe);
	if (cached)
		return safe.back(cb,null,cached);
	var b1 = new Buffer(45);
	fs.read(self._fd, b1, 0, 45, pos, safe.trap_sure(cb, function (bytes, data) {
		var h1 = JSON.parse(data.toString());
		h1.o = parseInt(h1.o,10);
		h1.k = parseInt(h1.k,10);
		var b2 = new Buffer(h1.o);
		fs.read(self._fd,b2,0,h1.o,pos+45+2+h1.k, safe.trap_sure(cb, function (bytes, data) {
			var obj = self._unwrapTypes(JSON.parse(data.toString()));
			if (bytes <= self._cmaxobj)
				self._cache.set(pos, obj);
			cb(null,obj);
		}))
	}))
}

tcoll.prototype.insert = function (docs, opts, cb ) {
	var self = this;
	if (_.isFunction(opts) && cb == null) {
		cb = opts;
		opts = {};
	}
	opts = opts || {};
	if (opts.w>0 && !_.isFunction(cb))
		throw new Error("Callback is required for safe update");
	cb = cb || function () {};
	if (!_.isArray(docs))
		docs = [docs];
	this._tq.add(function (cb) {
		safe.forEachSeries(docs, function (doc, cb) {
			if (_.isUndefined(doc._id)) {
				doc._id = new self._tdb.ObjectID();
			}
			self._put(doc, false, cb);
		}, safe.sure(cb, function () {
			cb(null, docs);
		}))
	}, true, cb)
}

tcoll.prototype._wrapTypes = function(obj) {
	var self = this;
	_.each(obj, function (v,k) {
		if (_.isDate(v))
			obj[k] = {$wrap:"$date",v:v.valueOf(),h:v}
		else if (v instanceof self._tdb.ObjectID)
			obj[k] = {$wrap:"$oid",v:v.toJSON()}
		else if (v instanceof self._tdb.Binary)
			obj[k] = {$wrap: "$bin", v: v.toJSON()};
		else if (_.isObject(v))
			self._wrapTypes(v)

	})
	return obj;
}

tcoll.prototype._ensureIds = function(obj) {
	var self = this;
	_.each(obj, function (v,k) {
		if (k.length >0) {
			if (k[0]=='$')
				throw new Error("key "+k+" must not start with '$'");
			if (k.indexOf('.')!=-1)
				throw new Error("key "+k+" must not contain '.'");
		}
		if (_.isObject(v)) {
			if (v instanceof self._tdb.ObjectID) {
				if (v.id<0) {
					v._persist(++self._id)
				}
			}
			else
				self._ensureIds(v)
		}
	})
	return obj;
}


tcoll.prototype._unwrapTypes = function(obj) {
	var self = this;
	_.each(obj, function (v,k) {
		if (_.isObject(v)) {
			switch (v.$wrap) {
				case "$date": obj[k] = new Date(v.v); break;
				case "$oid":
					var oid = new self._tdb.ObjectID(v.v);
					obj[k]=oid;
				break;
				case "$bin":
					var bin = new self._tdb.Binary(new Buffer(v.v, 'base64'));
					obj[k] = bin;
				break;
				default: self._unwrapTypes(v);
			}
		}
	})
	return obj;
}

tcoll.prototype._putM = function (item_, remove, cb) {
	var item = this._tdb._cloneDeep(item_);
	var self = this;
	self._wq.add(function (cb) {
		var err = _.attempt(function () {
			item = self._ensureIds(item);
		});
		if (err) {
			err.errmsg = err.toString();
			return cb(err);
		}
		if (_.isUndefined(item._id))
			return cb(new Error("Invalid object key (_id)"));

		var key = {_id:simplifyKey(item._id)};

		// check index update
		if (item && !remove) {
			err = _.attempt(function () {
				_.each(self._idx,function(v,k) {
					v.set(item,key._id,true);
				});
			});

			if (err) {
				err.errmsg = err.toString();
				return cb(err);
			}
		}

		if (remove) {
			self._mstore[self._store[key._id].pos-1]=null;
			delete self._store[key._id];
		}
		else {
			if (self._store[key._id]) {
				self._mstore[self._store[key._id].pos-1] = item;
			} else {
				self._mstore.push(item);
				self._store[key._id] = {pos: self._mstore.length};
			}
		}

		// update index
		_.each(self._idx,function(v,k) {
			if (!remove)
				v.set(item,key._id);
			else
				v.del(item,key._id);
		})
		cb(null);
	}, true, cb);
}


tcoll.prototype._putFS = function (item, remove, cb) {
	var self = this;
	self._wq.add(function (cb) {
		var err = _.attempt(function () {
			item = self._ensureIds(item);
		});
		if (err) {
			err.errmsg = err.toString();
			return cb(err);
		}
		if (_.isUndefined(item._id))
			return cb(new Error("Invalid object key (_id)"));
		item = self._wrapTypes(item);
		var sobj = new Buffer(remove?"":JSON.stringify(item));
		item = self._unwrapTypes(item);
		var key = {_id:simplifyKey(item._id),_uid:self._id,_dt:(new Date()).valueOf()};
		if (remove) key._a = "del";
		else {
			var hash = crypto.createHash('md5');
			hash.update(sobj, 'utf8');
			key._s = hash.digest('hex');
		}
		var skey = new Buffer(JSON.stringify(key));
		var zeros = "0000000000";
		var lobj = sobj.length.toString();
		var lkey = skey.length.toString();
		lobj = zeros.substr(0,zeros.length - lobj.length)+lobj;
		lkey = zeros.substr(0,zeros.length - lkey.length)+lkey;
		var h1={k:lkey,o:lobj,v:"001"};
		var buf = new Buffer(JSON.stringify(h1)+"\n"+skey+"\n"+sobj+"\n");

		// check index update
		if (item && !remove) {
			err = _.attempt(function () {
				_.each(self._idx,function(v,k) {
					v.set(item,key._id,true);
				})
			});
			if (err) {
				err.errmsg = err.toString();
				return cb(err);
			}
		}

		safe.run(function (cb) {
			var rec = self._store[key._id];
			if (rec && rec.sum == key._s) return safe.back(cb);
			fs.write(self._fd, buf, 0, buf.length, self._fsize, safe.sure(cb, function (written) {
				if (remove)
					delete self._store[key._id];
				else
					self._store[key._id] = { pos: self._fsize, sum: key._s };

				if (remove || sobj.length > self._cmaxobj)
					self._cache.unset(self._fsize)
				else
					self._cache.set(self._fsize,item);
				self._fsize+=written;
				// randomly check for non exclusive file usage
				// which is growth of file that we are nor aware
				// randomly to avoid overhead
				if (self._check1==0) {
					this._check1 = Math.random()*100+1;
					fs.fstat(self._fd, safe.sure(cb, function (stat) {
						if (self._fsize!=stat.size)
							cb(new Error("File size mismatch. Are you use db/collection exclusively?"))
						else
							cb()
					}))
				} else {
					self._check1--;
					cb();
				}
			}));
		},
		function () {
			// update index
			_.each(self._idx,function(v,k) {
				if (!remove)
					v.set(item,key._id);
				else
					v.del(item,key._id);
			})
			cb(null);
		});
	}, true, cb);
}

tcoll.prototype.count = function (query, options, cb) {
	var self = this;
	if (arguments.length == 1) {
		cb = arguments[0];
        options = null;
		query = null;
	}
	if (arguments.length == 2) {
        query = arguments[0];
		cb = arguments[1];
        options = null;
	}

	if (query==null || _.size(query)==0) {
		this._tq.add(function (cb) {
			cb(null, _.size(self._store));
		},false,cb);
	} else
		self.find(query, options).count(cb);
};

tcoll.prototype.stats = function (cb) {
	var self = this;
	this._tq.add(function (cb) {
		cb(null, {count:_.size(self._store)});
	},false,cb);
}


var findOpts = ['limit','sort','fields','skip','hint','timeout','batchSize','safe','w'];

tcoll.prototype.findOne = _.rest(function (args) {
	var cb = args.pop();
	this.find.apply(this,args).limit(1).nextObject(cb);
});

tcoll.prototype.find = function () {
	var cb = null, query = {}, opts = {}, fields = null, skip = null, limit = null, sort = null;
	var argc = arguments.length;
	if (argc>0) {
		// guess callback, it is always latest
		cb = arguments[argc-1];
		if (!_.isFunction(cb))
			cb=null
		else
			argc--;
		if (argc>0) {
			// query should always exist
			query = arguments[0]
			if (argc>1) {
				if (argc==2) {
					var val = arguments[1];
					// worst case we get either options either fiels
					if (_(val).keys().intersection(findOpts).size() > 0)
						opts = val
					else
						fields = val;
				} else {
					fields = arguments[1];
					if (argc == 3)
						opts = arguments[2]
					else {
						skip = arguments[2];
						limit = arguments[3]
					}
				}
			}
		}
	}

	opts = opts || {};
	skip = skip || opts.skip || null;
	limit = limit || opts.limit || null;
	fields = fields || opts.fields || null;
	sort = sort || opts.sort || null;


	var c = new tcursor(this,query, fields, opts);

	if (skip) c.skip(skip);
	if (limit) c.limit(limit);
	if (sort) c.sort(sort);
	if (cb)
		cb(null, c)
	else
		return c;
}


function simplifyKey(key) {
	var k = key;
	if (key.toJSON)
		k = key.toJSON();
	if (_.isNumber(k)||_.isString(k))
		return k;
	return k.toString();
}

tcoll.prototype.update = function (query, doc, opts, cb) {
	var self = this;
	if (_.isFunction(opts) && cb == null) {
		cb = opts;
	}
	opts = opts || {};
	if (opts.w>0 && !_.isFunction(cb))
		throw new Error("Callback is required for safe update");
	cb = cb || function () {}
	if (!_.isObject(query))
		throw new Error("selector must be a valid JavaScript object");
	if (!_.isObject(doc))
		throw new Error("document must be a valid JavaScript object");

	var multi = opts.multi || false;
    var updater = new Updater(doc, self._tdb);
	var $doc = updater.hasAtomic()?null:doc;
	this._tq.add(function (cb) {
		self.__find(query, null, 0, multi ? null : 1, null, opts.hint, {}, safe.sure(cb, function (res) {
			if (res.length==0) {
				if (opts.upsert) {
					$doc = $doc || query;
					$doc = self._tdb._cloneDeep($doc);
					updater.update($doc,true);
					if (_.isUndefined($doc._id))
						$doc._id = new self._tdb.ObjectID();
					self._put($doc, false, safe.sure(cb, function () {
						cb(null, 1,{updatedExisting:false,upserted:$doc._id,n:1})
					}))
				} else
					cb(null,0);
			} else {
				safe.forEachSeries(res, function (pos, cb) {
					self._get(pos, false, safe.sure(cb, function (obj) {
						// remove current version of doc from indexes
						_.each(self._idx,function(v,k) {
							v.del(obj,simplifyKey(obj._id));
						})
						var udoc = $doc;
						if (!$doc) {
							udoc = obj;
							updater.update(udoc);
						}
						udoc._id = obj._id;
						// put will add it back to indexes
						self._put(udoc, false, cb);
					}))
				}, safe.sure(cb,function () {
					cb(null, res.length, {updatedExisting:true,n:res.length});
				}))
			}
		}))
	},true,cb);
}

tcoll.prototype.findAndModify = function (query, sort, doc, opts, cb) {
	var self = this;
	if (_.isFunction(opts) && cb == null) {
		cb = opts;
		opts = {};
	}
	opts = opts || {};
	doc = doc || {};

	var updater = new Updater(doc, self._tdb);
	var $doc = updater.hasAtomic()?null:doc;

	var c = new tcursor(this,{}, opts.fields || {},{});
	c.sort(sort);
	if (c._err)
		return safe.back(cb,c._err);

	this._tq.add(function (cb) {
		self.__find(query, null, 0, 1, c._sort, opts.hint, {}, safe.sure(cb, function (res) {
			if (res.length==0) {
				if (opts.upsert) {
					$doc = $doc || query;
					$doc = self._tdb._cloneDeep($doc);
					updater.update($doc,true);
					if (_.isUndefined($doc._id))
						$doc._id = new self._tdb.ObjectID();
					self._put($doc, false, safe.sure(cb, function () {
						cb(null,opts.new?c._projectFields($doc):{})
					}))
				} else
					cb();
			} else {
				self._get(res[0], false, safe.sure(cb, function (obj) {
					var robj = (opts.new && !opts.remove) ? obj : self._tdb._cloneDeep(obj);
					// remove current version of doc from indexes
					_.each(self._idx,function(v,k) {
						v.del(obj,simplifyKey(obj._id));
					})
					var udoc = $doc;
					if (!$doc) {
						udoc = obj;
						updater.update(udoc);
					}
					udoc._id = obj._id;
					// put will add it back to indexes
					self._put(udoc, opts.remove?true:false, safe.sure(cb,function () {
						cb(null,c._projectFields(robj))
					}))
				}))
			}
		}))
	},true,cb);
}

tcoll.prototype.save = function (doc, opts, cb) {
	var self = this;
	cb = _.isFunction(doc)?doc:_.isFunction(opts)?opts:cb;
	cb = cb || function () {};
	doc = doc || {};
	opts = opts || {};
	this._tq.add(function (cb) {
		var res = doc;
		(function(cb) {
			if (_.isUndefined(doc._id)) {
				doc._id = new self._tdb.ObjectID();
				cb()
			} else {
				var id = simplifyKey(doc._id);
				var pos = self._store[id];
				// check if document with this id already exist
				if (pos) {
					// if so we need to fetch it to update index
					self._get(pos.pos, false, safe.sure(cb, function (oldDoc) {
						// remove current version of doc from indexes
						_.each(self._idx,function(v,k) {
							v.del(oldDoc,id);
						})
						res = 1;
						cb();
					}))
				} else cb();
			}
		})(safe.sure(cb, function () {
			self._put(doc, false, safe.sure(cb, function () {
				cb(null,res); // when update return 1 when new save return obj
			}))
		}))
	},true,cb);
}

tcoll.prototype.remove = function (query, opts, cb) {
	var self = this;
	if (_.isFunction(query)) {
		cb = query;
		query = opts = {};
	} else if (_.isFunction(opts)) {
		cb = opts;
		opts = {};
	}
	opts = opts || {};
	if (opts.w>0 && !_.isFunction(cb))
		throw new Error("Callback is required");
	cb = cb || function () {};
	var single = opts.single || false;
	this._tq.add(function (cb) {
		self.__find(query, null, 0, single ? 1 : null, null, opts.hint, {}, safe.sure(cb, function (res) {
			safe.forEachSeries(res, function (pos, cb) {
				self._get(pos, false, safe.sure(cb, function (obj) {
					self._put(obj,true,cb);
				}))
			}, safe.sure(cb, function () {
				cb(null,res.length);
			}))
		}))
	},true,cb);
}

tcoll.prototype.findAndRemove = function (query,sort,opts,cb) {
	var self = this;

	if (_.isFunction(sort) && cb == null && opts==null) {
		cb = sort;
		sort = {}
		opts = {};
	} else if (_.isFunction(opts) && cb == null) {
		cb = opts;
		opts = {};
	}
	opts = opts || {};
	sort = sort || {};

	var c = new tcursor(this,{},{},{});

	// Fix for mongoouse/tungus they pass sort as undefined
	c.sort(sort);

	if (c._err)
		return safe.back(cb,c._err);

	this._tq.add(function (cb) {
		self.__find(query, null, 0, 1, c._sort, opts.hint, {}, safe.sure(cb, function (res) {
			if (res.length==0)
				return cb();
			self._get(res[0], false, safe.sure(cb, function (obj) {
				self._put(obj,true,safe.sure(cb, function () {
					cb(null,obj);
				}))
			}))
		}))
	},true,cb);
}

tcoll.prototype._bestSortIndex = function (sort) {
	// no sort
	if (!sort) return null;
	// exact match
	if (this._idx[sort]) return this._idx[sort];
	// find potential sort indexes
	var pi = [];
	_.each(this._idx,function (idx) {
		var fields = idx.fields();
		var match = _.takeWhile(fields, function (kv, i) {
			return i < sort.length ? kv[0] == sort[i][0] : false;
		});
		if (match.length == sort.length) {
			var score = fields.length;
			_.each(sort,function (kv, i) {
				if (kv[1] != fields[i][1]) score += 1;
			});
			pi.push({ value: idx, score: score });
		}
	});
	if (pi.length === 0) return null;
	// select best index
	pi = pi.sort(function (l, r) { return l.score < r.score; });
	return pi[0].value;
};

function reduceIndexSet(pi) {
	var hit;
	do {
		hit = false;
		// compare each potential index with each other
		_.each(pi,function (v1, i1) {
			_.each(pi,function (v2, i2) {
				if (i1 == i2) return;
				// compare the set of possible keys for both indexes
				if (_.union(v1.k, v2.k).length == v1.k.length) {
					// key for v2 is a subset of key for v1, check equality
					if (v1.k.length == v2.k.length && v1.i.depth() > v2.i.depth()) {
						// keys are equal, but the depth of v2 is lower;
						// v2 is preferable, strike out v1
						pi.splice(i1, 1);
					} else {
						// in other two cases v1 is preferable, strike out v2
						pi.splice(i2, 1);
					}
					hit = true;
					return false;
				}
			});
			if (hit) return false;
		});
	} while (hit);
}

tcoll.prototype.__find = function (query, fields, skip, limit, sort, hint, arFields, cb) {
	var self = this;
	var range;
	// find sort index
	var si = this._bestSortIndex(sort);
	// for non empty query check indexes that we can use
	var qt = self._tdb.Finder.matcher(query);
	var pi = [];
	if (_.size(qt)>0) {
		_.each(self._idx,function (i) {
			var f = _.map(i.fields(), 0);
			var e = _.takeWhile(f, function (k) {
				return qt._ex(k) == 1 && (!hint || hint[k]);
			});
			if (e.length > 0) pi.push({ i: i, k: e, e: f.length > e.length });
		});
	}

	// if possible indexes found split the query and process
	// indexes separately
	if (!_.isEmpty(pi)) {
		// choose the most appropriate indexes
		reduceIndexSet(pi);
		// split query
		var io = {};
		_.each(pi,function (v) {
			_.each(v.k,function (k) {
				if (!io[k]) io[k] = qt.split(k);
			});
		});
		// process indexes
		var p = _.map(pi,function (st) {
			// this action applies to all indexes
			var r = io[st.k[0]]._index(st.i);
			// process subfields of compound index
			_.each(st.k.slice(1),function (k) {
				var v = io[k];
				r = _(r).map(function (si) { return v._index(si); }).flatten().value();
			});
			// expand subindexes to plain ids
			if (st.e) r = _(r).map(function (si) { return si.all(); }).flatten().value();
			// store result of index search
			return r;
		});
		if (p.length == 1) {
			p = p[0];
			// optimization for the case when search and sorting indexes are the same
			if (si && pi[0].i === si) {
				var sif = si.fields();
				if (_.every(sort, function (v, i) { return sif[i][1] == v[1]; })) {
					// sort order exactly matches index order,
					// so the result is already sorted
					sort = null;
				} else if (_.every(sort, function (v, i) { return sif[i][1] == -v[1]; })) {
					// sort order is exactly opposite to index order,
					// so the result is sorted, but in reverse direction
					p.reverse();
					sort = null;
				}
			}
		} else {
			// TODO: use sort index as intersect base to speedup sorting
			p = tutils.intersectIndexes(p);
		}
		// nowe we have ids, need to convert them to positions
		range = _.map(p,function (_id) {
			return self._store[_id].pos;
		})
	} else {
		if (si) {
			range = _.map(si.all(_.map(sort, 1)), function (_id) {
				return self._store[_id].pos;
			})
			//if (order==-1)
			//	range.reverse();
			sort = null;
		} else
			range = _(self._store).values().map('pos').value();
	}

	if (sort && si) {
		var ps = {};
		_.each(range, function (pos) {
			ps[pos] = true;
		});
		range = [];
		_.each(si.all(_.map(sort, 1)), function (_id) {
			var pos = self._store[_id].pos;
			if (_.has(ps, pos))
				range.push(pos);
		});
		//if (order == -1)
		//	range.reverse();
		sort = null;
	}

	// no sort, no query then return right away
	if (sort==null && (_.size(qt)==0 || qt._args.length==0)) {
		if (skip!=0 || limit!=null) {
			var c = Math.min(range.length-skip,limit?limit:range.length-skip);
			range = range.splice(skip,c)
		}
		return safe.back(cb,null,range);
	}

	// check if we can use simple match or array match function
	var arrayMatch = false;
	if (self._tdb._gopts.searchInArray)
		arrayMatch = true;
	else {
		_.each(qt.fields(), function (v,k) {
			if (_.get(arFields, k)) {
				arrayMatch = true;
				return false;
			}
		})
	}

	var matcher = new Function("obj", "return " + (arrayMatch ? qt.native3() : qt.native()));

	// create sort index
	if (sort) {
		si = new tindex(sort,self);
	}

	// now simple non-index search
	var res = [];
	var found = 0;
	safe.forEachSeries(range, function (pos, cb) {
		if (sort==null && limit && res.length>=limit)
			return safe.back(cb);
		self._get(pos, true, safe.sure(cb, function (obj) {
			if (matcher(obj)) {
				if (sort!=null || found>=skip) {
					if (sort==null)
						res.push(pos);
					else
						si.set(obj,pos);
				}
				found++;
			}
			cb()
		}))
	}, safe.sure(cb, function () {
		if (sort) {
			res = si.all();
			//if (order==-1) {
			//	res.reverse();
			//}
			if (skip!=0 || limit!=null) {
				var c = Math.min(res.length-skip,limit?limit:res.length-skip);
				res = res.splice(skip,c)
			}
		}
		cb(null, res);
	}))
}

tcoll.prototype._find = function (query, fields, skip, limit, sort_, hint, arFields, cb) {
	var self = this;
	this._tq.add(function (cb) {
		self.__find(query, fields, skip, limit, sort_, hint, arFields, cb);
	}, false, cb);
}

function code2fn(obj) {
	if (_.isObject(obj)) {
		_.each(obj,function (value, key) {
			if (value instanceof Code) {
				with (value.scope) {
					obj[key] = eval('(' + value.code + ')');
				}
			}
			else code2fn(value);
		});
	}
}

tcoll.prototype.mapReduce = function (map, reduce, opts, cb) {
	var self = this;
	if (_.isFunction(opts)) {
		cb = opts;
		opts = {};
	}

	if (!opts.out) return safe.back(cb, new Error('the out option parameter must be defined'));
	if (!opts.out.inline && !opts.out.replace) {
		return safe.back(cb, new Error('the only supported out options are inline and replace'));
	}

	code2fn(opts.scope);

	var m = {};

	function emit(k, v) {
		var values = m[k];
		if (!values) m[k] = [ v ];
		else {
			values.push(v);
			if (values.length > 1000) values = [ reduce(k, values) ];
		}
	}

	with (opts.scope || {}) {
		try {
			if (map instanceof Code) {
				with (map.scope) {
					map = eval('(' + map.code + ')');
				}
			} else map = eval('(' + map + ')');
			if (reduce instanceof Code) {
				with (reduce.scope) {
					reduce = eval('(' + reduce.code + ')');
				}
			} else reduce = eval('(' + reduce + ')');
			if (finalize instanceof Code) {
				with (finalize.scope) {
					finalize = eval('(' + finalize.code + ')');
				}
			} else var finalize = eval('(' + opts.finalize + ')');
		} catch (e) {
			return safe.back(cb,e);
		}
	}

	self.find(opts.query, null, { limit: opts.limit, sort: opts.sort }, safe.sure(cb, function (c) {
		var doc;
		safe.doUntil(
			function (cb) {
				c.nextObject(safe.trap_sure(cb, function (_doc) {
					doc = _doc;
					if (doc) map.call(doc);
					return cb();
				}));
			},
			function () {
				return doc === null;
			},
			safe.trap_sure(cb, function () {
				_.each(m,function (v, k) {
					v = v.length > 1 ? reduce(k, v) : v[0];
					if (finalize) v = finalize(k, v);
					m[k] = v;
				});

				var stats = {};
				if (opts.out.inline) return process.nextTick(function () {
					cb(null, _.values(m), stats); // execute outside of trap
				});

				// write results to collection
				safe.waterfall([
					function (cb) {
						self._tdb.collection(opts.out.replace, { strict: 1 }, function (err, col) {
							if (err) return cb(null, null);
							col.drop(cb);
						});
					},
					function (arg, cb) {
						self._tdb.collection(opts.out.replace, {}, cb);
					},
					function (col, cb) {
						var docs = _.map(m, function (value, key) {
							return {
								_id: key,
								value: value
							};
						});
						col.insert(docs, safe.sure(cb, function () {
							if (opts.verbose) cb(null, col, stats);
							else cb(null, col);
						}));
					}
				], cb);
			}
		)); // doUntil
	}));
};

tcoll.prototype.group = _.rest(function (keys, condition, initial, args) {
	var self = this;

	var callback = args.pop(),
		reduce = args.length ? args.shift() : null,
		finalize = args.length ? args.shift() : null,
		command = args.length ? args.shift() : null,
		options = args.length ? args.shift() : {};

	if (!_.isFunction(finalize)) {
		command = finalize;
		finalize = null;
	}

	code2fn(options.scope);

	with (options.scope || {}) {
		try {
			if (_.isFunction(keys)) keys = eval('(' + keys + ')');
			else if (keys instanceof Code) {
				with (keys.scope) {
					keys = eval('(' + keys.code + ')');
				}
			}
			if (reduce instanceof Code) {
				with (reduce.scope) {
					reduce = eval('(' + reduce.code + ')');
				}
			} else reduce = eval('(' + reduce + ')');
			if (finalize instanceof Code) {
				with (finalize.scope) {
					finalize = eval('(' + finalize.code + ')');
				}
			} else finalize = eval('(' + finalize + ')');
		} catch (e) {
			return callback(e);
		}
	}

	var m = {};
	self.find(condition, safe.sure(callback, function (c) {
		var doc;
		safe.doUntil(
			function (cb) {
				c.nextObject(safe.sure(cb, function (_doc) {
					doc = _doc;
					if (!doc) return cb();
					var keys2 = keys;
					if (_.isFunction(keys)) keys2 = keys(doc);
					if (!_.isArray(keys2)) {
						var keys3 = [];
						_.each(keys2,function (v, k) {
							if (v) keys3.push(k);
						});
						keys2 = keys3;
					}
					var key = {};
					_.each(keys2,function (k) {
						key[k] = doc[k];
					});
					var skey = JSON.stringify(key);
					var obj = m[skey];
					if (!obj) obj = m[skey] = _.extend({}, key, initial);
					try {
						reduce(doc, obj);
					} catch (e) {
						return cb(e);
					}
					cb();
				}));
			},
			function () {
				return doc === null;
			},
			safe.sure(callback, function () {
				var result = _.values(m);
				if (finalize) {
					_.each(result,function (value) {
						finalize(value);
					});
				}
				callback(null, result);
			})
		);
	}));
});
