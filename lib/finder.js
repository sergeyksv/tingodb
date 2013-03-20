var safe = require('safe');
var _ = require('underscore');

var ops = {
	$lt : {
		_get : function (obj) {
			return this._args[0]._get(obj) < this._args[1]._get(obj);
		}
	},
	$lte : {
		_get : function (obj) {
			return this._args[0]._get(obj) < this._args[1]._get(obj);
		}
	},
	$gt : {
		_get : function (obj) {
			return this._args[0]._get(obj) > this._args[1]._get(obj);
		}
	},
	$gte : {
		_get : function (obj) {
			return this._args[0]._get(obj) > this._args[1]._get(obj);
		}
	},
	$eq : {
		_get : function (obj) {
			return this._args[0]._get(obj) == this._args[1]._get(obj);
		}
	},
	$ne : {
		_get : function (obj) {
			return this._args[0]._get(obj) != this._args[1]._get(obj);
		}
	},
	$in : {
		_get : function (obj) {
			var c = this._args[0]._get(obj);
			for (var i=1; i<this._args.length; i++) {
				if (c == this._args[i]._get(obj))
					return true;
			}
			return false;			
		}
	},
	$nin : {
		_get : function (obj) {
			var c = this._args[0]._get(obj);
			for (var i=1; i<this._args.length; i++) {
				if (c == this._args[i]._get(obj))
					return false;
			}
			return true;			
		}
	},
	$all : {
		_get : function () {
			throw new Error("Unsupported");
		}
	},
	$not : {
		_get : function () {
			return !this._args[0]._get();
		}
	},
	$and : {
		_get : function (obj) {
			for (var i=0; i<this._args.length; i++) {
				if (!this._args[i]._get(obj))
					return false;
			}
			return true;
		}
	},
	$or : {
		_get : function (obj) {
			for (var i=0; i<this._args.length; i++) {
				if (this._args[i]._get(obj))
					return true;
			}
			return false;
		}
	},
	$nor : {
		_get : function () {
			for (i=0; i<this._args.length; i++) {
				if (this._args[i]._get())
					return false;
			}
			return true;
		}
	}	
}

function stree(query,ctx) {
	var _query = _(query);
	var args = [];
	if (_query.isArray()) {
		_query.forEach(function (v) {
			var sub = stree(v);
			_(sub).forEach(function (v) {
				args.push(v);
			})
		})
	} else if (_query.isObject()) {
		_query.forEach(function (v,k) {
			var op = ops[k];
			var n = null;		
			if (op!=null) {
				n = {_args:stree(v),
					_get:op._get
				}
				if (ctx!=null)
					n._args.splice(0,0,ctx);
				args.push(n);					
			}
			else {
				if (_(v).isObject()) {
					var sub = stree(v,{_get:function (obj) {return obj._get(k)}});
					_(sub).forEach(function (v) {
						args.push(v);
					})					
				} else {
					n = {_args:[
						{_get:function (obj) {return obj._get(k)}}
						],
						_get:ops.$eq._get}
					var sub = stree(v);
					_(sub).forEach(function (v) {
						n._args.push(v);
					})						
					args.push(n);						
				}
			}
		})
	} else {
		return [{_get:function () { return query; }}];
	}
	return args;
}

function sget(obj, k) {
	var i=0; var p = obj;
	var path = k.split(".");
	for (;i<path.length;i++) {
		p=p[path[i]];
		if (!p)
			break;
	}
	if (p != undefined && i==path.length)
		return p;
	else
		return null;
}

module.exports.matcher = function (query) {
	if (_(query).size()==0) 
		return function () { return true; }	
	var aq = [];
	_(query).each(function (v,k) {
		o = {};	o[k]=v;
		aq.push(o)
	})
	var wrap={$and:aq};
	var res = stree(wrap);
	
	return function (obj) {
		var m = false;
		try {
			var o = {_get:function (key) { return sget(obj,key); }};
			m = res[0]._get(o);
		} catch (e) {
			console.log(e);
		}
		return m;
	}
}
