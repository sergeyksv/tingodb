var _ = require('lodash');

var tdb = null;
var expo = {};
module.exports = function (tdb_) {
	tdb=tdb_;
	return expo;
};

var ops = {
	$range : function () {
		this.op = "$range";
		this.dump = function () {
			return this._args[0].dump() +" in range (" +this._args[1].dump()+","+this._args[2].dump()+","+this._args[3].dump()+","+this._args[4].dump()+")";
		},
		this._index = function (index) {
			if (index.order > 0) return index.range(this._args[1]._get(), this._args[2]._get(), this._args[3]._get(), this._args[4]._get());
			else return index.range(this._args[2]._get(), this._args[1]._get(), this._args[4]._get(), this._args[3]._get());
		}
	},

	$lt : function () {
		this.op = "$lt";
		this._index = function (index) {
			if (index.order > 0) return index.range(null, this._args[1]._get(), false, true);
			else return index.range(this._args[1]._get(), null, true, false);
		};
		this._ex = function (f) {
			if (this.fields()[f]!=null)
				return 1;
			else
				return -1;
		}
		this.dump = function () {
			return this._args[0].dump() +" < " +this._args[1].dump()
		}
		this.native = function () {
			return this._args[0].native() +" < " +this._args[1].native()
		}
		this.native3 = function () {
			var s = "(function () {\n";
			s+= "var v = "+this._args[0].native3()+"\n";
			s+= "if (!v) return false;\n";
			s+= "for (var i=0; i<v.length; i++ ) {\n";
			s+= "if (v[i]<"+ this._args[1].native3()+") return true;\n"
			s+= "}\nreturn false;})()";
			return s;
		}
	},
	$lte : function () {
		this.op = "$lte";
		this._index = function (index) {
			if (index.order > 0) return index.range(null, this._args[1]._get(), false, false);
			else return index.range(this._args[1]._get(), null, false, false);
		};
		this._ex = function (f) {
			if (this.fields()[f]!=null)
				return 1;
			else
				return -1;
		}
		this.dump = function () {
			return this._args[0].dump() +" <= " +this._args[1].dump()
		}
		this.native = function () {
			return this._args[0].native() +" <= " +this._args[1].native()
		}
		this.native3 = function () {
			var s = "(function () {\n";
			s+= "var v = "+this._args[0].native3()+"\n";
			s+= "if (!v) return false;\n";
			s+= "for (var i=0; i<v.length; i++ ) {\n";
			s+= "if (v[i]<="+ this._args[1].native3()+") return true;\n"
			s+= "}\nreturn false;})()";
			return s;
		}
	},
	$gt : function () {
		this.op = "$gt";
		this._index = function (index) {
			if (index.order > 0) return index.range(this._args[1]._get(), null, true, false);
			else return index.range(null, this._args[1]._get(), false, true);
		};
		this._ex = function (f) {
			if (this.fields()[f]!=null)
				return 1;
			else
				return -1;
		}
		this.dump = function () {
			return this._args[0].dump() +" > " + this._args[1].dump()
		}
		this.native = function () {
			return this._args[0].native() +" > " +this._args[1].native()
		}
		this.native3 = function () {
			var s = "(function () {\n";
			s+= "var v = "+this._args[0].native3()+"\n";
			s+= "if (!v) return false;\n";
			s+= "for (var i=0; i<v.length; i++ ) {\n";
			s+= "if (v[i]>"+ this._args[1].native3()+") return true;\n"
			s+= "}\nreturn false;})()";
			return s;
		}
	},
	$gte : function () {
		this.op = "$gte";
		this._get = function (obj) {
			return this._args[0]._get(obj) >= this._args[1]._get(obj);
		}
		this._index = function (index) {
			if (index.order > 0) return index.range(this._args[1]._get(), null, false, false);
			else return index.range(null, this._args[1]._get(), false, false);
		};
		this._ex = function (f) {
			if (this.fields()[f]!=null)
				return 1;
			else
				return -1;
		}
		this.dump = function () {
			return this._args[0].dump() +" >= " + this._args[1].dump()
		}
		this.native = function () {
			return this._args[0].native() +" >= " +this._args[1].native()
		}
		this.native3 = function () {
			var s = "(function () {\n";
			s+= "var v = "+this._args[0].native3()+"\n";
			s+= "if (!v) return false;\n";
			s+= "for (var i=0; i<v.length; i++ ) {\n";
			s+= "if (v[i]>="+ this._args[1].native3()+") return true;\n"
			s+= "}\nreturn false;})()";
			return s;
		}
	},
	$exists : function () {
		this.op = "$exists";
		this._index = function (index) {
			if (this._args[1]._get())
				return index.values();
			else
				return index.nuls();
		},
		this._ex = function (f) {
			if (this.fields()[f]!=null)
				return 1;
			else
				return -1;
		}
		this.dump = function () {
			return this._args[0].dump() + " exists " + this._args[1].dump()
		}
		this.native = function () {
			var v = this._args[0].native();
			return this._args[1]._get()?v:"!"+v;
		}
		this.native3 = function () {
			var exists = this._args[1]._get();
			var s = "(function () {\n";
			s+= "var v = "+this._args[0].native3()+"\n";
			s+= "if (!v) return "+!exists+";\n";
			s+= "for (var i=0; i<v.length; i++ ) {\n";
			if (exists)
				s+= "if (!v[i]) return false;\n"
			else
				s+= "if (v[i]) return false;\n"
			s+= "}\nreturn true;})()";
			return s;
		}
	},
	$eq : function () {
		this.op = "$eq";
		this._index = function (index) {
			return index.match(this._args[1]._get());
		},
		this._ex = function (f) {
			if (this.fields()[f]!=null)
				return 1;
			else
				return -1;
		}
		this.dump = function () {
			return this._args[0].dump() +" = " + this._args[1].dump()
		}
		this.native = function () {
			return this._args[0].native() +" === " +this._args[1].native()
		}
		this.native3 = function () {
			var s = "(function () {\n";
			s+= "var v = "+this._args[0].native3()+"\n";
			s+= "if (!v) return false;\n";
			s+= "for (var i=0; i<v.length; i++ ) {\n";
			s+= "if (v[i]==="+ this._args[1].native3()+") return true;\n"
			s+= "}\nreturn false;})()";
			return s;
		}
	},
	$ne : function () {
		this.op = "$ne";
		this._index = function (index) {
			var m = index.match(this._args[1]._get());
			var a = index.all();
			return _.difference(a,m);
		},
		this._ex = function (f) {
			if (this.fields()[f]!=null)
				return 1;
			else
				return -1;
		}
		this.dump = function () {
			return this._args[0].dump() +" != " + this._args[1].dump()
		}
		this.native = function () {
			return this._args[0].native() +" != " +this._args[1].native()
		}
		this.native3 = function () {
			var s = "(function () {\n";
			s+= "var v = "+this._args[0].native3()+"\n";
			s+= "if (!v) return true;\n";
			s+= "for (var i=0; i<v.length; i++ ) {\n";
			s+= "if (v[i]=="+ this._args[1].native3()+") return false;\n"
			s+= "}\nreturn true;})()";
			return s;
		}
	},
	$in : function () {
		this.op = "$in";
		this._index = function (index) {
			var u = _(this._args.slice(1)).map(function (v) { return v._get(); }).uniq().value();
			return _(u).map(function (v) { return index.match(v); }).flattenDeep().value();
		},
		this._ex = function (f) {
			if (this.fields()[f]!=null)
				return 1;
			else
				return -1;
		}
		this.dump = function (obj) {
			var s = this._args[0].dump()+" in [";
			for (var i=1; i<this._args.length; i++) {
				s+=this._args[i].dump();
				if (i!=this._args.length-1)
					s+=",";
			}
			return s+"]";
		}
		this.native = function (obj) {
			if (this._args.length==1)
				return "true";
			var s = "(function (obj) {\n"
			s+="var v = "+this._args[0].native()+";\n";
			s+="var args=[";
			for (var i=1; i<this._args.length; i++) {
				s+=JSON.stringify(this._args[i]._get());
				if (i!=this._args.length-1)
					s+=",";
			}
			s+="];\n"
			s+="for (var i=0; i<args.length; i++) { if (args[i]==v) return true };\n"
			s+="return false;\n"
			s+="})(obj)";
			return s;
		}
		this.native3 = function (obj) {
			if (this._args.length==1)
				return "false";
			var s = "(function (obj) {\n"
			s+="var v = "+this._args[0].native3()+";\n";
			s+= "if (!v) return false;\n";
			s+="var args=[";
			for (var i=1; i<this._args.length; i++) {
				s+=JSON.stringify(this._args[i]._get());
				if (i!=this._args.length-1)
					s+=",";
			}
			s+="];\n"
			s+= "for (var i=0; i<v.length; i++ ) {\n";
			s+="for (var j=0; j<args.length; j++) { if (args[j]==v[i]) return true };\n"
			s+="};return false;\n"
			s+="})(obj)";
			return s;
		}
	},
	$nin : function () {
		this.op = "$nin";
		this._index = function (index) {
			var u = _(this._args.slice(1)).map(function (v) { return v._get(); }).uniq().value();
			var x = _(u).map(function (v) { return index.match(v); }).flattenDeep().value();
			var a = index.all(null, true);
			return _.difference(a, x);
		},
		this._ex = function (f) {
			if (this.fields()[f]!=null)
				return 1;
			else
				return -1;
		}
		this.dump = function (obj) {
			var s = this._args[0].dump()+" nin [";
			for (var i=1; i<this._args.length; i++) {
				s+=this._args[i].dump();
				if (i!=this._args.length-1)
					s+=",";
			}
			return s+"]";
		}
		this.native = function (obj) {
			if (this._args.length==1)
				return "true";
			var s = "(function (obj) {\n"
			s+="var v = "+this._args[0].native()+";\n";
			s+="var args=[";
			for (var i=1; i<this._args.length; i++) {
				s+=JSON.stringify(this._args[i]._get());
				if (i!=this._args.length-1)
					s+=",";
			}
			s+="];\n"
			s+="for (var i=0; i<args.length; i++) { if (args[i]==v) return false };\n"
			s+="return true;\n"
			s+="})(obj)";
			return s;
		}
		this.native3 = function (obj) {
			if (this._args.length==1)
				return "true";
			var s = "(function (obj) {\n"
			s+="var v = "+this._args[0].native3()+";\n";
			s+= "if (!v) return true;\n";
			s+="var args=[";
			for (var i=1; i<this._args.length; i++) {
				s+=JSON.stringify(this._args[i]._get());
				if (i!=this._args.length-1)
					s+=",";
			}
			s+="];\n"
			s+= "for (var i=0; i<v.length; i++ ) {\n";
			s+="for (var j=0; j<args.length; j++) { if (args[j]==v[i]) return false };\n"
			s+="};return true;\n"
			s+="})(obj)";
			return s;
		}
	},
	$all : function () {
		this.op = "$all";
		this._ex = function (f) {
			// check if our args do not have regexps,
			// if so index can't be used
			for (var i=1; i<this._args.length; i++) {
				if (_.isRegExp(this._args[i]._get()))
					return 0;
			}
			if (this.fields()[f]!=null)
				return 1;
			else
				return -1;
		}
		this._index = function (index) {
			var m = [];
			for (var i=1; i<this._args.length; i++) {
				if (i==1)
					m = index.match(this._args[i]._get());
				else
					m = _.intersection(m,index.match(this._args[i]._get()));
			}
			return m;
		},
		this.native = function (obj) {
			if (this._args.length==1)
				return "true";
			var val,s = "(function (obj) {\n"
			s+="var v = "+this._args[0].native()+";\n";
			s+= "if (!v) return false;\n";
			s+="var args=[";
			for (var i=1; i<this._args.length; i++) {
				val=this._args[i]._get();
				if(val instanceof RegExp){
					s+=val;
				} else {
					s+=JSON.stringify(val);
				}
				if (i!=this._args.length-1)
					s+=",";
			}
			s+="];\n"
			s+= "for (var i=0; i<args.length; i++ ) {\n";
			s+= "if (args[i] instanceof RegExp)\n"
			s+="for (var j=0; j<v.length; j++) { if (args[i].test(v[j])) break; }\n"
			s+= "else\n"
			s+="for (var j=0; j<v.length; j++) { if (v[j]==args[i]) break; };\n"
			s+="if (j==v.length) return false;\n";
			s+="};return true;\n"
			s+="})(obj)";

			return s;
		},
		this.native3 = function (obj) {
			if (this._args.length==1)
				return "true";
			var val,s = "(function (obj) {\n"
			s+="var vv = "+this._args[0].native3()+";\n";
			s+= "if (!vv) return false;\n";
			s+= "if (!Array.isArray(vv[0])) vv=[vv];\n";
			s+="var args=[";
			for (var i=1; i<this._args.length; i++) {
				val=this._args[i]._get();
				if(val instanceof RegExp){
					s+=val;
				} else {
					s+=JSON.stringify(val);
				}
				if (i!=this._args.length-1)
					s+=",";
			}
			s+="];\n"
			s+="for (var k=0; k<vv.length; k++) {\n";
			s+="var v = vv[k];\n"
			s+="for (var i=0; i<args.length; i++ ) {\n";
			s+= "if (args[i] instanceof RegExp)\n"
			s+="for (var j=0; j<v.length; j++) { if (args[i].test(v[j])) break; }\n"
			s+= "else\n"
			s+="for (var j=0; j<v.length; j++) { if (v[j]==args[i]) break; };\n"
			s+="if (j==v.length) return false;\n";
			s+="}};return true;\n"
			s+="})(obj)";
			return s;
		}
	},
	$not : function () {
		this.op = "$not";
		this._index = function (index) {
			var m = this._args[1]._index(index);
			var a = index.all();
			return _.difference(a,m);
		},
		this._ex = function (f) {
			return this._args[1]._ex(f);
		}
		this.dump = function () {
			return "!("+this._args[1].dump()+")";
		}
		this.native = function () {
			return "!("+this._args[1].native()+")";
		}
		this.native3 = function () {
			return "!("+this._args[1].native3()+")";
		}
	},
	$and : function () {
		this.op = "$and";
		this._index = function (index) {
			var ops = [];
			for (var i=0; i<this._args.length; i++) {
				ops.push(this._args[i]._index(index));
			}
			return _.intersection.apply(_,ops);
		}
		this.split = function (f) {
			var s = new ops.$and();
			var d = [];
			var o = [];
			var i = 0;

			for (i=0; i<this._args.length; i++) {
				if (this._args[i].op=="$and" || this._args[i].op=="$or" ) {
					var sub = this._args[i].split(f);
					if (this._args[i]._args.length>0) {
						_.each(this._args[i]._args, function (a) {
							o.push(a);
						});
					}
					if (sub._args.length>0) {
						_.each(sub._args, function (a) {
							d.push(a);
						});
					}
				} else {
					var ex = this._args[i]._ex(f);
					if (ex==1)
						d.push(this._args[i])
					else
						o.push(this._args[i])
				}
			}
			// special check for paired ops
			var l=null,g=null;
			for (i=0; i<d.length; i++) {
				if (d[i].op=="$lt" || d[i].op=="$lte")
					l = i;
				if (d[i].op=="$gt" || d[i].op=="$gte")
					g = i;
			}
			if (l!=null && g!=null) {
				var r = new ops.$range();
				r._args = [d[l]._args[0]];
				r._args.push(d[g]._args[1]);
				r._args.push(d[l]._args[1]);
				r._args.push(new value(d[g].op=="$gte"?false:true));
				r._args.push(new value(d[l].op=="$lte"?false:true));
				d.splice(l,1);
				d.splice(l<g?g:g-1,1);
				d.push(r);
			}
			s._args = d;
			this._args = o;
			return s;
		}
		this._ex = function (f) {
			var r = -1;
			for (var i=0; i<this._args.length; i++) {
				var ex = this._args[i]._ex(f);
				if (ex==0)
					return 0;
				if (ex==1)
					r=1;
			}
			return r;
		}
		this.dump = function (obj) {
			var s = "("
			for (var i=0; i<this._args.length; i++) {
				s+=this._args[i].dump();
				if (i!=this._args.length-1)
					s+=",";
			}
			return s+")";
		}
		this.native = function (obj) {
			if (this._args.length==0)
				return "true";
			var s = "("
			for (var i=0; i<this._args.length; i++) {
				s+=this._args[i].native();
				if (i!=this._args.length-1)
					s+=" && ";
			}
			return s+")";
		}
		this.native3 = function (obj) {
			if (this._args.length==0)
				return "true";
			var s = "("
			for (var i=0; i<this._args.length; i++) {
				s+=this._args[i].native3();
				if (i!=this._args.length-1)
					s+=" && ";
			}
			return s+")";
		}
	},
	$or : function () {
		this.op = "$or";
		this._ex = function (f) {
			return 0;
		}
		this.dump = function (obj) {
			var s = "("
			for (var i=0; i<this._args.length; i++) {
				s+=this._args[i].dump();
				if (i!=this._args.length-1)
					s+=" || ";
			}
			return s+")";
		}
		this.native = function (obj) {
			if (this._args.length==0)
				return "true";
			var s = "("
			for (var i=0; i<this._args.length; i++) {
				s+=this._args[i].native();
				if (i!=this._args.length-1)
					s+=" || ";
			}
			return s+")";
		}
		this.native3 = function (obj) {
			if (this._args.length==0)
				return "true";
			var s = "("
			for (var i=0; i<this._args.length; i++) {
				s+=this._args[i].native3();
				if (i!=this._args.length-1)
					s+=" || ";
			}
			return s+")";
		}
	},
	$nor : function () {
		this.op = "$nor";
		this._ex = function (f) {
			return 0;
		}
		this.dump = function (obj) {
			var s = "!("
			for (var i=0; i<this._args.length; i++) {
				s+=this._args[i].dump();
				if (i!=this._args.length-1)
					s+=" || ";
			}
			return s+")";
		}
		this.native = function (obj) {
			if (this._args.length==0)
				return "true";
			var s = "!("
			for (var i=0; i<this._args.length; i++) {
				s+=this._args[i].native();
				if (i!=this._args.length-1)
					s+=" || ";
			}
			return s+")";
		}
		this.native3 = function (obj) {
			if (this._args.length==0)
				return "true";
			var s = "!("
			for (var i=0; i<this._args.length; i++) {
				s+=this._args[i].native3();
				if (i!=this._args.length-1)
					s+=" || ";
			}
			return s+")";
		}
	},
	$where: function () {
		this.op = "$where";
		this._ex = function (f) {
			return 0;
		};
		this.dump = function (obj) {
			return "where(" + this._args[0].dump() + ")";
		};
		this.native = this.native3 = function (obj) {
			var v = this._args[0]._get();
			if (_.isFunction(v)) return '(' + v + ').call(obj)';
			else if (v instanceof tdb.Code) {
				if (_.isFunction(v.code)) return '(function () { with (' + JSON.stringify(v.scope) + ') return (' + v.code + ').call(obj); })()';
				else return '(function () { with (' + JSON.stringify(v.scope) + ') return (' + v.code + '); }).call(obj)';
			} else return '(function () { return (' + v + '); }).call(obj)';
		};
	},
	$regex: function () {
		this.op = "$regex";
		this._ex = function (f) {
			return 0;
		};
		this._get = function (obj) {
			var match = this._args[0].native();
			match = match.replace(/\"/g,"");
			var opts = this._args.length>1?this._args[1].native():'';
			opts = opts.replace(/\"/g,"");
			return new RegExp(match,opts);
		}
		this.dump = function () {
			return this._args[0].dump() +" ~ " + this._args[1].dump()
		}
		this.native = function () {
			var match = this._args[1].native();
			match = match.replace(/\/+/g, "\\/").replace(/\\\\/g,"\\").replace(/^\"/,"/").replace(/\"$/,"/");
			var opts = this._args.length>2?this._args[2].native():'';
			opts = opts.replace(/\"/g,"");
			var s = '('+match+opts+').test('+this._args[0].native() +')';
			return s;
		}
		this.native3 = function () {
			var match = this._args[1].native();
			match = match.replace(/\/+/g, "\\/").replace(/\\\\/g,"\\").replace(/^\"/,"/").replace(/\"$/,"/");
			var opts = this._args.length>2?this._args[2].native():'';
			opts = opts.replace(/\"/g,"");
			var s = "(function () {\n";
			s+= "var v = "+this._args[0].native3()+"\n";
			s+= "if (!v) return false;\n";
			s+= "for (var i=0; i<v.length; i++ ) {\n";
			s+= 'if (('+match+opts+').test(v[i])) return true;\n'
			s+= "}\nreturn false;})()";
			return s;
		}
	}
}

_.each(ops, function (op) {
	op.prototype.fields = function () {
		var f = {};
		_.each(this._args, function (a) {
			if (a.op=="f")
				f[a.f]=1;
			else if (a.op!="v") {
				var s = a.fields();
				_.each(s,function (n,k) {
					f[k]=1;
				})
			}
		})
		return f;
	}
})

var field = function (k) {
	this.op = "f";
	this.f = k;
	this.dump = function () {
		return k;
	}
	this.native = function () {
		var path = k.split(".");
		var s = "(";
		var ps = "obj";
		for (var i=0;i<path.length;i++) {
			ps+="['"+path[i]+"']";
			if (i!=path.length-1)
				s+=ps+ " && ";
			else
				s+= "("+ps+" && "+ps+".valueOf()"+") )";
		}
		return s;
	}
	this.native3 = function () {
		function steep(ps, path, i) {
			ps+="['"+path[i]+"']";
			if (path.length==i)
				return "";
			i++;
			var s = "";
			s+= "v"+i+"="+ps+"\n";
			s+= "if (!v"+i+") return null;\n"
			if (path.length!=i) {
				s+= "if (Array.isArray(v"+i+")) {\n";
				s+= "for (i"+i+"=0; i"+i+"<v"+i+".length; i"+i+"++) {\n";
				s+= steep("v"+i+"[i"+i+"]",path,i);
				s+= "}\n}\nelse\n{\n";
				s+= steep("v"+i,path,i);
				s+= "}\n"
			} else
				s+= "res.push(v"+i+".valueOf())\n";
			return s;
		}

		var path = k.split(".");
		if (path.length==1)
			return "Array.isArray(obj['"+k+"'])?obj['"+k+"']:[obj['" + k + "'] && obj['"+k+"'].valueOf()]";
		else {
			var s = "(function () {\n";
			var ps = "obj";
			s+="var res=[]\n";
			for (var i=0; i<path.length; i++) {
				s+="var v"+i+",i"+i+";\n";
			}
			s+=steep(ps,path,0);
			return s+"\nreturn [].concat.apply([],res);})()";
		}

	}
}


var value = function (v) {
	this.op = "v";
	this.v = null;

	if (_.isNumber(v) || _.isString(v) || _.isFunction(v) || _.isBoolean(v))
		this.v = v
	else if (_.isObject(v) && v.valueOf)
		this.v = v.valueOf();
	else if (v !== null)
		console.log("WARNING: Using unrecognized value in query",v)

	this._get = function (obj) {
		return this.v;
	}
	this.dump = function () {
		return this.v;
	}
	this.native3 = this.native = function () {
		return _.isFunction(this.v) ? this.v.toString() : JSON.stringify(this.v);
	}
}

function stree(query,ctx,opf) {
	var args = [];
	if (_.isArray(query)) {
		_.each(query, function (v) {
			var sub = stree(v);
			_.each(sub,function (v) {
				args.push(v);
			})
		})
	} else if (_.isPlainObject(query)) {
		var options = null;
		_.each(query, function (v,k) {
			var op = ops[k];
			var n = null;
			if (op!=null) {
				n = new op();
				n._args = stree(v,ctx,k);
				if (ctx!=null)
					n._args.splice(0,0,ctx);
				if (options) {
					n._args.push(options);
					options = null;
				}
				args.push(n);
			}
			else if (k=="$options") {
				if (args.length>0)
					args[args.length-1]._args.push(new value(v))
				options = new value(v);
			}
			else {
				if (_.isPlainObject(v)) {
					var sub = stree(v,new field(k));
					if (sub.length==1)
						args.push(sub[0]);
					else if (sub.length>1) {
						n = new ops["$and"];
						n._args = sub;
						args.push(n);
					}
				} else if (_.isRegExp(v)) {
					n = new ops["$regex"]();
					n._args = [new field(k), new value(v.source), new value((v.global?'g':'')+(v.ignoreCase?'i':'')+(v.multiline?'m':''))];
					args.push(n);
				}
				else {
					n = new ops["$eq"]();
					n._args = [new field(k)];
					var sub = stree(v);
					_.each(sub,function (v) {
						n._args.push(v);
					})
					args.push(n);
				}
			}
		})
	} else {
		if (_.isRegExp(query)) {
			var v, n;
			if (opf!="$regex") {
				v = query;
				n = new ops["$regex"]();
				n._args = [new value(v.source), new value((v.global?'g':'')+(v.ignoreCase?'i':'')+(v.multiline?'m':''))];
				if (ctx!=null)
					n._args.splice(0,0,ctx);
				args.push(n);
			} else {
				v = query.toString();
				return [new value(v.substring(1,v.length-1))]
			}
		} else
			return [new value(query)];
	}
	return args;
}

expo.matcher = function (query) {
	var aq = [];
	_.each(query, function (v,k) {
		var o = {};	o[k]=v;
		aq.push(o)
	})
	var wrap={$and:aq};
	var res = stree(wrap);
	return res[0];
}

expo.field = field;
