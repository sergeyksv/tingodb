module.exports = function (opts) {
	var db = require('./tdb.js');
	function gdb (path,optsLocal) {
		gdb.superclass.constructor(path,optsLocal,opts)
		if (opts.nativeObjectID)
			this.ObjectID = require("mongodb").ObjectID;
		else
			this.ObjectID = require("./ObjectId");
		this.Finder = require("./finder")(this);		
	}
	var F = function() { }
    F.prototype = db.prototype
    gdb.prototype = new F()
    gdb.prototype.constructor = gdb
    gdb.superclass = db.prototype	
	return {
		Db:gdb,
		Collection:require('./tcoll.js'),
		ObjectID:opts.nativeObjectID?require("mongodb").ObjectID:require("./ObjectId")
	}
}
