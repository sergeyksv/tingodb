module.exports = function (optsGlobal) {
	var db = require('./tdb.js');
	function gdb (path,optsLocal) {
		gdb.superclass.constructor(path,optsLocal,optsGlobal)
//		this.ObjectID = require("mongodb").ObjectID;
		this.ObjectID = require("./ObjectId");
	}
	var F = function() { }
    F.prototype = db.prototype
    gdb.prototype = new F()
    gdb.prototype.constructor = gdb
    gdb.superclass = db.prototype	
	return {
		Db:gdb,
		Collection:require('./tcoll.js'),
//		ObjectID:require("mongodb").ObjectID
		ObjectID:require("./ObjectId")
	}
}
