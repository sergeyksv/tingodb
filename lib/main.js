var safe = require('safe');
var tdb = require('./tdb.js');

module.exports.open = function (dbpath, options, cb) {
	var db = new tdb();
	db.init(dbpath,options,safe.sure(cb, function () {
		cb(null, db) 
	}))
}
