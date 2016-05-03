function tdb() {}

module.exports = tdb;

tdb.prototype.init = function (path, options, cb) {
	this._path = path;
	cb(null);
};
