var _ = require('lodash');

function ObjectID(val) {
	this.$oid = NaN;
	this.init(val);
}

ObjectID.prototype.init = function (val) {
	if (_.isNumber(val))
		this.$oid = val;	
	else if (_.isString(val))
		this.$oid = parseInt(val)
	else if (val instanceof ObjectID)
		this.$oid = val.$oid;
	if (val && isNaN(this.$oid))
		throw new Error("ObjectId should be ObjectId (whatever it is designed to be)")
}

ObjectID.prototype.toString = ObjectID.prototype.toJSON = ObjectID.prototype.inspect = ObjectID.prototype.toHexString = function () {
	return this.$oid.toString();
}

module.exports = ObjectID;

