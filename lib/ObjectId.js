var _ = require('lodash');

function ObjectID(val) {
	this.$oid = -1;
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

ObjectID.prototype.toString = ObjectID.prototype.toJSON = ObjectID.prototype.inspect = function () {
	return this.$oid.toString();
}

// Something for fake compatibiltity with BSON.ObjectId
ObjectID.prototype.toHexString = function () {
	var l = this.$oid.toString();
	var zeros = "000000000000000000000000";	
	return zeros.substr(0,zeros.length - l.length)+l;
}
		
module.exports = ObjectID;

