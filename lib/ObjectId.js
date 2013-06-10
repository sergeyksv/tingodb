var _ = require('lodash');

var inproc_id = -1;
function ObjectID(val) {
	// every new instance will have temporary inproc unique value
	// minus sign will let know to db layer that value is temporary
	// and need to be replaced
	this.$oid = --inproc_id;
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

ObjectID.prototype.toString = ObjectID.prototype.inspect = function () {
	return this.$oid.toString();
}

ObjectID.prototype.toJSON = ObjectID.prototype.valueOf = function () {
	return this.$oid;
}

// Something for fake compatibiltity with BSON.ObjectId
ObjectID.prototype.toHexString = function () {
	var l = this.$oid.toString();
	var zeros = "000000000000000000000000";	
	return zeros.substr(0,zeros.length - l.length)+l;
}
		
module.exports = ObjectID;

