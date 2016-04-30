var _ = require('lodash')

module.exports.intersectIndexes = function (indexes,base) {
	// do intersection of indexes using hashes
	var ops = [], i = 0;
	// convert to hashes
	for (i=0; i<indexes.length; i++) {
		var ids = {};
		_.each(indexes[i], function (id) {
			ids[id]=id;
		})
		ops.push(ids);
	}
	// find minimal one
	if (_.isUndefined(base)) {
		base = 0;
		for (i=0; i<ops.length; i++) {
			if (ops[i].length<ops[base].length)
				base = i;
		}
	}
	// iterate over it
	var m = [];
	_.each(indexes[base], function (id) {
		var match = true;
		for (var i=0; i<ops.length; i++) {
			if (i==base) continue;
			if (!ops[i][id]) {
				match = false;
				break;
			}
		}
		if (match)
			m.push(id);
	})
	return m;
}
