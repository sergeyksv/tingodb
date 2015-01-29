var finder = require('../lib/finder');
var Benchmark = require('benchmark');

var suite = new Benchmark.Suite();
suite.on('cycle', function(event) {
  console.log(String(event.target));
})

var qt = finder.matcher({"data.sin":{$gt:0,$lt:0.5},t:15});

var f = "sn = function (obj) { return "+ qt.native() + " }";
eval(f);
var obj = {num:1, data:{sin:-0.2}, t:15};
var obj1 = {num:1, data:[{sin:-0.2}], t:15};
console.log(sn(obj));
console.log(sn.toString());
var matcher = function (obj) {
	var m = false;
	try {
		var o = {_get:function (key) { return obj[key]; }};
		m = qt._get(o);
	} catch (e) {
		console.log(e);
	}
	return m;
}

suite.add({name:"matcher", defer:false, fn:function(next) {
	return matcher(obj);
}});
suite.add({name:"seminative", defer:false, fn:function(next) {
	return sn(obj);
}});
suite.add({name:"native", defer:false, fn:function(next) {
	return obj.data.sin>0 && obj.data.sin<0.5 && obj.t==15;
}});
suite.run({async:true,maxTime:240});
