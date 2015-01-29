var finder = require('../lib/finder');
var Benchmark = require('benchmark');
var _ = require('underscore');

var suite = new Benchmark.Suite();
suite.on('cycle', function(event) {
  console.log(String(event.target));
})

var f1 = "sn1 = function (obj) { return "+ (new finder.field("d1.d2.s1.s2")).native() + " }";
var f2 = "sn2 = function (obj) { return "+ (new finder.field("d1.d2.s1.s2")).native2() + " }";
var f3 = "sn3 = function (obj) { return "+ (new finder.field("d1.d2.s1.s2")).native3() + " }";
var f4 = "sn4 = function (obj) { return "+ (new finder.field("d1.d2")).native3() + " }";
var f5 = "sn5 = function (obj) { return "+ (new finder.field("d1.d2")).native() + " }";
console.log(f4);
eval(f1);
eval(f2);
eval(f3);
eval(f4);
eval(f5);
var obj1 = {num:1, d1:{d2:{s1:{s2:-0.2}}}, t:15};
var obj2 = {num:1, d1:{d2:[{s1:{s2:-0.2}}]}, t:15};

console.log(sn1(obj1));
console.log(sn2(obj2));
console.log(sn3(obj2));

suite.add({name:"simple obj getter", defer:false, fn:function(next) {
	return sn1(obj1);
}});
suite.add({name:"recursive array getter", defer:false, fn:function(next) {
	return sn2(obj2);
}});
suite.add({name:"flat array getter", defer:false, fn:function(next) {
	return sn3(obj2);
}});
suite.add({name:"flat obj getter", defer:false, fn:function(next) {
	return sn3(obj1);
}});
suite.add({name:"flat 1 getter", defer:false, fn:function(next) {
	return sn4(obj1);
}});
suite.add({name:"simple 1 getter", defer:false, fn:function(next) {
	return sn5(obj1);
}});

suite.run({async:true,maxTime:240});
