var libpath = process.env['TINGODB_COV'] ? '../lib-cov' : '../lib';
var tingodb = require(libpath + '/main')({});
var tutils = require('./utils');

var config = function(options) {
  return function() {
    var self = this;
    options = options != null ? options : {};
    var db = tutils.getDbSync('test', true);

    // Server Manager options
    var server_options = {
      purgedirectories: true
    }

    // Merge in any options
    for(var name in options) {
      server_options[name] = options[name];
    }

    // Test suite start
    this.start = function(callback) {
	callback();
    }

    this.restart = function(callback) {
	callback();
    }

    // Test suite stop
    this.stop = function(callback) {
        callback();
    };

    // Pr test functions
    this.setup = function(callback) { callback(); }
    this.teardown = function(callback) { callback(); };

    // Returns the package for using Mongo driver classes
    this.getMongoPackage = function() {
      return tingodb;
    }


    this.newDbInstance = function() {
		return tutils.getDbSync("test",true);
    }

    // Returns a db
    this.db = function() {
      return db;
    }

    this.url = function(user, password) {
      if(user) {
        return 'mongodb://' + user + ':' + password + '@localhost:27017/' + self.db_name + '?safe=false';
      }

      return 'mongodb://localhost:27017/' + self.db_name + '?safe=false';
    }

    // Used in tests
    this.db_name = "test";    
  }
}


var assert = require('assert');
var _ = require('lodash');

var dir = './contrib';
var files = [
	'collection_tests',
	'cursor_tests',
	'find_tests',
	'insert_tests',
	'mapreduce_tests',
	'remove_tests'
];

_(files).each(function (file) {
	var tests = require(dir + '/' + file);
	describe(file, function () {
		this.timeout(10000);
		_(tests).each(function (fn, name) {
			if (typeof fn != 'function') return;
			it(name, function (done) {
				var configuration = new (config())();
				var test = {
					ok: function (x) { assert.ok(x); },
					equal: function (x, y) { assert.equal(x, y); },
					deepEqual: function (x, y) { assert.deepEqual(x, y); },
					throws: function (x, y) { assert.throws(x, y); },
					done: done
				};
				fn(configuration, test);
			});
		});
	});
});
