var tutils = require('./utils');

var config = function(options) {
  return function() {
    var self = this;
    options = options != null ? options : {};
    var db = tutils.getDbSync('test', {w: 0, native_parser: false}, {auto_reconnect: false, poolSize: 4}, true);

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
      tutils.openEmpty(db, callback);
    };

    this.restart = function(callback) {
      self.stop(function (err) {
        if (err) callback(err);
        else self.start(callback);
      });
    };

    // Test suite stop
    this.stop = function(callback) {
      db.close(callback);
    };

    // Pr test functions
    this.setup = function(callback) { callback(); }
    this.teardown = function(callback) { callback(); };

    // Returns the package for using Mongo driver classes
    this.getMongoPackage = function() {
      return tutils.getDbPackage();
    }


    this.newDbInstance = function(db_options, server_options) {
		return tutils.getDbSync("test", db_options, server_options, true);
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
	'cursorstream_tests',
	'find_tests',
	'insert_tests',
	'mapreduce_tests',
	'remove_tests'
];

var names = {};

describe('contrib', function () {
	var configuration = new (config())();
	before(function (done) {
		configuration.start(done);
	});
	_(files).each(function (file) {
		var tests = require(dir + '/' + file);
		describe(file, function () {
			this.timeout(15000);
			_(tests).each(function (fn, name) {
				if (typeof fn != 'function') return;
				describe(name, function () {
					var done;
					it('test', function (_done) {
						done = _done;
						if (names[name]) {
							console.log('dup: ' + name);
							return done();
						}
						names[name] = true;
						var test = {
							ok: function (x) { assert.ok(x); },
							equal: function (x, y) { assert.equal(y, x); },
							deepEqual: function (x, y) { assert.deepEqual(y, x); },
							throws: function (x, y) { assert.throws(x, y); },
							done: function () { done(); }
						};
						fn(configuration, test);
					});
					afterEach(function () {
						done = function () {};
					});
				});
			});
		});
	});
	after(function (done) {
		configuration.stop(done);
	});
});
