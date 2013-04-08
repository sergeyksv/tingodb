var Configuration = require('integra').Configuration
  , Runner = require('integra').Runner
  , ParallelRunner = require('integra').ParallelRunner
  , tutils = require('../../../utils')
  , tingodb = require('../../../..');

var single_server_config = function(options) {
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

exports.single_server_config = single_server_config;
