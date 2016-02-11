TingoDB
=======

[![Build Status](https://travis-ci.org/sergeyksv/tingodb.png?branch=master)](https://travis-ci.org/sergeyksv/tingodb)

**TingoDB** is an embedded JavaScript in-process filesystem or in-memory database upwards compatible with MongoDB at the API level.

Upwards compatible means that if you build an app that uses functionality implemented by TingoDB you can switch to MongoDB almost without code changes. This greatly reduces implementation risks and give you freedom to switch to a mature solution at any moment.

As a proof for upward compatibility, all tests designed to run against both MongoDB and TingoDB.
Moreover, significant parts of tests contributed from MongoDB nodejs driver projects and are used as is without modifications.

For those folks who familiar with the Mongoose.js ODM, we suggest to look at [Tungus](https://github.com/sergeyksv/tungus), an experimental driver that allows using the famous ODM tool with our database.

TingoDB can be dropin replacement for existing apps and frameworks that are based on MongoDB. Please see some [3rd party integrations](#integrations)

For more details please visit http://www.tingodb.com

Submitting bugs
--------------

Goal of our project is to fully mimic MongoDB functionality. Which means that we will consider bug as bug only when you find something that is working with MongoDB but isn't working with TingoDB. It would be very helpful to get bugs in that case as pull requests to /test/misc-test.js file (or new file) which will contain code that reproduce issue.

To run test with MongoDB: ```./test.sh --quick --single=misc-test --db=mongodb```.

To run test with TingoDB: ```./test.sh --quick --single=misc-test --db=tingodb```

Usage
======

	npm install tingodb

As stated, the API is fully compatible with MongoDB. The only differences are the initialization and getting the Db object. Consider this MongoDB code:

```javascript
var Db = require('mongodb').Db,
	Server = require('mongodb').Server,
	assert = require('assert');

var db = new Db('test', new Server('localhost', 27017));
var collection = db.collection("batch_document_insert_collection_safe");
collection.insert([{hello:'world_safe1'}
  , {hello:'world_safe2'}], {w:1}, function(err, result) {
  assert.equal(null, err);

  collection.findOne({hello:'world_safe2'}, function(err, item) {
	assert.equal(null, err);
	assert.equal('world_safe2', item.hello);
  })
});
```

The same example using TingoDB is as follows:

```javascript
var Db = require('tingodb')().Db,
	assert = require('assert');

var db = new Db('/some/local/path', {});
// Fetch a collection to insert document into
var collection = db.collection("batch_document_insert_collection_safe");
// Insert a single document
collection.insert([{hello:'world_safe1'}
  , {hello:'world_safe2'}], {w:1}, function(err, result) {
  assert.equal(null, err);

  // Fetch the document
  collection.findOne({hello:'world_safe2'}, function(err, item) {
	assert.equal(null, err);
	assert.equal('world_safe2', item.hello);
  })
});
```

As you can see, the difference is in the `require` call and database object initialization.

#### require('tingodb')(options)

In contrast to MongoDB, the module `require` call will not return a usable module. It will return a function that accepts configuration options. This function will return something similar to the MongoDB module. The extra step allows for passing some options that will control database behavior.

##### memStore: true|false Default is false
Enable in memory (no file access) mode. Can be useful for unit tests. File path will be used as db identity.

##### nativeObjectID: true|false Default is false

Doing some experimentation we found that using integer keys we can get the database to work faster and save some space. Additionally, for in-process databases there are almost no drawbacks versus globally unique keys. However, at the same time, it is relatively hard to keep unique integer keys outside of the database engine, so we made it part of the database engine code. Generated keys will be unique in the collection scope.

When required, it is possible to switch to BSON ObjectID using the configuration option.

##### cacheSize: integer Default is 1000

Maximum number of cached objects per collection.

##### cacheMaxObjSize: integer Default is 1024 bytes

Maximum size of objects that can be placed in the cache.

##### searchInArray: true|false Default is false

Globally enables support of search in nested arrays. MongoDB supports this unconditionally. For TingoDB, searching arrays when there are no arrays incurs a performance penalty. That's why this is switched off by default.
Additionally, and this might be a better approach, nested array support can be enabled for individual indexes or search queries.

To enable nested arrays in individual indexed, use "_tiarr:true" option.

	self._cash_transactions.ensureIndex("splits.accountId",{_tiarr:true},cb);

To enable nested arrays in individual queries for fields that do not use indexes, use "_tiarr." to prefix field names.

	coll.find({'arr.num':10},{"_tiar.arr.num":0})

####  new Db(path, options)

The only required parameter is the database path. It should be a valid path to an empty folder or a folder that already contains collection files.

API extensions
==============

#### Collection.compactCollection, Database.compactDatabase

From the initial release compactionation function was available internally. There were several requests to make this avilable through API and we did it. Please keep in mind that compactination is best called as the first operation with database. Using compactionation in the middle of work session is also possible, but all cursors obtained prior to that will be invalidated and will throw errors on data access.

Dual usage
=========

It is possible to build applications that will transparently support both MongoDB and TingoDB. Here are some hints on how to do that:

* Wrap the module `require` call into a helper module or make it part of the core object. This way you can control which engine is loaded in one place.
* Use only native JavaScript types. BSON types can be slow in JavaScript and will need special attention when passed to or from client JavaScript.
* Treat ObjectID just as a unique value that can be converted to and from String regardless its actual meaning.

Example below (please see the three files).

###### engine.js - wrapper around TingoDB and MongoDB


```javascript
var fs = require('fs'),db,engine;

// load config
var cfg = JSON.parse(fs.readFileSync("./config.json"));

// load requestd engine and define engine-agnostic getDB function
if (cfg.app.engine=="mongodb") {
	engine = require("mongodb");
	module.exports.getDB = function () {
		if (!db) db = new engine.Db(cfg.mongo.db,
			new engine.Server(cfg.mongo.host, cfg.mongo.port, cfg.mongo.opts),
				{native_parser: false, safe:true});
		return db;
	}
} else {
	engine = require("tingodb")({});
	module.exports.getDB = function () {
		if (!db) db = new engine.Db(cfg.tingo.path, {});
		return db;
	}
}
// Depending on engine, this can be a different class
module.exports.ObjectID = engine.ObjectID;
```

###### sample.js - Dummy usage example, pay attention to comments


```javascript
var engine = require('./engine');
var db = engine.getDB();

console.time("sample")
db.open(function(err,db) {
	db.collection("homes", function (err, homes) {
		// it's fine to create ObjectID in advance
		// NOTE!!! we get class through engine because its type
		// can depends on database type
		var homeId = new engine.ObjectID();
		// but with TingoDB.ObjectID righ here it will be negative
		// which means temporary. However it's unique and can be used for
		// comparisons
		console.log(homeId);
		homes.insert({_id:homeId, name:"test"}, function (err, home) {
			var home = home[0];
			// here, homeID will change its value and will be in sync
			// with the database
			console.log(homeId,home);
			db.collection("rooms", function (err, rooms) {
				for (var i=0; i<5; i++) {
					// it's ok also to not provide id, then it will be generated
					rooms.insert({name:"room_"+i,_idHome:homeId}, function (err, room) {
						console.log(room[0]);
						i--;
						if (i==0) {
							// now lets assume we serving request like
							// /rooms?homeid=_some_string_
							var query = "/rooms?homeid="+homeId.toString();
							// dirty code to get simulated GET variable
							var getId = query.match("homeid=(.*)")[1];
							console.log(query, getId)
							// typical code to get id from external world
							// and use it for queries
							rooms.find({_idHome:new engine.ObjectID(getId)})
								.count(function (err, count) {
									console.log(count);
									console.timeEnd("sample");
							})
						}
					})
				}
			})
		})
	})
})
```

###### config.json - Dummy config

```javascript
{
	"app":{
		"engine":"tingodb"
	},
	"mongo":{
		"host":"127.0.0.1",
		"port":27017,
		"db":"data",
		"opts":{
			"auto_reconnect": true,
			"safe": true
		}
	},
	"tingo":{
		"path":"./data"
	}
}
```

###### Console output running on TingoDB


	-2
	13 { _id: 13, name: 'test' }
	{ name: 'room_0', _idHome: 13, _id: 57 }
	{ name: 'room_1', _idHome: 13, _id: 58 }
	{ name: 'room_2', _idHome: 13, _id: 59 }
	{ name: 'room_3', _idHome: 13, _id: 60 }
	{ name: 'room_4', _idHome: 13, _id: 61 }
	/rooms?homeid=13 13
	5
	sample: 27ms

###### Console output running on MongoDB

	51b43a05f092a1c544000001
	51b43a05f092a1c544000001 { _id: 51b43a05f092a1c544000001, name: 'test' }
	{ name: 'room_3',
	  _idHome: 51b43a05f092a1c544000001,
	  _id: 51b43a05f092a1c544000005 }
	{ name: 'room_2',
	  _idHome: 51b43a05f092a1c544000001,
	  _id: 51b43a05f092a1c544000004 }
	{ name: 'room_1',
	  _idHome: 51b43a05f092a1c544000001,
	  _id: 51b43a05f092a1c544000003 }
	{ name: 'room_0',
	  _idHome: 51b43a05f092a1c544000001,
	  _id: 51b43a05f092a1c544000002 }
	{ name: 'room_4',
	  _idHome: 51b43a05f092a1c544000001,
	  _id: 51b43a05f092a1c544000006 }
	/rooms?homeid=51b43a05f092a1c544000001 51b43a05f092a1c544000001
	5
	sample: 22ms

Compatibility
=========
We maintain full API and functionality compatibility with MongoDB **BUT** only for what we implemented support. I.e. if we support something it will work exactly the same way, but some features are not yet supported or support is limited.

- Search, almost all clauses. Indexes are used to increase search speed and sorting.
- Map reduce, almost all
- Grouping, almost all
- Collection, almost all methods
- Cursor, almost all methods
- GridFS, no support
- Feature X, might be :)

<a name="integrations"></a>3rd Party Integrations
=========
We are open for contributions for this section. If you found or implemented some integration please open pull request :).

__Sails.js__

TingoDB adapter for Sails.js: http://github.com/andyhu/sails-tingo
Sails.js is a RoR like Node.js framework. It has a powerful ORM system called `waterline`, it supports all major databases (and web services) and provides an easy to use, unified interface for querying all different kind of databases. It also supports cross database (or different database engines) joins.

__KeystoneJS__

Running KeystoneJS with TingoDB: http://ifrederik.com/blog/2014/11/cms-without-db-running-keystonejs-without-mongodb/

__Realistic app with dual database support__

Web based GnuCash clone: https://github.com/sergeyksv/skilap

## MIT License

Copyright (c) [PushOk Software](http://www.pushok.com)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
