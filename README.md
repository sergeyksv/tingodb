TingoDB
=======

Javascript in-process file system backed database upward compatible on API level with MongoDB.

Upward compatible means that if you build app that uses functionality implemented by TingoDB you can switch to MongoDB almost without code changes. This gretaly reduces implementation risks and give you freedom to switch to mature solution at any moment.

As a proof for upward compatibility all tests designed to run against both MongoDB and TingoDB. More over significant part of tests contributed from MongoDB nodejs driver project and used as is without modifications.

Usage
======

	npm install tingodb

As it stated API is fully compatible with MongoDB. Difference is only initialization thase and obtaining of Db object. Consider this MongoDB code:

	var Db = require('mongodb').Db,
		Server = require('mongodb').Server,
		assert = require('assert');

	var db = new Db('test', new Server('locahost', 27017));
	var collection = db.collection("batch_document_insert_collection_safe");
	collection.insert([{hello:'world_safe1'}
	  , {hello:'world_safe2'}], {w:1}, function(err, result) {
	  assert.equal(null, err);

	  collection.findOne({hello:'world_safe2'}, function(err, item) {
		assert.equal(null, err);
		assert.equal('world_safe2', item.hello);
	  })
	});

The same example using TingoDB will be following:

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

So, as you can see difference is in require call and database object initialization. 

#### require('tingodb')(options)

In contrast to MongoDB, module require call will not return usable module. It will return a function that accept configuration options. This function will return something similar to MongoDB module. Extra step allows to inject some options that will control database behavior.

##### nativeObjectID: true|false Default is false

Doing some experimentation we found that using integer keys we can get database work faster and save some space. Additionally for in-process database there are almost no any drawbacks versus globally unique keys. However in the same time it is relatively hard to keep unique integer keys outside of the database engine. So we make it part of the database engine code. Generated keys will be unique in collection scope. 

When required it is possible to switch to BSON ObjectID using the configuration option.

##### cacheSize: integer Default is 1000

Maximum number of cached objects per collection.

##### cacheMaxObjSize: integer Default is 1024 bytes

Maximum size of object that can be placed to cache.

##### searchInArray: true|false Default is false

Globally enables support of search in nested array. MongoDB support this unconditionally. For TingoDB search in arrays when there are no arrays is performance penalty. That's why it is switched off by default. 
Additionally, and it might be better approach, nested arrays support can be enabled for individual indexes or search queries.

To enable nested arrays in individual index use "_tiarr:true" option.
 
	self._cash_transactions.ensureIndex("splits.accountId",{_tiarr:true},cb); 
 
To enable nested arrays in individual query for fields that do not use indexes use "_tiarr." prefixed field names.
 
	coll.find({'arr.num':10},{"_tiar.arr.num":0}) 

####  new Db(path, options)

The only required parameter is database path. It should be valid path to empty folder or folder that already contain collection files.

Dual usage
=========

It is possible to build application that will transparently support both MongoDB and TingoDB. Here are some rules that help to do it:

* Wrap module require call into helper module or make it part of core object. This way you can control which engine is loaded in single place.
* Use only native JavaScript types. BSON types can be slow in JavaScript and will need special attention when passed to or from client JavaScript.
* Think about ObjectID as of just unique value that can be converted to and from String regardless its actual meaning.

Compatibility
=========
We maintain full API and functionality compatibility with MongoDB **BUT** only for what we implemented support. I.e. if we support something it will work exactly the same, but something is not yet supported or support is limited. 

- Search, almost all clauses. Indexes are used to increase search speed and sorting.
- Map reduce, almost all
- Grouping, almost all
- Collection, almost all methods
- Cursor, almost all methods
- Indexes, no support for compaund indxes, only single field indexes are supported. Full text search is also not supprted
- GridFS, no support
- Feature X, now know, might be :)


## MIT License

Copyright (c) [PushOk Software](http://www.pushok.com)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

[![githalytics.com alpha](https://cruel-carlota.pagodabox.com/43ade4aa68ffeff6305805e22bcf676a "githalytics.com")](http://githalytics.com/sergeyksv/tingodb)
