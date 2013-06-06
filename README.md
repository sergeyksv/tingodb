TingoDB
=======

Javascript in-process file system backed database upward compatible on API level with MongoDB.

Upward compatible means that if you build app that uses functionality implemented by TingoDB you can switch to MongoDB almost without code changes. This gretaly reduces implementation risks and give you freedom to switch to mature solution at any moment.

As a proof for upward compatibility all tests designed to run against both MongoDB and TingoDB. More over significant part of tests contributed from MongoDB nodejs driver project and used as is without modifications.

Usage
======

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

API
======

require('tingodb')(options)
------------------------
In contrast to mongodb module require will not return object that contains Db and other classes. It will return callable function that being called will return something similar to MongoDB require. Such implementation allows to inject some options that will control database behavior. Following options supported:

- nativeObjectID: true|false Default is false

Doing some experimentation we found that using integer ids we can get db work faster and save some space. Additionally for inprocess database there are almost no any drawbacks versus globally unique ids. However in the same time its relatively hard to keep unique integer ids outside of database engine. So we make it part of database engine code. So when you insert records and didn't provide id explicetly it will use require('tingodb')().ObjectID class. By default it will generate integer ids that are unique in collection scope. But if you want MongoDB.ObjectID you can do it by setting nativeObjectID function to true.
If you want to have ability to switch between MongoDB and TingoDB ObjectID (that uses integers) you should avoid direct require of BSON.ObjectID class but always refer it thru primary db object (require('tingodb)() or require('mongodb'). TingoDB ObjectID class support basic subset of MongoDB.ObjectID functionality. It also can be created and used directly for any fields in your documents.

- cacheSize: integer

Maximum number of memory cached objects per collection. Default 1000

- cacheMaxObjSize: integer
Maximum size of object that can be placed to cache. Default 1024 bytes

- searchInArray: true|false

MongoDB allows search inside nested arrays. Sometime it is useful. Search code that didn't support that 2 to 4 times faster. And the problem is that we not know in advance what code to use. You know because you know your data, we not. So if you want this feature you can enbale it globally. However if you prefer you can finetune this in every find request by using kind of hack providing fields for which you expect arrays giving them _tiar prefix. Example:

	coll.find({'arr.num':10},{"_tiar.arr.num":0})

new Db(path, options)
-------------------------------

Database is just a folder containing file for every collection, so when you create database you need to specify it. Collection files are used in append only mode which makes them relatively safe for use.

Compatibilty
=========
We maintain full API and functionality compatibility with MongoDB **BUT** only for what we implement support. I.e. if we support something it will work exactly the same, but something is not yet supported or support is limited. 

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
