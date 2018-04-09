var assert = require('assert');
var _ = require('lodash');
var safe = require('safe');
var tutils = require("./utils");
var assert = require('assert');

describe('API Compat test', function () {
    describe("2.x specific",function () {
        var db;
        before(function (done) {
            tutils.getDb('appcompat', true, require('../lib/main')({ apiLevel: 200 }), safe.sure(done, function (_db) {
                db = _db;
                done();
            }))
        })        

        it('API1 findAndModify result structure', function (done) {
            db.collection("API1", {}, safe.sure(done,function (_coll) {
                _coll.insert({name:'Tony',age:'37'}, safe.sure(done, function () {
                    _coll.findAndModify({},{age:1},{$set: {name: 'Tony'}, $unset: { age: true }},{new:true},safe.sure(done, function (result) {
                        // expecting here something like { value: { name: 'Tony', _id: 2 }, ok: 1 }
                        assert(result.value);
                        assert(result.value._id)
                        assert(result.value.name)
                        assert.equal(result.ok,1);1
                        done()
                    }))
                }))
            }))
        })
    })
});
