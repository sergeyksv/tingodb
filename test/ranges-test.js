var assert = require('assert');
var _ = require('lodash');
var safe = require('safe');
var tutils = require("./utils");

describe('Misc', function () {
    var db, coll;
    before(function (done) {
        tutils.getDb('misc', true, safe.sure(done, function (_db) {
            db = _db;
            done();
        }))
    })

    it('multiple fields search by range', function (done) {

        var samples = [
            {"date2": "2008-12-29T23:00:00.000Z", "n_length": 16},
            {"date2": "2009-12-29T23:00:00.000Z", "n_length": 5},
            {"date2": "2009-12-30T23:00:00.000Z", "n_length": 10},
            {"date2": "2009-12-28T23:00:00.000Z", "n_length": 11},
            {"date2": "2008-12-29T23:00:00.000Z", "n_length": 16},
            {"date2": "2008-12-29T23:00:00.000Z", "n_length": 16}
        ]

        var q = {
            date2: {$gte: '2009-01-01T00:00:00.000Z', $lt: '2010-01-02T00:00:00.000Z'},
            n_length: {$gte: 8, $lte: 10}
        }

        db.collection("RANGES", {}, safe.sure(done, function (_coll) {

            _coll.insert(samples, safe.sure(done, function () {

                _coll.createIndex({"date2": 1}, function (err, indexname) {
                    _coll.find(q).toArray(function (err, res) {
                        assert(res)
                        assert(res.length > 0)
                        assert(res[0].date2 === '2009-12-30T23:00:00.000Z')
                        done()
                    })
                })

            }));
        }));
    });
});
