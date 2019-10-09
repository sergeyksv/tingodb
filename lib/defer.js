function defer() {
    var value, error, resolved, rejected
    var deferred = function(err, value) {
        if (err) deferred.reject(err)
        else deferred.resolve(value)
    }
    deferred.resolve = function(v) {
        if (!resolved && !rejected) {
            value = v
            resolved = true
        }
    }
    deferred.reject = function(e) {
        if (!resolved && !rejected) {
            error = e
            rejected = true
        }
    }
    deferred.promise = new Promise(function(resolve, reject) {
        if (resolved) resolve(value)
        if (rejected) reject(error)
        deferred.resolve = resolve
        deferred.reject = reject
    })
    return deferred
}

module.exports = defer