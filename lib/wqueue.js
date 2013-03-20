var safe = require('safe');
var _ = require('underscore');

function wqueue (limit) {
	this.limit = limit || 1;
	this._rc = 0;
	this._q = [];
	this._blocked = false;
}

wqueue.prototype.add = function(task,block,cb) {
	this._q.push({task:task,block:block, cb:cb});
	this._ping();
}

wqueue.prototype._exec = function (task,block,cb) {
	var self = this;
	this._blocked = block;
	self._rc++;
	task(function () {
		cb.apply(this,arguments)
		self._rc--;
		if (self._rc==0)
			self._blocked = false;
		self._ping();
	})
}

wqueue.prototype._ping = function () {
	var self = this;
	process.nextTick(function () {
		while (self._q.length>0 && self._rc<self.limit && (!(self.blocked || self._q[0].block) || self._rc==0)  ) {
			var t = self._q.splice(0,1)[0];
			self._exec(t.task, t.block, t.cb)
		}
	})
}

module.exports = wqueue;
