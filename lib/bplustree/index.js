var LeafNode     = require('./leaf_node'),
    InternalNode = require('./internal_node');

var default_options = {
  order: 100, // Min 1
  sort: 1 // 1, -1 or array
};

var BPlusTree = module.exports = function(options) {
  this.options = options || default_options;
  if (!this.options.order) {
    this.options.order = default_options.order;
  }
  if (!this.options.sort) {
    this.options.sort = default_options.sort;
  }

  var self = this;
  if (Array.isArray(this.options.sort)) this._compare = function (a1, a2) {
	  if (a2.length > a1.length) return -self._compare(a2, a1);
	  for (var i = 0; i < a1.length; i++) {
		  if (i >= a2.length) return self.options.sort[i];
		  var v1 = a1[i];
		  var v2 = a2[i];
		  if (v1 < v2) return -self.options.sort[i];
		  else if (v1 > v2) return self.options.sort[i];
	  }
	  return 0;
  };
  else this._compare = function (k1, k2) {
	  if (k1 < k2) return -self.options.sort;
	  else if (k1 > k2) return self.options.sort;
	  else if (k1 == k2) return 0;
	  // else FIXME: probably different data types
  };

  this.root = new LeafNode(options.order, this._compare);
};


BPlusTree.prototype.set = function(key, value) {
  var node = this._search(key);
  var ret = node.insert(key, value);
  if (ret) {
    this.root = ret;
  }
};

BPlusTree.prototype.get = function(key) {
  var node = this._search(key);
  for(var i=0; i<node.data.length; i++){
    if(this._compare(node.data[i].key, key) === 0) return node.data[i].value;
  }
  return null;
};

BPlusTree.prototype.del = function(key) {
  var node = this._search(key);
  for(var i=0; i<node.data.length; i++){
    if(this._compare(node.data[i].key, key) === 0) {
		node.data.splice(i,1)
		// TODO, NOTE SURE IF THIS IS ENOUGH
		break;
	}
  }
  return null;
};

BPlusTree.prototype.getNode = function(key) {
  return this._search(key);
};

BPlusTree.prototype._search = function(key) {
  var current = this.root;
  var found = false;

  while(current.isInternalNode){
    found = false;
    var len = current.data.length;
    for(var i=1; i<len; i+=2){
      if(this._compare(key, current.data[i]) <= 0) {
        current = current.data[i-1];
        found = true;
        break;
      }
    }

    // Follow infinity pointer
    if(!found) current = current.data[len - 1];
  }

  return current;
};

// walk the tree in order
BPlusTree.prototype.each = function(callback, node) {
  if (!node) {
    node = this.root;
  }
  var current = node;
  if(current.isLeafNode){
    for(var i = 0; i < current.data.length; i++) {
      var node = current.data[i];
      if (node.value) {
        callback(node.key, node.value);
      }
    }
  } else {
    for(var i=0; i<node.data.length; i+=2) {
      this.each(callback, node.data[i]);
    }
  }
};

// walk the tree in order
BPlusTree.prototype.all = function(node,res) {
  if (!res)
	res = [];
  if (!node) {
    node = this.root;
  }
  var current = node;
  if(current.isLeafNode){
    for(var i = 0; i < current.data.length; i++) {
      var node = current.data[i];
      res.push(node.value)
    }
  } else {
    for(var i=0; i<node.data.length; i+=2) {
      this.all(node.data[i],res);
    }
  }
  return res;
};

BPlusTree.prototype.each_reverse = function(callback, node) {
  if (!node) {
    node = this.root;
  }
  var current = node;
  if(current.isLeafNode){
    for(var i = current.data.length - 1; i >= 0 ; i--) {
      var node = current.data[i];
      if (node.value) {
        callback(node.key, node.value);
      }
    }
  } else {
    for(var i=node.data.length - 1; i >= 0; i-=2) {
      this.each(callback, node.data[i]);
    }
  }
};


// Get a range
BPlusTree.prototype.range = function(start, end, callback) {
  var node = this._search(start);
  if (!node) {
    node = this.root;
    while (!node.isLeafNode) {
      node = node[0]; // smallest node
    }
  }
  var ended = false;

  while (!ended) {
    for(var i = 0; i < node.data.length; i ++) {
      var data = node.data[i];
      var key = data.key;
      if (end && this._compare(key, end) > 0) {
        ended = true;
        break;
      } else {
        if ((start === undefined || this._compare(start, key) <= 0) && (end === undefined || this._compare(end, key) >= 0) && data.value) {
          callback(key, data.value);
        }
      }
    }
    node = node.nextNode;
    if (!node) {
      ended = true
    }
  }
};

BPlusTree.prototype.rangeSync = function(start, end, exclusive_start, exclusive_end) {
  var values = [];
  var node = this._search(start);
  if (!node) {
    node = this.root;
    while (!node.isLeafNode) {
      node = node[0]; // smallest node
    }
  }
  var ended = false;

  var self = this;
  function keyCheck(key) {
        return (start === undefined
            || start === null
            || !exclusive_start && self._compare(start, key) <= 0
            || exclusive_start && self._compare(start, key) < 0
          ) && (
               end === undefined
            || end === null
            || !exclusive_end && self._compare(end, key) >= 0
            || exclusive_end && self._compare(end, key) > 0
          )
	}

  while (!ended) {
	if (values.length && node.data.length>0 && keyCheck(node.data[0].key,node.data[0].value) &&
		keyCheck(node.data[node.data.length-1].key,node.data[node.data.length-1].value))
	{
		// entire node is in range
		for(var i = 0; i < node.data.length; i ++) {
			values.push(node.data[i].value);
		}
	} else {
		for(var i = 0; i < node.data.length; i ++) {
		  var data = node.data[i];
		  var key = data.key;
		  if (end && this._compare(key, end) > 0) {
			ended = true;
			break;
		  } else {
			  if (keyCheck(key,data.value))
				values.push(data.value);
		  }
	  }
    }
    node = node.nextNode;
    if (!node) {
      ended = true
    }
  }
  return values;
};

// B+ tree dump routines
BPlusTree.prototype.walk = function(node, level, arr) {
  var current = node;
  if(!arr[level]) arr[level] = [];

  if(current.isLeafNode){
    for(var i=0; i<current.data.length; i++){
      arr[level].push("<"+current.data[i].key+">");
    }
    arr[level].push(" -> ");
  }else{

    for(var i=1; i<node.data.length; i+=2){
      arr[level].push("<"+node.data[i]+">");
    }
    arr[level].push(" -> ");
    for(var i=0; i<node.data.length; i+=2) {
      this.walk(node.data[i], level+1, arr);
    }

  }
  return arr;
};

BPlusTree.prototype.dump = function() {
  var arr = [];
  this.walk(this.root, 0, arr);
  for(var i=0; i<arr.length; i++){
    var s = "";
    for(var j=0; j<arr[i].length; j++){
      s += arr[i][j];
    }
  }
  return s;
};

module.exports.create = function(options) {
  return new BPlusTree(options);
};
