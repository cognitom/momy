var util = require('util'),
    mongo = require('mongodb');

module.exports = Tailer;

function Tailer(app) {
  util.log('Starting Tailer...');
  this.config = app.config;
  this.mongo = app.mongo;
  this.mysql = app.mysql;
  this.app = app;
}

Tailer.prototype.start = function () {
  this.tail();
};

Tailer.prototype.import = function (callback) {
  util.log('Begin to import...');
  var self = this;
  var stream = this.mongo.db2.collection(self.config.mongodb.collection).find().stream();
  stream.on("data", function (item) {
    stream.pause();
    self.mysql.insert(item, function () {
      stream.resume();
    });
  });
  stream.on('end', function (item) {
    util.log('Import Done.');
    self.app.tailer.read_timestamp(function () {
      callback();
    });
  });
};

Tailer.prototype.read_timestamp = function (callback) {
  var self = this;
  var last = this.mongo.db.collection('oplog.$main').find().sort({
    $natural: -1
  }).limit(1);
  last.nextObject(function (err, item) {
    var timestamp = item.ts.toNumber();
    self.app.mysql.update_timestamp(timestamp);
    self.app.last_timestamp = timestamp;
    callback();
  });
};

Tailer.prototype.tail = function () {
  var self = this;
  util.log('Last timestamp: ' + this.app.last_timestamp);
  //console.log(new mongo.Timestamp.fromNumber(5856968703085642000));
  var options = {
    'ns': self.config.mongodb.db + '.' + self.config.mongodb.collection,
    'ts': {
      '$gt': new mongo.Timestamp.fromNumber(this.app.last_timestamp)
    }
  };

  var stream = this.mongo.db.collection('oplog.$main').find(options, {
    tailable: true,
    awaitdata: true,
    numberOfRetries: -1
  }).stream();

  stream.on('data', function (item) {
    if (item.op !== 'n' && item.ts.toNumber() !== self.app.last_timestamp) {
      //util.log(JSON.stringify(item)+'\r\n');
      self.process(item, function () {});
    }
  });

  stream.on('close', function () {
    util.log("No more....");
  });
};

Tailer.prototype.process = function (log, callback) {
  this.mysql.update_timestamp(log.ts.toNumber());
  switch (log.op) {
  case 'i':
    this.mysql.insert(log.o, callback);
    break;
  case 'u':
    this.mysql.update(log.o2._id, log.o['$set'], log.o['$unset'], callback);
    break;
  case 'd':
    this.mysql.remove(log.o._id, callback);
    break;
  }
};