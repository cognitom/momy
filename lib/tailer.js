var util  = require('util')
var mongo = require('mongodb')

function Tailer(app) {
  util.log('Starting Tailer...')
  this.config = app.config
  this.mongo  = app.mongo
  this.mysql  = app.mysql
  this.app    = app
}

Tailer.prototype.start = function() {
  this.tail()
}

Tailer.prototype.import = function(callback) {
  util.log('Begin to import...')
  var self = this
  var col = self.config.mongodb.collection
  var stream = self.mongo.db2.collection(col).find().stream()

  stream
    .on('data', function(item) {
      stream.pause()
      self.mysql.insert(item, function() {
        stream.resume()
      })
    })
    .on('end', function(item) {
      util.log('Import Done.')
      self.app.tailer.read_timestamp(function() {
        callback()
      })
    })
}

Tailer.prototype.read_timestamp = function(callback) {
  var self = this
  var last = self.mongo.db.collection('oplog.rs')
    .find()
    .sort({ $natural: -1 })
    .limit(1)
  last.nextObject(function(err, item) {
    var timestamp = item.ts.toNumber()
    self.app.mysql.update_timestamp(timestamp)
    self.app.last_timestamp = timestamp
    callback()
  })
}

Tailer.prototype.tail = function() {
  var self = this
  var ts = self.app.last_timestamp
  var ns = self.config.mongodb.db + '.' + self.config.mongodb.collection
  var filters = {
    'ns': ns,
    'ts': { '$gt': mongo.Timestamp.fromNumber(ts) }
  }
  var curOpts = {
    tailable: true,
    awaitdata: true,
    numberOfRetries: Number.MAX_VALUE,
    tailableRetryInterval: 1000
  }

  util.log('Last timestamp: ' + ts)
  self.mongo.db.collection('oplog.rs')
    .find(filters, curOpts)
    .stream()
    .on('data', function(item) {
      if (item.op !== 'n' && item.ts.toNumber() !== ts) {
        util.log('Syncing ' + ns)
        self.process(item, function() {})
      }
    })
    .on('close', function() {
      util.log('No more....')
    })
}

Tailer.prototype.process = function(log, callback) {
  this.mysql.update_timestamp(log.ts.toNumber())
  switch (log.op) {
    case 'i':
      this.mysql.insert(log.o, callback)
      break
    case 'u':
      this.mysql.update(log.o2._id, log.o['$set'], log.o['$unset'], callback)
      break
    case 'd':
      this.mysql.remove(log.o._id, callback)
      break
  }
}

module.exports = Tailer
