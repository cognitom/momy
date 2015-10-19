var util      = require('util')
var Db        = require('mongodb').Db
var Server    = require('mongodb').Server
var Timestamp = require('mongodb').Timestamp
var MySQL     = require('./mysql.js')

/**
 * Tailer
 * @class
 */
function Tailer(config) {
  this.config  = config.mongodb
  this.last_ts = 0
  this.mysql   = new MySQL(config.mysql, config.sync_fields)
}

/** Start tailing **/
Tailer.prototype.start = function() {
  this.mysql.readTimestamp()
    .then(this.updateTimestamp)
    .then(this.tail)
}

/** Import all and start tailing **/
Tailer.prototype.importAndStart = function() {
  this.mysql.createTable()
    .then(this.import)
    .then(this.updateTimestamp)
    .then(this.tail)
}

/**
 * Import
 * @returns {Promise} with no value
 */
Tailer.prototype.import = function() {
  util.log('Begin to import...')
  var conf = this.config
  var self = this

  return new Promise(function(resolve, reject) {
    var db = connectToMongo(conf.host, conf.port, conf.db)
    var stream = db.collection(conf.collection).find().stream()
    stream
      .on('data', function(item) {
        stream.pause()
        self.mysql.insert(item, function(){ stream.resume() })
      })
      .on('end', function(item) {
        util.log('Import Done.')
        resolve()
      })
  })
}

/**
 * Check the latest log in Mongo, then catch the timestamp up in MySQL
 * @param {number} ts - unless null then skip updating in MySQL
 * @returns {Promise} with no value
 */
Tailer.prototype.updateTimestamp = function(ts) {
  var conf = this.config
  var self = this

  if (ts) {
    this.last_ts = ts
    return Promise.resolve()
  }
  return new Promise(function(resolve, reject) {
    var db = connectToMongo(conf.host, conf.port, 'local')
    var last = db.collection('oplog.rs')
      .find()
      .sort({ $natural: -1 })
      .limit(1)
    last.nextObject(function(err, item) {
      ts = item.ts.toNumber()
      self.last_ts = ts
      self.mysql.updateTimestamp(ts)
      resolve()
    })
  })
}

/**
 * Tail the log of Mongo by tailable cursors
 * @returns {Promise} with no value
 */
Tailer.prototype.tail = function() {
  var conf = this.config
  var ts   = this.last_ts
  var ns   = conf.db + '.' + conf.collection
  var self = this
  var filters = {
    'ns': ns,
    'ts': { '$gt': Timestamp.fromNumber(ts) }
  }
  var curOpts = {
    tailable: true,
    awaitdata: true,
    numberOfRetries: Number.MAX_VALUE,
    tailableRetryInterval: 1000
  }

  return new Promise(function(resolve, reject) {
    util.log('Last timestamp: ' + ts)
    var db = connectToMongo(conf.host, conf.port, 'local', resolve)
    var stream = db.collection('oplog.rs')
      .find(filters, curOpts)
      .stream()
    stream
      .on('data', function(log) {
        if (log.op == 'n' || log.ts.toNumber() == ts) return
        util.log('Syncing ' + ns)
        self.process(log)
      })
      .on('close', function() {
        util.log('No more....')
      })
  })
}

/**
 * Process the log and sync to MySQL
 * @param {object} log - the log retrieved from oplog.rs
 */
Tailer.prototype.process = function(log) {
  this.mysql.updateTimestamp(log.ts.toNumber())
  switch (log.op) {
    case 'i':
      this.mysql.insert(log.o)
      break
    case 'u':
      this.mysql.update(log.o2._id, log.o['$set'], log.o['$unset'])
      break
    case 'd':
      this.mysql.remove(log.o._id)
      break
  }
}

/**
 * Connect to MongoDB
 * @param {string} host - hostname of server
 * @param {string} port - port number of server
 * @param {string} dbName - name of database
 * @param {function} callback - called after the db closed (optional)
 * @returns {db} opened database
 */
function connectToMongo(host, port, dbName, callback) {
  util.log('Connect to ' + dbName + '...')
  var server = new Server(host, port, { auto_reconnect: true })
  var db = new Db(dbName, server, { safe: true })
  db.open(function(err) { if (err) throw err })
  db.on('close', function(err) {
    util.log('Connection to the database was closed!')
    if (callback) callback()
  })
  return db
}

module.exports = Tailer
