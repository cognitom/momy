'use strict'

const
  util      = require('util'),
  mongodb   = require('mongodb').MongoClient,
  Timestamp = require('mongodb').Timestamp,
  MySQL     = require('./mysql.js')

/**
 * Tailer
 * @class
 */
class Tailer {
  constructor (config) {
    this.config = config.mongodb || {}
    this.lastTs = 0
    this.mysql  = new MySQL(config.mysql, config.sync_fields)
  }

  /** Start tailing **/
  start () {
    this.mysql.readTimestamp()
      .then(ts => this.updateTimestamp(ts))
      .then(() => this.tail())
  }

  /** Import all and start tailing **/
  importAndStart () {
    this.mysql.createTable()
      .then(() => this.import())
      .then(() => this.updateTimestamp())
      .then(() => this.tail())
  }

  /**
   * Import
   * @returns {Promise} with no value
   */
  import () {
    util.log('Begin to import...')
    const
      conf = this.config,
      url  = 'mongodb://' + conf.host + ':' + conf.port + '/' + conf.db

    return new Promise(resolve =>
      mongodb.connect(url, { 'auto_reconnect': true })
        .then(db => {
          const stream = db.collection(conf.collection).find().stream()
          stream
            .on('data', item => {
              stream.pause()
              this.mysql.insert(item, () => stream.resume())
            })
            .on('end', () => {
              util.log('Import Done.')
              resolve()
            })
        }))
  }

  /**
   * Check the latest log in Mongo, then catch the timestamp up in MySQL
   * @param {number} ts - unless null then skip updating in MySQL
   * @returns {Promise} with no value
   */
  updateTimestamp (ts) {
    const
      conf = this.config,
      url  = 'mongodb://' + conf.host + ':' + conf.port + '/local'

    if (ts) {
      this.lastTs = ts
      return Promise.resolve()
    }
    return new Promise(resolve =>
      mongodb.connect(url, { 'auto_reconnect': true })
        .then((db) =>
          db.collection('oplog.rs').find().sort({ $natural: -1 }).limit(1)
            .nextObject()
            .then((item) => {
              ts = item.ts.toNumber()
              this.lastTs = ts
              this.mysql.updateTimestamp(ts)
              resolve()
            })))
  }

  /**
   * Tail the log of Mongo by tailable cursors
   */
  tail () {
    const
      conf = this.config,
      ts   = this.lastTs,
      ns   = conf.db + '.' + conf.collection,
      url  = 'mongodb://' + conf.host + ':' + conf.port + '/local',
      filters = { ns, ts: { $gt: Timestamp.fromNumber(ts) } },
      curOpts = {
        tailable: true,
        awaitdata: true,
        numberOfRetries: Number.MAX_VALUE,
        tailableRetryInterval: 1000
      }

    util.log('Last timestamp: ' + ts)
    mongodb.connect(url, { 'auto_reconnect': true })
      .then(db => {
        const stream = db.collection('oplog.rs').find(filters, curOpts).stream()
        stream
          .on('data', (log) => {
            if (log.op == 'n' || log.ts.toNumber() == ts) return
            util.log('Syncing ' + ns)
            this.process(log)
          })
          .on('close', () => util.log('No more....'))
      })
  }

  /**
   * Process the log and sync to MySQL
   * @param {object} log - the log retrieved from oplog.rs
   */
  process (log) {
    this.mysql.updateTimestamp(log.ts.toNumber())
    switch (log.op) {
      case 'i': this.mysql.insert(log.o); break
      case 'u': this.mysql.update(log.o2._id, log.o.$set, log.o.$unset); break
      case 'd': this.mysql.remove(log.o._id); break
      default: break
    }
  }
}

module.exports = Tailer
