'use strict'

const
  util      = require('util'),
  mongodb   = require('mongodb').MongoClient,
  Timestamp = require('mongodb').Timestamp,
  MySQL     = require('./mysql.js'),

  ACCEPTED_TYPES = ['number', 'string', 'boolean']

/**
 * Tailer
 * @class
 */
class Tailer {
  /**
   * Constructor
   * @param {object} config - configulation options
   */
  constructor (config) {
    this.url    = config.src || 'mongodb://localhost:27017/test'
    this.url2   = this.url.replace(/\/\w+(\?|$)/, '/local$1')
    this.dbName = this.url.split(/\/|\?/)[3]
    this.defs   = createDefs(config.collections, this.dbName, config.prefix)
    this.lastTs = 0
    this.mysql  = new MySQL(config.dist, this.defs)
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
      .then(() => this.importAll())
      .then(() => this.updateTimestamp())
      .then(() => this.tail())
  }

  /**
   * Import all
   * @returns {Promise} with no value
   */
  importAll () {
    util.log('Begin to import...')
    let promise = Promise.resolve()
    this.defs.forEach(def => {
      promise = promise.then(() => this.importCollection(def))
    })
    promise.then(() => {
      util.log('Import Done.')
    })
    return promise
  }

  /**
   * Import collection
   * @param {object} def - definition of fields
   * @returns {Promise} with no value
   */
  importCollection (def) {
    util.log(`Importing ${ def.ns }`)
    return new Promise(resolve =>
      mongodb.connect(this.url, { 'auto_reconnect': true })
        .then(db => {
          const stream = db.collection(def.name).find().stream()
          stream
            .on('data', item => {
              stream.pause()
              this.mysql.insert(def, item, () => stream.resume())
            })
            .on('end', () => {
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
    if (ts) {
      this.lastTs = ts
      return Promise.resolve()
    }
    return new Promise(resolve =>
      mongodb.connect(this.url2, { 'auto_reconnect': true })
        .then(db =>
          db.collection('oplog.rs').find().sort({ $natural: -1 }).limit(1)
            .nextObject()
            .then(item => {
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
      ts  = this.lastTs,
      nss = this.defs.map(def => def.ns),
      filters = {
        ns: { $in: nss },
        ts: { $gt: Timestamp.fromNumber(ts) }
      },
      curOpts = {
        tailable: true,
        awaitdata: true,
        numberOfRetries: Number.MAX_VALUE,
        tailableRetryInterval: 1000
      }

    util.log(`Bigin to watch... (from ${ ts })`)
    mongodb.connect(this.url2, { 'auto_reconnect': true })
      .then(db => {
        const stream = db.collection('oplog.rs').find(filters, curOpts).stream()
        stream
          .on('data', log => {
            if (log.op == 'n' || log.ts.toNumber() == ts) return
            util.log('Syncing ' + log.ns)
            this.process(log)
          })
          .on('close', () => util.log('No more....'))
      })
  }

  /**
   * Process the log and sync to MySQL
   * @param {object} log - the log retrieved from oplog.rs
   * @returns {undefined}
   */
  process (log) {
    const def = this.defs.filter(def => log.ns == def.ns)[0]
    if (!def) return

    this.mysql.updateTimestamp(log.ts.toNumber())
    switch (log.op) {
      case 'i':
        return this.mysql.insert(def, log.o)
      case 'u':
        return this.mysql.update(def, log.o2._id, log.o.$set, log.o.$unset)
      case 'd':
        return this.mysql.remove(def, log.o._id)
      default:
        return
    }
  }
}

/**
 * Create definition list from config object
 * @param {object} collections - config object
 * @param {string} dbName - name of database
 * @param {string} prefix - optional prefix for table
 * @returns {undefined} void
 */
function createDefs(collections, dbName, prefix) {
  prefix = prefix || ''
  return Object.keys(collections).map(name => ({
    name: name,
    ns: `${ dbName }.${ name }`,
    distName: prefix + name,
    idType: collections[name]._id,
    fields: Object.keys(collections[name])
      .map(fieldName => ({
        name: fieldName,
        type: collections[name][fieldName]
      }))
      // Skip unknown types
      .filter(field => ACCEPTED_TYPES.some(type => type == field.type))
  }))
}

module.exports = Tailer
