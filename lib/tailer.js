'use strict'

const util = require('util')
const mongodb = require('mongodb').MongoClient
const Timestamp = require('mongodb').Timestamp
const MySQL = require('./mysql.js')
const createDefs = require('./defs.js')
const { exit } = require('process')

/**
 * Tailer
 * @class
 */
class Tailer {
  /**
   * Constructor
   * @param {object} config - configulation options
   * @param {boolean} cliMode - set false for testing
   */
  constructor (config, cliMode) {
    // this.opts = {
    //   prefix: config.prefix || '',
    //   fieldCase: config.fieldCase || '',
    //   exclusions: config.exclusions || '',
    //   inclusions: config.inclusions || ''
    // }
    const opts = {
      prefix: config.prefix || '',
      fieldCase: config.fieldCase || '',
      exclusions: config.exclusions || '',
      inclusions: config.inclusions || ''
    }
    this.cliMode = cliMode === undefined ? true : !!cliMode
    this.url = config.src || 'mongodb://localhost:27017/test'
    this.url2 = this.url.replace(/\/\w+(\?|$)/, '/local$1')
    this.dbName = this.url.split(/\/|\?/)[3]
    this.defs = createDefs(config.collections, this.dbName, opts)
    this.relationDef = []
    this.relations = config.relations || {}
    this.lastTs = 0
    this.mysql = new MySQL(config.dist)
    const keys = Object.keys(this.relations)
    for (
      let i = 0, relation = this.relations[keys[i]];
      i < keys.length;
      i++, relation = this.relations[keys[i]]
    ) {
      if (relation.hasOwnProperty('collection')) {
        const def = createDefs(relation.collection, this.dbName, opts)
        let index = this.relationDef.push(def[0])
        relation.def = index - 1
      }
    }
  }

  /**
   * Start tailing
   * @param {boolean} forever - set false for testing
   */
  start (forever) {
    forever = forever === undefined ? true : !!forever
    this.mysql.readTimestamp()
      .then(ts => this.updateTimestamp(ts, true))
      .then(() => forever ? this.tailForever() : this.tail())
      .catch(err => {
        console.log(`${new Date()}: Tailer.start -> ${err} \n`)
        return this.stop(err)
      })
  }

  /**
   * Import all and start tailing
   * @param {boolean} forever - set false for testing
   */
  importAndStart (forever) {
    forever = forever === undefined ? true : !!forever
    this.mysql.bulkDropTable(this.defs)
      .then(() => this.mysql.bulkDropTable(this.relationDef)) // relation table
      .then(() => this.mysql.bulkCreateTable(this.defs))
      .then(() => this.mysql.bulkCreateTable(this.relationDef)) // relation table
      .then(() => this.importAll())
      .then(() => this.updateTimestamp())
      .then(() => forever ? this.tailForever() : this.tail())
      .catch(err => {
        console.log(`${new Date()}: Tailer.importAndStart -> ${err} \n`)
        return this.stop(err)
      })
  }

  stop (err) {
    if (this.cliMode) {
      if (err) console.log(err)
      util.log('Bye')
      process.exit()
    } else if (this.db) {
      this.db.close()
      this.db = null
    }
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
      util.log('Done.')
    })
    return promise
  }

  /**
   * Import collection
   * @param {object} def - definition of fields
   * @returns {Promise} with no value
   */
  importCollection (def) {
    util.log(`Import records in ${def.ns}`)
    return new Promise(resolve =>
      mongodb.connect(this.url, { 'auto_reconnect': true })
        .then(db => {
          const stream = db.collection(def.name).find().stream()
          stream
            .on('data', item => {
              stream.pause()
              this.objectWalk(item, (def, item, relation) => {
                this.mysql.insertRelation(def, item, relation)
              })
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
   * @param {boolean} skipUpdateMySQL - skip update in MySQL
   * @returns {Promise} with no value
   */
  updateTimestamp (ts, skipUpdateMySQL) {
    if (ts) {
      this.lastTs = ts
      if (!skipUpdateMySQL) this.mysql.updateTimestamp(ts)
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
              if (!skipUpdateMySQL) this.mysql.updateTimestamp(ts)
              resolve()
            })))
  }

  /**
   * Tail forever
   * @returns {Promise} with no value
   */
  tailForever () {
    return new Promise((resolve, reject) => {
      let counter = 0
      let promise = Promise.resolve()
      const chainPromise = () => {
        promise = promise
          .then(() => {
            const message = counter++
              ? 'Reconnect to MongoDB...'
              : 'Connect to MongoDB...'
            util.log(message)
            return this.tail()
          })
          .catch(err => {
            console.log(`${new Date()}: Tailer.tailForever -> ${err} \n`)
            return reject(err)
          })
          .then(chainPromise)
      }
      chainPromise()
    })
  }

  /**
   * Tail the log of Mongo by tailable cursors
   * @returns {Promise} with no value
   */
  tail () {
    const ts = this.lastTs
    const nss = this.defs.map(def => def.ns)
    const filters = {
      ns: { $in: nss },
      ts: { $gt: Timestamp.fromNumber(ts) }
    }
    const curOpts = {
      tailable: true,
      awaitdata: true,
      numberOfRetries: 60 * 60 * 24, // Number.MAX_VALUE,
      tailableRetryInterval: 1000
    }

    util.log(`Begin to watch... (from ${ts})`)
    return new Promise((resolve, reject) =>
      mongodb.connect(this.url2).then(db => {
        this.db = db
        const stream = db.collection('oplog.rs').find(filters, curOpts).stream()
        stream
          .on('data', log => {
            if (log.op === 'n' || log.ts.toNumber() === ts) return
            this.process(log)
          })
          .on('close', () => {
            util.log('Stream closed....')
            this.db = null
            db.close()
            resolve()
          })
          .on('error', err => {
            this.db = null
            db.close()
            console.log(`${new Date()}: Tailer.tail -> ${err} \n`)
            reject(err)
          })
      }))
  }

  /**
   * Process the log and sync to MySQL
   * @TODO delete relation field from relation table
   * @param {object} log - the log retrieved from oplog.rs
   * @returns {undefined}
   */
  process (log) {
    const def = this.defs.filter(def => log.ns === def.ns)[0]
    if (!def) return

    this.updateTimestamp(log.ts.toNumber())
    switch (log.op) {
      case 'i':
        util.log(`Insert a new record into ${def.ns}`)
        this.objectWalk(log.o, (def, item, relation) => {
          this.mysql.insertRelation(def, item, relation)
        })
        return this.mysql.insert(def, log.o)
      case 'u':
        let documentId
        if (log.o.$set || log.o.$unset) {
          documentId = log.o2[def.idName]
          util.log(`Update a record in ${def.ns} (${def.idName}=${log.o2[def.idName]})`)
          this.mysql.update(def, log.o2[def.idName], log.o.$set, log.o.$unset)
        } else {
          documentId = log.o[def.idName]
          const replaceFlag = true
          util.log(`Replace a record in ${def.ns} (${def.idName}=${log.o[def.idName]})`)
          this.mysql.insert(def, log.o, replaceFlag)
        }
        this.objectWalk(log.o, (def, item, relation, flag) => {
          this.mysql.updateRelation(def, item, relation, flag)
        }, documentId)
        break;
      case 'd':
        util.log(`Delete a record in ${def.ns} (${def.idName}=${log.o[def.idName]})`)
        return this.mysql.remove(def, log.o[def.idName])
      default:
        return
    }
  }

  async objectWalk (item, callback, parentId = null) {
    if (!callback) callback = function() {}
    const keys = Object.keys(item)
    let deleteFlag = true
    parent: for(let i = 0, key = keys[i]; i < keys.length; i++, key = keys[i]) {
      if (key === "_id") {
        if (item['_id']) {
          parentId = item['_id']
        }
        continue parent
      }
      if (item[key] instanceof Date) {
        continue parent
      }
      if (item[key] instanceof Object) {
        if (Array.isArray(item[key]) && this.checkRelation(key)) {
          child: for (
            let ii = 0, obj = item[key][ii];
            ii < item[key].length;
            ii++, obj = item[key][ii]
          ) {
            if (!obj) {
              continue child
            }
            let value = null
            const relation = this.relations[key]
            const def = this.relationDef[relation.def]
            if(obj instanceof Object) {
              obj.parent_id = parentId.toString()
              callback(def, obj, relation, deleteFlag)
              deleteFlag = false
              await this.objectWalk(obj, callback, parentId)
              continue child
            }
            value = []
            value.push(parentId.toString())
            value.push(obj)
            callback(def, value, relation, deleteFlag)
            deleteFlag = false
          }
        } else {
          await this.objectWalk(item[key], callback, parentId)
        }
        deleteFlag = true
        continue parent
      }
    }
  }

  checkRelation (key) {
    const relations = Object.keys(this.relations)
    return relations.find(relation => relation === key)
  }
}

module.exports = Tailer
