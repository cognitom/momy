import { MongoClient, Timestamp } from 'mongodb'
import { logger, getDbNameFromUri } from './util.js'
import MySQL from './mysql.js'
import { createDefs } from './defs.js'

/**
 * Tailer
 * @class
 */
export default class Tailer {
  /**
   * Constructor
   * @param {object} config - configulation options
   * @param {boolean} cliMode - set false for testing
   */
  constructor (config, cliMode) {
    const opts = {
      prefix: config.prefix || '',
      fieldCase: config.fieldCase || '',
      exclusions: config.exclusions || '',
      inclusions: config.inclusions || ''
    }
    this.cliMode = cliMode === undefined ? true : !!cliMode
    this.url = config.src || 'mongodb://localhost:27017/test'
    this.dbName = getDbNameFromUri(this.url)
    this.defs = createDefs(config.collections, this.dbName, opts)
    this.lastTs = 0
    this.mysql = new MySQL(config.dist, this.defs)
    this.client = null
  }

  /**
   * Start tailing
   * @param {boolean} forever - set false for testing
   */
  start (forever) {
    this.client = new MongoClient(this.url)
    forever = forever === undefined ? true : !!forever
    this.mysql.readTimestamp()
      .then(ts => this.updateTimestamp(ts, true))
      .then(() => forever ? this.tailForever() : this.tail())
      .catch(err => this.stop(err))
  }

  /**
   * Import all and start tailing
   * @param {boolean} forever - set false for testing
   */
  importAndStart (forever) {
    this.client = new MongoClient(this.url)
    forever = forever === undefined ? true : !!forever
    this.mysql.createTable()
      .then(() => this.importAll())
      .then(() => this.updateTimestamp())
      .then(() => forever ? this.tailForever() : this.tail())
      .catch(err => this.stop(err))
  }

  stop (err) {
    if (this.cliMode) {
      if (err) logger.error(err)
      logger.info('Bye')
      process.exit()
      return
    }
    this.mysql.clearConnection()
    this.client.close()
  }

  /**
   * Import all
   * @returns {Promise} with no value
   */
  importAll () {
    logger.info('Begin to import...')
    let promise = Promise.resolve()
    this.defs.forEach(def => {
      promise = promise.then(() => this.importCollection(def))
    })
    promise.then(() => {
      logger.info('Done.')
    })
    return promise
  }

  /**
   * Import collection
   * @param {object} def - definition of fields
   * @returns {Promise} with no value
   */
  importCollection (def) {
    logger.info(`Import records in ${def.ns}`)
    return new Promise(resolve => {
      const db = this.client.db(this.dbName)
      const stream = db.collection(def.name).find().stream()
      stream
        .on('data', item => {
          stream.pause()
          this.mysql.insert(def, item, () => stream.resume())
        })
        .on('end', () => {
          resolve()
        })
    })
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
    return new Promise(resolve => {
      const db = this.client.db('local')
      db.collection('oplog.rs').find().sort({ $natural: -1 }).limit(1)
        .next()
        .then(item => {
          ts = item.ts.toNumber()
          this.lastTs = ts
          if (!skipUpdateMySQL) this.mysql.updateTimestamp(ts)
          resolve()
        })
    })
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
            logger.info(message)
            return this.tail()
          })
          .catch(err => reject(err))
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

    logger.info(`Begin to watch... (from ${ts})`)
    return new Promise((resolve, reject) => {
      const db = this.client.db('local')
      const stream = db.collection('oplog.rs').find(filters, curOpts).stream()
      stream
        .on('data', log => {
          if (log.op === 'n' || log.ts.toNumber() === ts) return
          this.process(log)
        })
        .on('close', () => {
          logger.info('Stream closed....')
          resolve()
        })
        .on('error', err => {
          reject(err)
        })
    })
  }

  /**
   * Process the log and sync to MySQL
   * @param {object} log - the log retrieved from oplog.rs
   * @returns {undefined}
   */
  process (log) {
    const def = this.defs.filter(def => log.ns === def.ns)[0]
    if (!def) return

    this.updateTimestamp(log.ts.toNumber())
    switch (log.op) {
      case 'i':
        logger.info(`Insert a new record into ${def.ns}`)
        return this.mysql.insert(def, log.o)
      case 'u':
        switch (log.o.$v) {
          case 2: // MongoDB 5.0 or newer
            if (log.o.diff && (log.o.diff.u || log.o.diff.d)) {
              logger.info(`Update a record in ${def.ns} (${def.idName}=${log.o2[def.idName]})`)
              return this.mysql.update(def, log.o2[def.idName], log.o.diff.u, log.o.diff.d)
            } else {
              const replaceFlag = true
              logger.info(`Replace a record in ${def.ns} (${def.idName}=${log.o[def.idName]})`)
              return this.mysql.insert(def, log.o, replaceFlag)
            }
          default: // MongoDB 4.x or older
            if (log.o.$set || log.o.$unset) {
              logger.info(`Update a record in ${def.ns} (${def.idName}=${log.o2[def.idName]})`)
              return this.mysql.update(def, log.o2[def.idName], log.o.$set, log.o.$unset)
            } else {
              const replaceFlag = true
              logger.info(`Replace a record in ${def.ns} (${def.idName}=${log.o[def.idName]})`)
              return this.mysql.insert(def, log.o, replaceFlag)
            }
        }
      case 'd':
        logger.info(`Delete a record in ${def.ns} (${def.idName}=${log.o[def.idName]})`)
        return this.mysql.remove(def, log.o[def.idName])
    }
  }
}
