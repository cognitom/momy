import { MongoClient, Timestamp } from 'mongodb'
import { logger, getDbNameFromUri } from './util.js'
import MySQL from './mysql.js'
import { createDefs } from './defs.js'

const RECONNECT_INTERVAL = 1000
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

/**
 * Log and ignore errors from SQL statements. Rethrow connection errors, which are fatal in mysql.
 * @param {Promise} promise - a promise of MySQL query
 * @param {string} message - logged when ignored
 * @returns {Promise} with no value
 */
async function skipSqlError (promise, message) {
  try {
    await promise
  } catch (err) {
    if (err.fatal || !err.code) throw err
    logger.error(`${message}: ${err.message}`)
  }
}

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
    this.mysql.clearConnection()
    this.client.close()

    if (!this.cliMode) return

    if (err) logger.error(err)
    logger.info('Bye')
    process.exit()
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
  async importCollection (def) {
    logger.info(`Import records in ${def.ns}`)
    const db = this.client.db(this.dbName)
    for await (const item of db.collection(def.name).find()) {
      await this.mysql.insert(def, item)
    }
  }

  /**
   * Check the latest log in Mongo, then catch the timestamp up in MySQL
   * @param {number} ts - unless null then skip updating in MySQL
   * @param {boolean} skipUpdateMySQL - skip update in MySQL
   * @returns {Promise} with no value
   */
  async updateTimestamp (ts, skipUpdateMySQL) {
    if (!ts) {
      const db = this.client.db('local')
      const item = await db.collection('oplog.rs')
        .find().sort({ $natural: -1 }).limit(1).next()
      ts = item.ts.toNumber()
    }
    this.lastTs = ts
    if (!skipUpdateMySQL) await this.mysql.updateTimestamp(ts)
  }

  /**
   * Tail forever
   * @returns {Promise} with no value
   */
  async tailForever () {
    for (let counter = 0; ; counter++) {
      if (counter) await sleep(RECONNECT_INTERVAL)
      logger.info(counter ? 'Reconnect to MongoDB...' : 'Connect to MongoDB...')
      await this.tail()
    }
  }

  /**
   * Tail the log of Mongo by tailable cursors
   * @returns {Promise} with no value
   */
  async tail () {
    const ts = this.lastTs
    const nss = this.defs.map(def => def.ns)
    const filters = {
      ns: { $in: nss },
      ts: { $gt: Timestamp.fromNumber(ts) }
    }
    const curOpts = { tailable: true, awaitData: true }

    logger.info(`Begin to watch... (from ${ts})`)
    const db = this.client.db('local')
    const cursor = db.collection('oplog.rs').find(filters, curOpts)
    try {
      // Wait for MySQL before reading the next log, so that logs never pile up in memory
      for await (const log of cursor) {
        if (log.op === 'n' || log.ts.toNumber() === ts) continue
        await this.process(log)
      }
    } finally {
      await cursor.close()
      logger.info('Stream closed....')
    }
  }

  /**
   * Process the log and sync to MySQL
   * @param {object} log - the log retrieved from oplog.rs
   * @returns {Promise} with no value
   */
  async process (log) {
    const def = this.defs.find(def => log.ns === def.ns)
    if (!def) return

    const ts = log.ts.toNumber()
    // Skip an entry MySQL rejects, so that it does not stop syncing on every restart
    await skipSqlError(this.apply(def, log), `Skip the log in ${def.ns} (ts=${ts})`)
    await skipSqlError(this.updateTimestamp(ts), `Failed to save the timestamp (ts=${ts})`)
  }

  /**
   * Apply the log to MySQL
   * @param {object} def - definition of fields
   * @param {object} log - the log retrieved from oplog.rs
   * @returns {Promise} with no value
   */
  apply (def, log) {
    switch (log.op) {
      case 'i': {
        // REPLACE keeps it idempotent: the entry is replayed after a crash before the timestamp is saved
        const replaceFlag = true
        logger.info(`Insert a new record into ${def.ns}`)
        return this.mysql.insert(def, log.o, replaceFlag)
      }
      case 'u':
        switch (log.o.$v) {
          case 2: // MongoDB 5.0 or newer
            if (log.o.diff && (log.o.diff.i || log.o.diff.u || log.o.diff.d)) {
              logger.info(`Update a record in ${def.ns} (${def.idName}=${log.o2[def.idName]})`)
              const toSet = Object.assign({}, log.o.diff.i, log.o.diff.u)
              const toUnset = log.o.diff.d
              return this.mysql.update(def, log.o2[def.idName], toSet, toUnset)
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
    return Promise.resolve()
  }
}
