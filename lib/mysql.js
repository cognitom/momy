import mysql from 'mysql'
import { Timestamp } from 'mongodb'
import { logger } from './util.js'

/**
 * MySQL helper
 * @class
 */
export default class MySQL {
  /**
   * Constructor
   * @param {string} url - database url
   * @param {object} defs - syncing fields definitions
   */
  constructor (url, defs) {
    this.url = url || 'mysql://localhost/test?user=root'
    this.defs = defs
    this.dbName = this.url.split(/\/|\?/)[3]
    this.con = null
  }

  /**
   * Insert the record
   * @param {object} def - definition of fields
   * @param {object} item - the data of the record to insert
   * @param {boolean} replaceFlag - set true to replace the record
   * @param {function} callback - callback
   */
  insert (def, item, replaceFlag, callback) {
    if (typeof replaceFlag === 'function') {
      callback = replaceFlag
      replaceFlag = false
    }
    return this.run(insertSql(def, item, replaceFlag), callback)
  }

  /**
   * Run the SQL, and log it when it fails
   * @param {string} sql - SQL string, or null to do nothing
   * @param {function} callback - callback
   * @returns {Promise} with no value
   */
  run (sql, callback) {
    const promise = !sql
      ? Promise.resolve()
      : this.query(sql).then(() => {}, err => {
        logger.error(sql)
        throw err
      })

    if (callback) promise.then(() => callback())
    return promise
  }

  /**
   * Write the data and the timestamp in one transaction, by one round trip
   * If MySQL rejects the data, roll it back, then save the timestamp alone so that the entry is skipped.
   * Connection errors are thrown as they are. MySQL rolls back the transaction when the connection is lost.
   * @param {string} sql - SQL string of the data, or null to save the timestamp only
   * @param {Timestamp} ts - a new timestamp
   * @returns {Promise} with the error of the rejected data, or null
   */
  async applyWithTimestamp (sql, ts) {
    const tsSql = this.timestampSql(ts)
    if (!sql) {
      await this.run(tsSql)
      return null
    }
    try {
      await this.query(`START TRANSACTION; ${sql} ${tsSql} COMMIT;`)
      return null
    } catch (err) {
      if (err.fatal || !err.code) throw err
      // err.index is the index of the failed statement
      logger.error(err.index === 2 ? tsSql : sql)
      // The statements after the failed one were not run, so the transaction is still open
      await this.run(`ROLLBACK; ${tsSql}`)
      return err
    }
  }

  /**
   * Create tables
   * @returns {Promise} with no value
   */
  createTable () {
    // TODO: Create mongo_to_mysql table only if not exists
    const sql0 = 'DROP TABLE IF EXISTS mongo_to_mysql; ' +
      'CREATE TABLE mongo_to_mysql (service varchar(20), timestamp BIGINT);'
    const sql1 = 'INSERT INTO mongo_to_mysql ' +
      `(service, timestamp) VALUES ("${this.dbName}", 0);`
    const sql2 = this.defs.map(def => {
      const fields = def.fields.map(field =>
        `\`${field.distName}\` ${field.type}${field.primary ? ' PRIMARY KEY' : ''}`)
      return `DROP TABLE IF EXISTS \`${def.distName}\`; ` +
        `CREATE TABLE \`${def.distName}\` (${fields.join(', ')});`
    }).join('')

    return this.query(sql0)
      .then(() => this.query(sql1))
      .then(() => this.query(sql2))
  }

  /**
   * Read timestamp
   * The BIGINT is read as a decimal string, since a JS number cannot hold it exactly
   * @returns {Promise} with a Timestamp, or null unless saved
   */
  readTimestamp () {
    const q = 'SELECT CAST(timestamp AS CHAR) AS timestamp FROM mongo_to_mysql ' +
      `WHERE service = '${this.dbName}'`
    return this.query(q)
      .then(results => {
        const str = results[0] && results[0].timestamp
        if (!str || str === '0') return null
        return Timestamp.fromString(str, 10)
      })
      .catch(err => {
        logger.error(q)
        throw err
      })
  }

  /**
   * Update timestamp
   * @param {Timestamp} ts - a new timestamp
   * @returns {Promise} with no value
   */
  updateTimestamp (ts) {
    return this.run(this.timestampSql(ts))
  }

  /**
   * SQL to update timestamp
   * @param {Timestamp} ts - a new timestamp
   * @returns {string} SQL string
   */
  timestampSql (ts) {
    // Write the decimal string as it is, so that the timestamp never goes through a JS number
    return `UPDATE mongo_to_mysql SET timestamp = ${ts.toString()} WHERE service = '${this.dbName}';`
  }

  /**
   * Connect to MySQL
   * @returns {connection} MySQL connection
   */
  getConnection () {
    if (this.con &&
      this.con._socket &&
      this.con._socket.readable &&
      this.con._socket.writable) return this.con

    const params = 'multipleStatements=true'
    const url = this.url + (/\?/.test(this.url) ? '&' : '?') + params
    const con = mysql.createConnection(url)

    logger.info('Connect to MySQL...')
    con.connect(function (err) {
      if (err) logger.error(`SQL CONNECT ERROR: ${err}`)
    })
    con.on('close', () => logger.info('SQL CONNECTION CLOSED.'))
    con.on('error', err => logger.error(`SQL CONNECTION ERROR: ${err}`))

    return (this.con = con)
  }

  /**
   * Close the connection if exists
   */
  clearConnection () {
    if (this.con) {
      this.con.end()
      this.con = null
    }
  }

  /**
   * Query method with promise
   * @param {string} sql - SQL string
   * @returns {Promise} with results
   */
  query (sql) {
    return new Promise((resolve, reject) => {
      this.getConnection()
        .query(sql, (err, results) => {
          if (err) reject(err)
          else resolve(results)
        })
    })
  }
}

/**
 * SQL to insert the record
 * @param {object} def - definition of fields
 * @param {object} item - the data of the record to insert
 * @param {boolean} replaceFlag - set true to replace the record
 * @returns {string} SQL string
 */
export function insertSql (def, item, replaceFlag) {
  const command = replaceFlag ? 'REPLACE' : 'INSERT'
  const fs = def.fields.map(field => '`' + field.distName + '`')
  const vs = def.fields.map(field => field.convert(getFieldVal(field.name, item)))
  return `${command} INTO \`${def.distName}\`` +
    ` (${fs.join(', ')}) VALUES (${vs.join(', ')});`
}

/**
 * SQL to update the record
 * @param {object} def - definition of fields
 * @param {string} id - the id of the record to update
 * @param {object} item - the columns to update
 * @param {object} unsetItems - the columns to drop
 * @returns {string} SQL string, or null if no column to update
 */
export function updateSql (def, id, item, unsetItems) {
  const fields = def.fields.filter(field =>
    (!!item && typeof getFieldVal(field.name, item) !== 'undefined') ||
    (!!unsetItems && typeof getFieldVal(field.name, unsetItems) !== 'undefined'))
  const sets = fields.map(field => {
    const val = field.convert(getFieldVal(field.name, item))
    return `\`${field.distName}\` = ${val}`
  })
  if (!sets.length) return null

  const setsStr = sets.join(', ')
  const id2 = def.idType === 'number' ? id : `'${id}'`
  return `UPDATE \`${def.distName}\` SET ${setsStr} WHERE ${def.idDistName} = ${id2};`
}

/**
 * SQL to remove the record
 * @param {object} def - definition of fields
 * @param {string} id - the id of the record to remove
 * @returns {string} SQL string
 */
export function removeSql (def, id) {
  const id2 = def.idType === 'number' ? id : `'${id}'`
  return `DELETE FROM \`${def.distName}\` WHERE ${def.idDistName} = ${id2};`
}

function getFieldVal (name, record) {
  return name.split('.').reduce((p, c) => p && p[c], record)
}
