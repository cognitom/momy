'use strict'

const mysql = require('mysql')
const util = require('util')

/**
 * MySQL helper
 * @class
 */
class MySQL {
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
    const command = replaceFlag ? 'REPLACE' : 'INSERT'
    const fs = def.fields.map(field => '`' + field.distName + '`')
    const vs = def.fields.map(field => field.convert(getFieldVal(field.name, item)))
    const sql = `${command} INTO \`${def.distName}\`` +
      ` (${fs.join(', ')}) VALUES (${vs.join(', ')});`
    const promise = this.query(sql)
      .catch(err => {
        util.log(sql)
        throw err
      })

    if (callback) promise.then(() => callback())
  }

  /**
   * Update the record
   * @param {object} def - definition of fields
   * @param {string} id - the id of the record to update
   * @param {object} item - the columns to update
   * @param {object} unsetItems - the columns to drop
   * @param {function} callback - callback
   */
  update (def, id, item, unsetItems, callback) {
    const fields = def.fields.filter(field =>
      !!item && typeof getFieldVal(field.name, item) !== 'undefined' ||
      !!unsetItems && typeof getFieldVal(field.name, unsetItems) !== 'undefined')
    const sets = fields.map(field => {
      const val = field.convert(getFieldVal(field.name, item))
      return `\`${field.distName}\` = ${val}`
    })
    if (!sets.length) return

    const setsStr = sets.join(', ')
    const id2 = def.idType === 'number' ? id : `'${id}'`
    const sql = `UPDATE \`${def.distName}\` SET ${setsStr} WHERE ${def.idDistName} = ${id2};`
    const promise = this.query(sql)
      .catch(err => {
        util.log(sql)
        throw err
      })

    if (callback) promise.then(() => callback())
  }

  /**
   * Remove the record
   * @param {object} def - definition of fields
   * @param {string} id - the id of the record to remove
   * @param {function} callback - callback
   */
  remove (def, id, callback) {
    const id2 = def.idType === 'number' ? id : `'${id}'`
    const sql = `DELETE FROM \`${def.distName}\` WHERE ${def.idDistName} = ${id2};`
    const promise = this.query(sql)
      .catch(err => {
        util.log(sql)
        throw err
      })

    if (callback) promise.then(() => callback())
  }

  /**
   * Create tables
   * @returns {Promise} with no value
   */
  createTable () {
    // TODO: Create mongo_to_mysql table only if not exists
    const sql0 = 'DROP TABLE IF EXISTS mongo_to_mysql; ' +
      'CREATE TABLE mongo_to_mysql (service varchar(20), timestamp BIGINT);'
    const sql1 = `INSERT INTO mongo_to_mysql ` +
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
   * @returns {Promise} with timestamp
   */
  readTimestamp () {
    let q = 'SELECT timestamp FROM mongo_to_mysql' +
      ` WHERE service = '${this.dbName}'`
    return this.query(q)
      .then(results => results[0] && results[0].timestamp || 0)
      .catch(err => {
        util.log(q)
        throw err
      })
  }

  /**
   * Update timestamp
   * @param {number} ts - a new timestamp
   */
  updateTimestamp (ts) {
    let q = `UPDATE mongo_to_mysql SET timestamp = ${ts}` +
      ` WHERE service = '${this.dbName}';`
    this.getConnection()
      .query(q)
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

    util.log('Connect to MySQL...')
    con.connect(function (err) {
      if (err) util.log(`SQL CONNECT ERROR: ${err}`)
    })
    con.on('close', () => util.log('SQL CONNECTION CLOSED.'))
    con.on('error', err => util.log(`SQL CONNECTION ERROR: ${err}`))

    return (this.con = con)
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

function getFieldVal (name, record) {
  return name.split('.').reduce((p, c) => p && p[c], record)
}

module.exports = MySQL
