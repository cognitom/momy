'use strict'

const
  mysql = require('mysql'),
  util  = require('util')

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
    this.url    = url || 'mysql://localhost/test?user=root'
    this.defs   = defs
    this.dbName = this.url.split(/\/|\?/)[3]
    this.con    = null
  }

  /**
   * Insert the record
   * @param {object} def - definition of fields
   * @param {object} item - the data of the record to insert
   * @param {function} callback - callback
   */
  insert (def, item, callback) {
    const
      fs = def.fields.map(field => field.name),
      vs = def.fields.map(field => {
        let val = item[field.name]
        switch (field.type) {
          case 'string':
            val = (val || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')
            return `"${ val }"`
          case 'number':
          case 'boolean':
            return val || 0
          default:
            return
        }
      }),
      sql = `INSERT INTO ${ def.distName }`
        + ` (${ fs.join(', ') }) VALUES (${ vs.join(', ') });`,
      promise = this.query(sql)
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
    const
      fields = def.fields.filter(field =>
        !!item       && typeof item[field.name]       != 'undefined' ||
        !!unsetItems && typeof unsetItems[field.name] != 'undefined'),
      sets = fields.map(field => {
        let val = item[field.name]
        switch (field.type) {
          case 'string':
            val = (val || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')
            return `${ field.name } = "${ val }"`
          case 'number':
          case 'boolean':
            return `${ field.name } = ${ val || 0 }`
          default:
            return
        }
      })

    if (!sets.length) return

    const
      setsStr = sets.join(', '),
      id2     = def.idType == 'number' ? id : `'${ id }'`,
      sql     = `UPDATE ${ def.distName } SET ${ setsStr } WHERE _id = ${ id2 };`,
      promise = this.query(sql)
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
    const
      id2 = def.idType == 'number' ? id : `'${ id }'`,
      sql = `DELETE FROM ${ def.distName } WHERE _id = ${ id2 };`,
      promise = this.query(sql)
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
    const
      // TODO: Create mongo_to_mysql table only if not exists
      sql0 = 'DROP TABLE IF EXISTS mongo_to_mysql; '
        + 'CREATE TABLE mongo_to_mysql (service varchar(20), timestamp BIGINT);',
      sql1 = `INSERT INTO mongo_to_mysql `
        + `(service, timestamp) VALUES ("${ this.dbName }", 0);`,
      sql2 = this.defs.map(def => {
        const fields = def.fields.map(field => {
          switch (field.type) {
            case 'string': return `${ field.name } VARCHAR(1000)`
            case 'number': return `${ field.name } BIGINT`
            case 'boorean': return `${ field.name } TINYINT`
            default: return
          }
        })
        return `DROP TABLE IF EXISTS ${ def.distName }; `
          + `CREATE TABLE ${ def.distName } (${ fields.join(', ') });`
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
    let q = 'SELECT timestamp FROM mongo_to_mysql'
      + ` WHERE service = '${ this.dbName }'`
    return this.query(q)
      .then(results => results[0] && results[0].timestamp || 0)
      .catch(err => {
        util.log(sql)
        throw err
      })
  }

  /**
   * Update timestamp
   * @param {number} ts - a new timestamp
   */
  updateTimestamp (ts) {
    let q = `UPDATE mongo_to_mysql SET timestamp = ${ ts }`
      + ` WHERE service = '${ this.dbName }';`
    this.getConnection()
      .query(q)
  }

  /**
   * Connect to MySQL
   */
  getConnection () {
    if (this.con && this.con._socket
      && this.con._socket.readable && this.con._socket.writable)
      return this.con

    const
      params = 'multipleStatements=true',
      url = this.url + (/\?/.test(this.url) ? '&' : '?') + params,
      con = mysql.createConnection(url)

    util.log('Connect to MySQL...')
    con.connect(function(err) {
      if (err) util.log(`SQL CONNECT ERROR: ${ err }`)
    })
    con.on('close', () => util.log('SQL CONNECTION CLOSED.'))
    con.on('error', err => util.log(`SQL CONNECTION ERROR: ${ err }`))

    return this.con = con
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


module.exports = MySQL
