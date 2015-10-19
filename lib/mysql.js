'use strict'

const
  mysql = require('mysql'),
  _     = require('underscore'),
  util  = require('util')

let con = null // MySQL connection reference

/**
 * MySQL helper
 * @class
 */
class MySQL {
  /**
   * Constructor
   * @param {object} config - config
   * @param {object} syncFields - syncing fields definitions
   */
  constructor (config, syncFields) {
    if (!con || !con._socket || !con._socket.readable || !con._socket.writable)
      con = connectToMySQL(config)
    this.config = config || {}
    this.syncFields = syncFields || {}
  }

  /**
   * Insert the record
   * @param {object} item - the data of the record to insert
   * @param {function} callback - callback
   */
  insert (item, callback) {
    const
      syncFields = this.syncFields,
      keys       = _.keys(syncFields)
    let fields = []
    let values = []
    _.each(item, (val, key) => {
      if (_.contains(keys, key)) {
        fields.push(key)
        switch (syncFields[key]) {
          case 'string':
            val = (val || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')
            values.push(`"${ val }"`)
            break
          case 'int':
            values.push(val || 0)
            break
          default: break
        }
      }
    })
    const sql = `INSERT INTO ${ this.config.table }`
      + ` (${ fields.join(', ') }) VALUES (${ values.join(', ') });`
    con.query(sql, err => {
      if (err) {
        util.log(sql)
        throw err
      }
      if (callback) callback()
    })
  }

  /**
   * Update the record
   * @param {string} id - the id of the record to update
   * @param {object} item - the columns to update
   * @param {object} unsetItems - the columns to drop
   * @param {function} callback - callback
   */
  update (id, item, unsetItems, callback) {
    const
      syncFields = this.syncFields,
      keys       = _.keys(syncFields)
    let sets = []

    _.each(item, (val, key) => {
      if (!_.contains(keys, key)) return
      switch (syncFields[key]) {
        case 'string':
          val = (val || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')
          sets.push(`${ key } = "${ val }"`)
          break
        case 'int':
          sets.push(`${ key } = ${ val || 0 }`)
          break
        default: break
      }
    })

    _.each(unsetItems, (val, key) => {
      if (_.contains(keys, key)) {
        switch (syncFields[key]) {
          case 'string': sets.push(`${ key } = ""`); break
          case 'int': sets.push(`${ key } = 0`); break
          default: break
        }
      }
    })

    if (sets.length === 0) return

    const
      table = this.config.table,
      id2   = syncFields._id === 'int' ? id : `'${ id }'`,
      sql   = `UPDATE ${ table } SET ${ sets.join(', ') } WHERE _id = ${ id2 };`

    con.query(sql, err => {
      if (err) {
        util.log(sql)
        throw err
      }
      if (callback) callback()
    })
  }

  /**
   * Remove the record
   * @param {string} id - the id of the record to remove
   * @param {function} callback - callback
   */
  remove (id, callback) {
    const
      id2 = this.syncFields._id === 'int' ? id : `'${ id }'`,
      sql = `DELETE FROM ${ this.config.table } WHERE _id = ${ id2 };`
    con.query(sql, err => {
      if (err) {
        util.log(sql)
        throw err
      }
      if (callback) callback()
    })
  }

  /**
   * Create tables
   * @returns {Promise} with no value
   */
  createTable () {
    let fields = []
    _.each(this.syncFields, (val, key) => {
      switch (val) {
        case 'string': fields.push(`${ key } VARCHAR(1000)`); break
        case 'int':    fields.push(`${ key } BIGINT`); break
        default: break
      }
    })
    const
      sql = `DROP TABLE IF EXISTS ${ this.config.table }; `
        + `CREATE TABLE ${ this.config.table } (${ fields.join(', ') });`,
      sql2 = 'DROP TABLE IF EXISTS mongo_to_mysql; '
        + 'CREATE TABLE mongo_to_mysql (service varchar(20), timestamp BIGINT);',
      sql3 = 'INSERT INTO mongo_to_mysql (service, timestamp)'
        + ` VALUES ("${ this.config.table }", 0);`

    return new Promise(resolve => {
      con.query(sql, err => {
        if (err) util.log(err)
        con.query(sql2, () => con.query(sql3, () => resolve()))
      })
    })
  }


  /**
   * Read timestamp
   * @returns {Promise} with timestamp
   */
  readTimestamp () {
    let q = 'SELECT timestamp FROM mongo_to_mysql'
      + ` WHERE service = '${ this.config.table }'`

    return new Promise((resolve, reject) => {
      con.query(q, (err, results) => {
        if (results && results[0])
          resolve(results[0].timestamp)
        else
          reject(err)
      })
    })
  }

  /**
   * Update timestamp
   * @param {number} ts - a new timestamp
   */
  updateTimestamp (ts) {
    let q = `UPDATE mongo_to_mysql SET timestamp = ${ ts }`
      + ` WHERE service = '${ this.config.table }';`
    con.query(q)
  }
}

/**
 * Connect to MySQL
 * @param {object} config - config for connecting to db
 * @returns {connection} opened connection
 */
function connectToMySQL(config) {
  util.log('Connect to MySQL...')
  const c = mysql.createConnection({
    host: config.host,
    user: config.user,
    password: config.password,
    multipleStatements: true
  })

  c.connect(function(err) {
    util.log(err ? `SQL CONNECT ERROR: ${ err }` : 'SQL CONNECT SUCCESSFUL.')
  })
  c.on('close', () => util.log('SQL CONNECTION CLOSED.'))
  c.on('error', err => util.log(`SQL CONNECTION ERROR: ${ err }`))
  c.query('USE ' + config.db)

  return c
}

module.exports = MySQL
