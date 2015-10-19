var mysql = require('mysql')
var _     = require('underscore')
var util  = require('util')

var con = null // MySQL connection reference

/**
 * MySQL helper
 * @class
 */
function MySQL(config, sync_fields) {
  if (!con || !con._socket || !con._socket.readable || !con._socket.writable)
    con = connectToMySQL(config)
  this.config = config
  this.sync_fields = sync_fields
}

/**
 * Insert the record
 * @param {object} item - the data of the record to insert
 * @param {function} callback
 */
MySQL.prototype.insert = function(item, callback) {
  var sync_fields = this.sync_fields
  var keys = _.keys(sync_fields)
  var fields = [], values = []
  _.each(item, function(val, key) {
    if (_.contains(keys, key)) {
      fields.push(key)
      switch (sync_fields[key]) {
        case 'string':
          val = (val || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')
          values.push('"' + val + '"')
          break
        case 'int':
          values.push(val || 0)
          break
      }
    }
  })
  var sql = 'INSERT INTO ' + this.config.table
    + ' (' + fields.join(', ') + ') VALUES (' + values.join(', ') + ');'
  con.query(sql, function(err, results) {
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
 * @param {object} unset_item - the columns to drop
 * @param {function} callback
 */
MySQL.prototype.update = function(id, item, unset_items, callback) {
  var sync_fields = this.sync_fields
  var keys = _.keys(sync_fields)
  var sets = []

  _.each(item, function(val, key) {
    if (_.contains(keys, key)) {
      switch (sync_fields[key]) {
        case 'string':
          val = (val || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')
          sets.push(key + ' = "' + val + '"')
          break
        case 'int':
          sets.push(key + ' = ' + (val || 0))
          break
      }
    }
  })

  _.each(unset_items, function(val, key) {
    if (_.contains(keys, key)) {
      switch (sync_fields[key]) {
        case 'string': sets.push(key + ' = ""'); break
        case 'int': sets.push(key + ' = 0'); break
      }
    }
  })

  if (sets.length === 0) return

  var table = this.config.table
  var id = sync_fields['_id'] === 'int' ? id : '\'' + id + '\''
  var sql = 'UPDATE ' + table
    + ' SET ' + sets.join(', ') + ' WHERE _id = ' + id + ';'

  con.query(sql, function(err, results) {
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
 * @param {function} callback
 */
MySQL.prototype.remove = function(id, callback) {
  var id = this.sync_fields['_id'] === 'int' ? id : '\'' + id + '\''
  var sql = 'DELETE FROM ' + this.config.table + ' WHERE _id = ' + id + ';'
  con.query(sql, function(err, results) {
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
MySQL.prototype.createTable = function() {
  var fields = []
  _.each(this.sync_fields, function(val, key) {
    switch (val) {
      case 'string': fields.push(key + ' VARCHAR(1000)'); break
      case 'int':    fields.push(key + ' BIGINT'); break
    }
  })
  var sql = 'DROP TABLE IF EXISTS ' + this.config.table + '; '
    + 'CREATE TABLE ' + this.config.table + ' (' + fields.join(', ') + ');'
  var sql2 = 'DROP TABLE IF EXISTS mongo_to_mysql; '
    + 'CREATE TABLE mongo_to_mysql (service varchar(20), timestamp BIGINT);'
  var sql3 = 'INSERT INTO mongo_to_mysql (service, timestamp)'
    + ' VALUES ("' + this.config.table + '", 0);'

  return new Promise(function(resolve, reject) {
    con.query(sql, function(err) {
      if (err) util.log(err)
      con.query(sql2, function() {
        con.query(sql3, function() {
          resolve()
        })
      })
    })
  })
}


/**
 * Read timestamp
 * @returns {Promise} with timestamp
 */
MySQL.prototype.readTimestamp = function() {
  var self = this
  var q = 'SELECT timestamp FROM mongo_to_mysql'
    + ' WHERE service = "' + this.config.table + '"'

  return new Promise(function(resolve, reject) {
    con.query(q, function(err, results) {
      if (results && results[0])
        resolve(results[0].timestamp)
      else
        reject(err)
    })
  })
}

/**
 * Update timestamp
 * @returns {number} ts - a new timestamp
 */
MySQL.prototype.updateTimestamp = function(ts) {
  var q = 'UPDATE mongo_to_mysql SET timestamp = ' + ts
    + ' WHERE service = \'' + this.config.table + '\';'
  con.query(q)
}

/**
 * Connect to MySQL
 * @param {object} config - config for connecting to db
 * @returns {connection} opened connection
 */
function connectToMySQL(config) {
  util.log('Connect to MySQL...')
  var connection = mysql.createConnection({
    host:               config.host,
    user:               config.user,
    password:           config.password,
    multipleStatements: true
  })

  connection.connect(function(err) {
    util.log(err ? 'SQL CONNECT ERROR: ' + err : 'SQL CONNECT SUCCESSFUL.')
  })
  connection.on('close', function(err) { util.log('SQL CONNECTION CLOSED.') })
  connection.on('error', function(err) { util.log('SQL CONNECTION ERROR: ' + err) })
  connection.query('USE ' + config.db)

  return connection
}

module.exports = MySQL
