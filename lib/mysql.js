var mysql = require('mysql')
var _     = require('underscore')
var util  = require('util')

function MySQL(app, callback) {
  util.log('Connect to MySQL...')
  this.config = app.config
  if (app.refresh)
    this.create_table(function() { callback() })
  else
    this.read_timestamp(function() { callback() })
  this.app = app
}

MySQL.prototype.getConnection = function() {
  if (this.client && this.client._socket
    && this.client._socket.readable
    && this.client._socket.writable)
    return this.client

  var config = this.config.mysql
  var client = mysql.createConnection({
    host:               config.host,
    user:               config.user,
    password:           config.password,
    multipleStatements: true
  })

  client.connect(function(err) {
    util.log(err ? 'SQL CONNECT ERROR: ' + err : 'SQL CONNECT SUCCESSFUL.')
  })
  client.on('close', function(err) { util.log('SQL CONNECTION CLOSED.') })
  client.on('error', function(err) { util.log('SQL CONNECTION ERROR: ' + err) })
  client.query('USE ' + config.db)

  return this.client = client
}

MySQL.prototype.insert = function(item, callback) {
  item = transform(item)

  var self = this
  var keys = _.keys(self.config.sync_fields)
  var fields = [], values = []
  _.each(item, function(val, key) {
    if (_.contains(keys, key)) {
      fields.push(key)
      switch (self.config.sync_fields[key]) {
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
  var fields_str = fields.join(', ')
  var values_str = values.join(', ')
  var sql = 'INSERT INTO ' + self.config.mysql.table
    + ' (' + fields_str + ') VALUES (' + values_str + ');'
  var conn = self.getConnection()
  conn.query(sql, function(err, results) {
    if (err) {
      util.log(sql)
      throw err
    }
    return callback()
  })
}

MySQL.prototype.update = function(id, item, unset_items, callback) {
  if (item) item = transform(item)
  if (unset_items) unset_items = transform(unset_items)

  var self = this
  var table = self.config.mysql.table
  var id = self.config.sync_fields['_id'] === 'int' ? id : '\'' + id + '\''
  var keys = _.keys(self.config.sync_fields)
  var sets = []

  _.each(item, function(val, key) {
    if (_.contains(keys, key)) {
      switch (self.config.sync_fields[key]) {
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
      switch (self.config.sync_fields[key]) {
        case 'string': sets.push(key + ' = ""'); break
        case 'int': sets.push(key + ' = 0'); break
      }
    }
  })

  if (sets.length === 0) return

  var sets_str = sets.join(', ')
  var sql = 'UPDATE ' + table + ' SET ' + sets_str + ' WHERE _id = ' + id + ';'
  var conn = self.getConnection()
  conn.query(sql, function(err, results) {
    if (err) {
      util.log(sql)
      throw err
    }
    return callback()
  })
}

MySQL.prototype.remove = function(id, callback) {
  var table = this.config.mysql.table
  var id = this.config.sync_fields['_id'] === 'int' ? id : '\'' + id + '\''
  var sql = 'DELETE FROM ' + table + ' WHERE _id = ' + id + ';'
  var conn = this.getConnection()
  conn.query(sql, function(err, results) {
    if (err) {
      util.log(sql)
      throw err
    }
    return callback()
  })
}

MySQL.prototype.create_table = function(callback) {
  var table = this.config.mysql.table
  var fields = []
  _.each(this.config.sync_fields, function(val, key) {
    switch (val) {
      case 'string': fields.push(key + ' VARCHAR(1000)'); break
      case 'int':    fields.push(key + ' BIGINT'); break
    }
  })
  var fields_str = fields.join(', ')
  var sql = 'DROP TABLE IF EXISTS ' + table + '; '
    + 'CREATE TABLE ' + table + ' (' + fields_str + ');'
  var sql2 = 'DROP TABLE IF EXISTS mongo_to_mysql; '
    + 'CREATE TABLE mongo_to_mysql (service varchar(20), timestamp BIGINT);'
  var sql3 = 'INSERT INTO mongo_to_mysql (service, timestamp)'
    + ' VALUES ("' + this.config.service + '", 0);'
  var conn = this.getConnection()
  conn.query(sql, function(err, results) {
    if (err) util.log(err)
    conn.query(sql2, function(err, results) {
      conn.query(sql3, function(err, results) {
        callback()
      })
    })
  })
}

MySQL.prototype.read_timestamp = function(callback) {
  var self = this
  var conn = this.getConnection()
  var q = 'SELECT timestamp FROM mongo_to_mysql'
    + ' WHERE service = "' + this.config.service + '"'
  conn.query(q, function(err, results) {
    if (results && results[0]) {
      self.app.last_timestamp = results[0].timestamp
      callback()
    }
  })
}

MySQL.prototype.update_timestamp = function(ts) {
  var conn = this.getConnection()
  var q = 'UPDATE mongo_to_mysql SET timestamp = ' + ts
    + ' WHERE service = \'' + this.config.service + '\';'
  conn.query(q)
}

function transform(item) {
  // do something
  return item
}

module.exports = MySQL
