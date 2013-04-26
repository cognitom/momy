var mysql = require('mysql'),
    _ = require('underscore'),
    util = require('util');

module.exports = MySQL;

function MySQL(app, callback) {
  util.log('Connect to MySQL...');
  var self = this;
  this.config = app.config;
  if (app.refresh) {
    this.create_table(function () {
      callback();
    });
  } else {
    this.read_timestamp(function () {
      callback();
    });
  }
  this.app = app;
}

MySQL.prototype.getConnection = function () {

  if (this.client && this.client._socket && this.client._socket.readable && this.client._socket.writable) {
    return this.client;
  }
  this.client = mysql.createConnection({
    host: this.config.mysql.host,
    user: this.config.mysql.user,
    password: this.config.mysql.password,
    multipleStatements: true
  });

  this.client.connect(function (err) {
    if (err) {
      util.log("SQL CONNECT ERROR: " + err);
    } else {
      util.log("SQL CONNECT SUCCESSFUL.");
    }
  });

  this.client.on("close", function (err) {
    util.log("SQL CONNECTION CLOSED.");
  });
  this.client.on("error", function (err) {
    util.log("SQL CONNECTION ERROR: " + err);
  });

  this.client.query('USE ' + this.config.mysql.db);
  return this.client;
};

MySQL.prototype.insert = function (item, callback) {

  item = transform(item);

  var self = this;
  var keys = _.keys(this.config.sync_fields);
  var fields = [],
      values = [];
  _.each(item, function (val, key) {
    if (_.contains(keys, key)) {
      fields.push(key);
      if (self.config.sync_fields[key] === 'string') {
        val = val || '';
        val = val.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        values.push('"' + val + '"');
      } else if (self.config.sync_fields[key] === 'int') {
        values.push(val || 0);
      }
    }
  });
  var fields_str = fields.join(', ');
  var values_str = values.join(', ');
  var sql = 'INSERT INTO ' + this.config.mysql.table + ' (' + fields_str + ') VALUES (' + values_str + ');';
  var conn = self.getConnection();
  conn.query(sql, function (err, results) {
    if (err) {
      util.log(sql);
      throw err;
    }
    return callback();
  });
};

MySQL.prototype.update = function (id, item, unset_items, callback) {
  if (item) {
    item = transform(item);
  }
  if (unset_items) {
    unset_items = transform(unset_items);
  }
  var self = this;
  var keys = _.keys(this.config.sync_fields);
  var sets = [];
  _.each(item, function (val, key) {
    if (_.contains(keys, key)) {
      if (self.config.sync_fields[key] === 'string') {
        val = val || '';
        val = val.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        sets.push(key + ' = "' + val + '"');
      } else if (self.config.sync_fields[key] === 'int') {
        sets.push(key + ' = ' + (val || 0));
      }
    }
  });

  _.each(unset_items, function (val, key) {
    if (_.contains(keys, key)) {
      if (self.config.sync_fields[key] === 'string') {
        sets.push(key + ' = ""');
      } else if (self.config.sync_fields[key] === 'int') {
        sets.push(key + ' = 0');
      }
    }
  });

  if (sets.length === 0) return;

  var sets_str = sets.join(', ');
  var sql;
  if (this.config.sync_fields['_id'] === 'int') {
    sql = 'UPDATE ' + this.config.mysql.table + ' SET ' + sets_str + ' WHERE _id = ' + id + ';';
  } else {
    sql = 'UPDATE ' + this.config.mysql.table + ' SET ' + sets_str + ' WHERE _id = "' + id + '";';
  }
  var conn = self.getConnection();
  conn.query(sql, function (err, results) {
    if (err) {
      util.log(sql);
      throw err;
    }
    return callback();
  });
};

MySQL.prototype.remove = function (id, callback) {
  var sql;
  if (this.config.sync_fields['_id'] === 'int') {
    sql = 'DELETE FROM ' + this.config.mysql.table + ' WHERE _id = ' + id + ';';
  } else {
    sql = 'DELETE FROM ' + this.config.mysql.table + ' WHERE _id = \'' + id + '\';';
  }
  var conn = this.getConnection();
  conn.query(sql, function (err, results) {
    if (err) {
      util.log(sql);
      throw err;
    }
    return callback();
  });
};

MySQL.prototype.create_table = function (callback) {
  var fields = [];
  _.each(this.config.sync_fields, function (val, key) {
    if (val === 'string') {
      fields.push(key + ' VARCHAR(1000)');
    } else if (val === 'int') {
      fields.push(key + ' BIGINT');
    }
  });
  var fields_str = fields.join(', ');
  var sql = 'DROP TABLE IF EXISTS ' + this.config.mysql.table + '; ' + 'CREATE TABLE ' + this.config.mysql.table + ' (' + fields_str + ') ENGINE INNODB;';
  var sql2 = 'DROP TABLE IF EXISTS mongo_to_mysql; ' + 'CREATE TABLE mongo_to_mysql (service varchar(20), timestamp BIGINT) ENGINE INNODB;';
  var sql3 = 'INSERT INTO mongo_to_mysql (service, timestamp) VALUES ("' + this.config.service + '", 0);';

  var conn = this.getConnection();
  conn.query(sql, function (err, results) {
    if (err) {
      util.log(err);
    }
    conn.query(sql2, function (err, results) {
      conn.query(sql3, function (err, results) {
        callback();
      });
    });
  });
};

MySQL.prototype.read_timestamp = function (callback) {
  var self = this;
  var conn = this.getConnection();
  conn.query('SELECT timestamp FROM mongo_to_mysql WHERE service = "' + this.config.service + '"', function (err, results) {
    if (results && results[0]) {
      self.app.last_timestamp = results[0].timestamp;
      callback();
    }
  });
};

MySQL.prototype.update_timestamp = function (timestamp) {
  var conn = this.getConnection();
  conn.query('UPDATE mongo_to_mysql SET timestamp = ' + timestamp + ' WHERE service = \'' + this.config.service + '\';', function (err, results) {});
};

// field name, value type, value

function transform(item) {
  if (item.cid) {
    item.cid = parseInt(item.cid.replace('c', ''), 10);
  }
  if (item.vid) {
    item.vid = parseInt(item.vid, 10);
  }
  if (item.order) {
    item._order = item.order;
    delete item.order;
  }
  return item;
}