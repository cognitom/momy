var util = require('util'),
    Db = require('mongodb').Db,
    Server = require('mongodb').Server,
    async = require('async'),
    _ = require('underscore');

module.exports = Mongo;

function Mongo(app) {
  util.log('Connect to MongoDB...');
  this.config = app.config;
  var server = new Server(this.config.mongodb.host, this.config.mongodb.port, {
    auto_reconnect: true
  });
  this.db = new Db('local', server, {
    safe: true
  });
  this.db.open(function (err, db) {
    if (err) throw err;
  });
  this.db.on("close", function (error) {
    util.log("Connection to the database was closed!");
  });

  var server2 = new Server(this.config.mongodb.host, this.config.mongodb.port, {
    auto_reconnect: true
  });

  this.db2 = new Db(this.config.mongodb.db, server2, {
    safe: true
  });
  this.db2.open(function (err, db) {
    if (err) throw err;
  });
  this.db2.on("close", function (error) {
    util.log("Connection to the database was closed!");
  });
}