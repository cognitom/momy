var util   = require('util')
var Db     = require('mongodb').Db
var Server = require('mongodb').Server

function Mongo(app) {
  util.log('Connect to MongoDB...')

  var config  = app.config.mongodb

  // Access to "local" database which has replication log
  var server = new Server(config.host, config.port, { auto_reconnect: true })
  var db = new Db('local', server, { safe: true })
  db.open(function(err) { if (err) throw err })
  db.on('close', function(err) {
    util.log('Connection to the database was closed!')
  })

  // Acceess to the database which has data
  var server2 = new Server(config.host, config.port, { auto_reconnect: true })
  var db2 = new Db(config.db, server2, { safe: true })
  db2.open(function(err) { if (err) throw err })
  db2.on('close', function(error) {
    util.log("Connection to the database was closed!")
  })

  this.config = app.config
  this.db = db
  this.db2 = db2
}

module.exports = Mongo
