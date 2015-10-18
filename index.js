var Mongo  = require('./lib/mongo.js')
var MySQL  = require('./lib/mysql.js')
var Tailer = require('./lib/tailer.js')
var fs     = require('fs')

function factory(config, refresh) {
  var app = {
    last_timestamp: 0,
    refresh: refresh == true,
    config: config
  }
  app.mongo = new Mongo(app)
  app.mysql = new MySQL(app, function () {
    app.tailer = new Tailer(app)
    app.tailer.start()
  })
}

module.exports = factory
