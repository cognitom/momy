#!/usr/bin/env node

var Mongo  = require('../lib/mongo.js')
var MySQL  = require('../lib/mysql.js')
var Tailer = require('../lib/tailer.js')
var fs     = require('fs')

var DEFAULT_CONFIG_PATH = 'm2mfile.json'

var refresh = process.argv.some(function(c) { return c == '--import' })
var file = process.argv.reduce(function(p, c, i, a) {
  return c == '--config' && a[i + 1] ? a[i + 1] : p
}, DEFAULT_CONFIG_PATH)

var app = {
  last_timestamp: 0,
  refresh: refresh, // import all data
  config: JSON.parse(fs.readFileSync(process.cwd() + '/' + file))
}

app.mongo = new Mongo(app)
app.mysql = new MySQL(app, function() {
  app.tailer = new Tailer(app)
  if (app.refresh === true)
    app.tailer.import(function() { app.tailer.start() })
  else
    app.tailer.start()
})
