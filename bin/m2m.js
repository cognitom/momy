#!/usr/bin/env node

var Tailer = require('../lib/tailer.js')
var fs     = require('fs')

var DEFAULT_CONFIG_PATH = 'm2mfile.json'

var refresh = process.argv.some(function(c) { return c == '--import' })
var file = process.argv.reduce(function(p, c, i, a) {
  return c == '--config' && a[i + 1] ? a[i + 1] : p
}, DEFAULT_CONFIG_PATH)
var config = JSON.parse(fs.readFileSync(process.cwd() + '/' + file))

var tailer = new Tailer(config)
if (refresh)
  tailer.importAndStart()
else
  tailer.start()
