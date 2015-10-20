#!/usr/bin/env node

'use strict'

const
  DEFAULT_CONFIG_PATH = 'momyfile.json',

  Tailer = require('../lib/tailer.js'),
  fs     = require('fs'),

  refresh = process.argv.some(c => c == '--import'),
  finder  = (p, c, i, a) => c == '--config' && a[i + 1] ? a[i + 1] : p,
  file    = process.argv.reduce(finder, DEFAULT_CONFIG_PATH),
  config  = JSON.parse(fs.readFileSync(process.cwd() + '/' + file)),
  tailer  = new Tailer(config)

if (refresh)
  tailer.importAndStart()
else
  tailer.start()
