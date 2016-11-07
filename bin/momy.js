#!/usr/bin/env node

'use strict'

const Tailer = require('../lib/tailer.js')
const fs = require('fs')

const DEFAULT_CONFIG_PATH = 'momyfile.json'
const refresh = process.argv.some(c => c === '--import')
const finder = (p, c, i, a) => c === '--config' && a[i + 1] ? a[i + 1] : p
const file = process.argv.reduce(finder, DEFAULT_CONFIG_PATH)
const config = JSON.parse(fs.readFileSync(process.cwd() + '/' + file))
const tailer = new Tailer(config)

if (refresh) tailer.importAndStart()
else tailer.start()
