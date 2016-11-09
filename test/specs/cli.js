/* eslint-env mocha */
'use strict'

const assert = require('assert')
const co = require('co')
const moment = require('moment')
const mongo = require('mongodb').MongoClient
const Tailer = require('../../lib/tailer.js')
const wait = require('../wait')
const MysqlConnector = require('../mysql-connector')
const config = require('../momyfile.json')

const waitingTime = 500 // waiting time for syncing from Mongo to MySQL

describe('Momy CLI', () => {
  let mo
  let my
  let tailer

  before(co.wrap(function* () {
    mo = yield mongo.connect(config.src)
    yield mo.collection('account').deleteMany({}) // clear all
    my = new MysqlConnector(config.dist)
    tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    yield wait(1000) // wait for syncing
  }))

  it('syncs a single doc with basic types', co.wrap(function* () {
    const colName = 'colBasicTypes'
    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: 'Tom' // string
    }
    const r0 = yield mo.collection(colName).insertOne(doc)
    yield wait(waitingTime) // wait for syncing
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE id = "${r0.insertedId}"`)

    assert.equal(r1[0].field1, 1)
    assert.equal(r1[0].field2, doc.field2)
    assert.equal(r1[0].field3, doc.field3)
  }))

  it('syncs a single doc with number types', co.wrap(function* () {
    const colName = 'colNumberTypes'
    const doc = {
      field1: 1234567, // BIGINT
      field2: 123.4567, // DOUBLE
      field3: true // TINYINT
    }
    const r0 = yield mo.collection(colName).insertOne(doc)
    yield wait(waitingTime) // wait for syncing
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE id = "${r0.insertedId}"`)

    assert.equal(r1[0].field1, 1234567)
    assert.equal(r1[0].field2, 123.4567)
    assert.equal(r1[0].field3, 1)
  }))

  it('syncs a single doc with date types', co.wrap(function* () {
    const colName = 'colDateTypes'
    const now = Date.now()
    const doc = {
      field1: now, // DATE
      field2: now, // DATETIME
      field3: now // TIME
    }
    const r0 = yield mo.collection(colName).insertOne(doc)
    yield wait(waitingTime) // wait for syncing
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE id = "${r0.insertedId}"`)

    assert.equal(
      moment(r1[0].field1).format('YYYY-MM-DD'),
      moment(now).format('YYYY-MM-DD'))
    assert.equal(
      moment(r1[0].field2).format('YYYY-MM-DD HH:mm:ss'),
      moment(now).format('YYYY-MM-DD HH:mm:ss'))
    assert.equal(r1[0].field3, moment(now).format('HH:mm:ss'))
  }))

  it('syncs a single doc with string types', co.wrap(function* () {
    const colName = 'colStringTypes'
    const allAscii = Array(95).map((_, i) => String.fromCharCode(32 + i)).join('')
    const string285 = allAscii + allAscii + allAscii
    const doc = {
      field1: string285, // VARCHAR
      field2: string285 // TEXT
    }
    const r0 = yield mo.collection(colName).insertOne(doc)
    yield wait(waitingTime) // wait for syncing
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE id = "${r0.insertedId}"`)

    assert.equal(r1[0].field1, string285.substring(0, 255))
    assert.equal(r1[0].field2, string285)
  }))

  after(() => {
    tailer.stop()
  })
})
