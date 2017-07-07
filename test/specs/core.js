/* eslint-env mocha */
'use strict'

const assert = require('assert')
const co = require('co')
const moment = require('moment')
const mongo = require('mongodb').MongoClient
const Tailer = require('../../lib/tailer.js')
const wait = require('../wait')
const MysqlConnector = require('../mysql-connector')
const momyfile = require('../momyfile.json')

const waitingTime = 500

describe('Momy: core', () => {
  let mo
  let my
  let tailer

  before(co.wrap(function* () {
    mo = yield mongo.connect(momyfile.src)
    // clear existing records
    yield mo.collection('colBasicTypes').deleteMany({})
    yield mo.collection('colNumberTypes').deleteMany({})
    yield mo.collection('colDateTypes').deleteMany({})
    yield mo.collection('colStringTypes').deleteMany({})
    my = new MysqlConnector(momyfile.dist)
    tailer = new Tailer(momyfile, false)
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
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)

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
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)

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
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)

    assert.equal(
      moment(r1[0].field1).format('YYYY-MM-DD'),
      moment(now).format('YYYY-MM-DD'))
    assert.equal(
      moment(r1[0].field2).format('YYYY-MM-DD HH:mm:ss'),
      moment(now).format('YYYY-MM-DD HH:mm:ss'))
    assert.equal(r1[0].field3, moment(now).format('HH:mm:ss'))
  }))

  it('syncs a single doc with date types (object edition) #15', co.wrap(function* () {
    const colName = 'colDateTypes'
    const now = new Date()
    const doc = {
      field1: now, // DATE
      field2: now, // DATETIME
      field3: now // TIME
    }
    const r0 = yield mo.collection(colName).insertOne(doc)
    yield wait(waitingTime) // wait for syncing
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)

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
    const allAscii = Array.from(Array(95)).map((_, i) => String.fromCharCode(32 + i)).join('')
    const string285 = allAscii + allAscii + allAscii
    const doc = {
      field1: string285, // VARCHAR
      field2: string285 // TEXT
    }
    const r0 = yield mo.collection(colName).insertOne(doc)
    yield wait(waitingTime) // wait for syncing
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)

    assert.equal(r1[0].field1, string285.substring(0, 255))
    assert.equal(r1[0].field2, string285)
  }))

  it('syncs a doc updated', co.wrap(function* () {
    const colName = 'colBasicTypes'
    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: 'Tom' // string
    }
    const r0 = yield mo.collection(colName).insertOne(doc)
    yield wait(waitingTime) // wait for syncing
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r1[0].field3, 'Tom')

    yield mo.collection(colName).update({_id: r0.insertedId}, {$set: {field3: 'John'}})
    yield wait(waitingTime) // wait for syncing
    const r2 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r2[0].field3, 'John')

    yield mo.collection(colName).update({_id: r0.insertedId}, {$unset: {field3: true}})
    yield wait(waitingTime) // wait for syncing
    const r3 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r3[0].field3, '')

    yield mo.collection(colName).update({_id: r0.insertedId}, doc)
    yield wait(waitingTime) // wait for syncing
    const r4 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r4[0].field3, 'Tom')
  }))

  it('inserts and remove a doc', co.wrap(function* () {
    const colName = 'colBasicTypes'
    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: 'Tom' // string
    }
    const r0 = yield mo.collection(colName).insertOne(doc)
    yield wait(waitingTime) // wait for syncing
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r1[0].field3, 'Tom')

    yield mo.collection(colName).removeOne({_id: r0.insertedId})
    yield wait(waitingTime) // wait for syncing
    const r2 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r2.length, 0)
  }))

  it('inserts multiple docs', co.wrap(function* () {
    const colName = 'colBasicTypes'
    const docs = Array.from(Array(10)).map((_, i) => ({
      field1: true,
      field2: i,
      field3: `Tom-${i}`
    }))
    for (const doc of docs) {
      const r = yield mo.collection(colName).insertOne(doc)
      doc._id = r.insertedId
    }
    yield wait(waitingTime) // wait for syncing
    for (const doc of docs) {
      const r = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${doc._id}"`)
      assert.equal(r[0].field2, doc.field2)
    }
  }))

  it('inserts 1000 docs', co.wrap(function* () {
    const colName = 'colBasicTypes'
    const docs = Array.from(Array(1000)).map((_, i) => ({
      field1: true,
      field2: i,
      field3: `Tom-${i}`
    }))
    for (const doc of docs) {
      const r = yield mo.collection(colName).insertOne(doc)
      doc._id = r.insertedId
    }
    yield wait(waitingTime * 4) // wait for syncing
    for (const doc of docs) {
      const r = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${doc._id}"`)
      assert.equal(r[0].field2, doc.field2)
    }
  }))

  after(co.wrap(function* () {
    tailer.stop()
    yield wait(waitingTime * 2)
  }))
})
