/* eslint-env mocha */
'use strict'

const assert = require('assert')
const co = require('co')
const mongo = require('mongodb').MongoClient
const Tailer = require('../../lib/tailer.js')
const wait = require('../wait')
const MysqlConnector = require('../mysql-connector')
const momyfile = require('../momyfile.json')

const waitingTime = 500

describe('Momy: importing', () => {
  it('imports all docs already exist', co.wrap(function* () {
    const mo = yield mongo.connect(momyfile.src)
    const colName = 'colBasicTypes'

    // clear existing records
    yield mo.collection(colName).deleteMany({})

    const docs = Array.from(Array(10)).map((_, i) => ({
      field1: true,
      field2: i,
      field3: `Tom-${i}`
    }))
    for (const doc of docs) {
      const r = yield mo.collection(colName).insertOne(doc)
      doc._id = r.insertedId
    }

    const tailer = new Tailer(momyfile, false)
    tailer.importAndStart(false)
    yield wait(waitingTime * 2) // wait for syncing
    tailer.stop()
    yield wait(waitingTime * 2)

    const my = new MysqlConnector(momyfile.dist)
    for (const doc of docs) {
      const r = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${doc._id}"`)
      assert.equal(r[0].field2, doc.field2)
    }
  }))

  it('imports all docs and restart syncing', co.wrap(function* () {
    const mo = yield mongo.connect(momyfile.src)
    const colName = 'colBasicTypes'

    // clear existing records
    yield mo.collection(colName).deleteMany({})

    const r0 = yield mo.collection(colName).insertOne({
      field1: true,
      field2: 1,
      field3: 'Tom'
    })
    const tailer = new Tailer(momyfile, false)
    tailer.importAndStart(false)
    yield wait(1000) // wait for syncing
    tailer.stop()
    yield wait(waitingTime) // wait for stopping

    tailer.start(false)
    yield wait(waitingTime) // wait for starting
    const r1 = yield mo.collection(colName).insertOne({
      field1: true,
      field2: 2,
      field3: 'John'
    })
    yield wait(waitingTime) // wait for syncing
    tailer.stop()
    yield wait(waitingTime * 2)

    const my = new MysqlConnector(momyfile.dist)
    const q0 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    const q1 = yield my.query(`SELECT * FROM ${colName} WHERE _id = "${r1.insertedId}"`)
    assert.equal(q0[0].field3, 'Tom')
    assert.equal(q1[0].field3, 'John')
  }))
})
