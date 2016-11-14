/* eslint-env mocha */
'use strict'

const assert = require('assert')
const co = require('co')
const mongo = require('mongodb').MongoClient
const Tailer = require('../../lib/tailer.js')
const wait = require('../wait')
const MysqlConnector = require('../mysql-connector')
const momyfileExclusions = require('../momyfile.exclusions.json')
const momyfileInclusions = require('../momyfile.inclusions.json')

const waitingTime = 500

describe('Momy: filter', () => {
  it('excludes custom chars', co.wrap(function* () {
    const config = momyfileExclusions
    const colName1 = 'colWithExclusions'
    const mo = yield mongo.connect(config.src)

    // clear existing records
    yield mo.collection(colName1).deleteMany({})

    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: 'T\uFFFDo\uFFFDm' // string
    }
    const r1 = yield mo.collection(colName1).insertOne(doc)
    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    yield wait(waitingTime * 4) // wait for syncing
    const my = new MysqlConnector(config.dist)
    const q1 = yield my.query(`SELECT * FROM ${colName1} WHERE _id = "${r1.insertedId}"`)

    assert.equal(q1[0].field3, 'Tom')
    tailer.stop()
    yield wait(waitingTime * 2)
  }))

  it('includes custom chars', co.wrap(function* () {
    const config = momyfileInclusions
    const colName1 = 'colWithInclusions'
    const mo = yield mongo.connect(config.src)

    // clear existing records
    yield mo.collection(colName1).deleteMany({})

    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: '河村Tom奨' // string
    }
    const r1 = yield mo.collection(colName1).insertOne(doc)
    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    yield wait(waitingTime * 4) // wait for syncing
    const my = new MysqlConnector(config.dist)
    const q1 = yield my.query(`SELECT * FROM ${colName1} WHERE _id = "${r1.insertedId}"`)

    assert.equal(q1[0].field3, 'Tom')
    tailer.stop()
    yield wait(waitingTime * 2)
  }))
})
