/* eslint-env mocha */
'use strict'

const assert = require('assert')
const co = require('co')
const mongo = require('mongodb').MongoClient
const Tailer = require('../../lib/tailer.js')
const wait = require('../wait')
const MysqlConnector = require('../mysql-connector')
const momyfilePrefix = require('../momyfile.prefix.json')

const waitingTime = 500

describe('Momy: prefix', () => {
  it('adds prefix to collections', co.wrap(function* () {
    const config = momyfilePrefix
    const prefix = 'p_'
    const colName1 = 'colWithPrefix1'
    const colName2 = 'colWithPrefix2'
    const mo = yield mongo.connect(config.src)

    // clear existing records
    yield mo.collection(colName1).deleteMany({})
    yield mo.collection(colName2).deleteMany({})

    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: 'Tom' // string
    }
    const r1 = yield mo.collection(colName1).insertOne(doc)
    const r2 = yield mo.collection(colName2).insertOne(doc)
    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    yield wait(waitingTime * 4) // wait for syncing
    const my = new MysqlConnector(config.dist)
    const q1 = yield my.query(`SELECT * FROM ${prefix}${colName1} WHERE _id = "${r1.insertedId}"`)
    const q2 = yield my.query(`SELECT * FROM ${prefix}${colName2} WHERE _id = "${r2.insertedId}"`)

    assert.equal(q1[0].field3, 'Tom')
    assert.equal(q2[0].field3, 'Tom')
    tailer.stop()
    yield wait(waitingTime * 2)
  }))
})
