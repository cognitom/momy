/* eslint-env mocha */
'use strict'

const assert = require('assert')
const co = require('co')
const mongo = require('mongodb').MongoClient
const Tailer = require('../../lib/tailer.js')
const wait = require('../wait')
const MysqlConnector = require('../mysql-connector')
const momyfileCamel = require('../momyfile.camel.json')
const momyfileSnake = require('../momyfile.snake.json')

const waitingTime = 1000

describe('Momy: fieldCase', () => {
  it('camel', co.wrap(function* () {
    const config = momyfileCamel
    const colName = 'colCamelCases'
    const mo = yield mongo.connect(config.src)

    // clear existing records
    yield mo.collection(colName).deleteMany({})

    const doc = {
      field1: 'abc',
      field2: {sub1: 'def', sub2: 'ghi'},
      field3: {sub1: {sub2: 'jkl'}},
      field4_sub1_sub2: 'mno',
      field5Sub1Sub2: 'pqr'
    }
    const r0 = yield mo.collection(colName).insertOne(doc)
    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    yield wait(waitingTime * 2) // wait for syncing
    const my = new MysqlConnector(momyfileCamel.dist)
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE id = "${r0.insertedId}"`)
    assert.equal(r1[0].field1, 'abc')
    assert.equal(r1[0].field2Sub1, 'def')
    assert.equal(r1[0].field2Sub2, 'ghi')
    assert.equal(r1[0].field3Sub1Sub2, 'jkl')
    assert.equal(r1[0].field4Sub1Sub2, 'mno')
    assert.equal(r1[0].field5Sub1Sub2, 'pqr')
    tailer.stop()
    yield wait(waitingTime * 2)
  }))

  it('snake', co.wrap(function* () {
    const config = momyfileSnake
    const colName = 'colSnakeCases'
    const mo = yield mongo.connect(config.src)

    // clear existing records
    yield mo.collection(colName).deleteMany({})

    const doc = {
      field1: 'abc',
      field2: {sub1: 'def', sub2: 'ghi'},
      field3: {sub1: {sub2: 'jkl'}},
      field4_sub1_sub2: 'mno',
      field5Sub1Sub2: 'pqr'
    }
    const r0 = yield mo.collection(colName).insertOne(doc)
    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    yield wait(waitingTime * 2) // wait for syncing
    const my = new MysqlConnector(config.dist)
    const r1 = yield my.query(`SELECT * FROM ${colName} WHERE id = "${r0.insertedId}"`)
    assert.equal(r1[0].field1, 'abc')
    assert.equal(r1[0].field2_sub1, 'def')
    assert.equal(r1[0].field2_sub2, 'ghi')
    assert.equal(r1[0].field3_sub1_sub2, 'jkl')
    assert.equal(r1[0].field4_sub1_sub2, 'mno')
    assert.equal(r1[0].field5_sub1_sub2, 'pqr')
    tailer.stop()
    yield wait(waitingTime * 2)
  }))
})
