/* eslint-env mocha */

import assert from 'assert'
import fs from 'fs'
import moment from 'moment'
import { MongoClient } from 'mongodb'
import wait from '../wait.js'

import Tailer from '../../lib/tailer.js'
import { MysqlConnector } from '../mysql-connector.js'
import { getDbNameFromUri } from '../../lib/util.js'

const momyfile = JSON.parse(fs.readFileSync('./test/momyfile.json'))
const waitingTime = 500

describe('Momy: core', () => {
  let mo
  let my
  let client
  let tailer

  before(async function () {
    const dbname = getDbNameFromUri(momyfile.src)
    client = new MongoClient(momyfile.src)
    mo = client.db(dbname)
    // clear existing records
    await mo.collection('colBasicTypes').deleteMany({})
    await mo.collection('colNumberTypes').deleteMany({})
    await mo.collection('colDateTypes').deleteMany({})
    await mo.collection('colStringTypes').deleteMany({})

    my = new MysqlConnector(momyfile.dist)
    tailer = new Tailer(momyfile, false)
    tailer.importAndStart(false)
    await wait(1000) // wait for syncing
  })

  it('syncs a single doc with basic types', async function () {
    const colName = 'colBasicTypes'
    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: 'Tom' // string
    }
    const r0 = await mo.collection(colName).insertOne(doc)
    await wait(waitingTime) // wait for syncing
    const r1 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)

    assert.equal(r1[0].field1, 1)
    assert.equal(r1[0].field2, doc.field2)
    assert.equal(r1[0].field3, doc.field3)
  })

  it('syncs a single doc with number types', async function () {
    const colName = 'colNumberTypes'
    const doc = {
      field1: 1234567, // BIGINT
      field2: 123.4567, // DOUBLE
      field3: true // TINYINT
    }
    const r0 = await mo.collection(colName).insertOne(doc)
    await wait(waitingTime) // wait for syncing
    const r1 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)

    assert.equal(r1[0].field1, 1234567)
    assert.equal(r1[0].field2, 123.4567)
    assert.equal(r1[0].field3, 1)
  })

  it('syncs a single doc with date types', async function () {
    const colName = 'colDateTypes'
    const now = Date.now()
    const doc = {
      field1: now, // DATE
      field2: now, // DATETIME
      field3: now // TIME
    }
    const r0 = await mo.collection(colName).insertOne(doc)
    await wait(waitingTime) // wait for syncing
    const r1 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)

    assert.equal(
      moment(r1[0].field1).format('YYYY-MM-DD'),
      moment(now).format('YYYY-MM-DD'))
    assert.equal(
      moment(r1[0].field2).format('YYYY-MM-DD HH:mm:ss'),
      moment(now).format('YYYY-MM-DD HH:mm:ss'))
    assert.equal(r1[0].field3, moment(now).format('HH:mm:ss'))
  })

  it('syncs a single doc with date types (object edition) #15', async function () {
    const colName = 'colDateTypes'
    const now = new Date()
    const doc = {
      field1: now, // DATE
      field2: now, // DATETIME
      field3: now // TIME
    }
    const r0 = await mo.collection(colName).insertOne(doc)
    await wait(waitingTime) // wait for syncing
    const r1 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)

    assert.equal(
      moment(r1[0].field1).format('YYYY-MM-DD'),
      moment(now).format('YYYY-MM-DD'))
    assert.equal(
      moment(r1[0].field2).format('YYYY-MM-DD HH:mm:ss'),
      moment(now).format('YYYY-MM-DD HH:mm:ss'))
    assert.equal(r1[0].field3, moment(now).format('HH:mm:ss'))
  })

  it('syncs a single doc with string types', async function () {
    const colName = 'colStringTypes'
    const allAscii = Array.from(Array(95)).map((_, i) => String.fromCharCode(32 + i)).join('')
    const string285 = allAscii + allAscii + allAscii
    const doc = {
      field1: string285, // VARCHAR
      field2: string285 // TEXT
    }
    const r0 = await mo.collection(colName).insertOne(doc)
    await wait(waitingTime) // wait for syncing
    const r1 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)

    assert.equal(r1[0].field1, string285.substring(0, 255))
    assert.equal(r1[0].field2, string285)
  })

  it('syncs a doc updated', async function () {
    const colName = 'colBasicTypes'
    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: 'Tom' // string
    }
    const r0 = await mo.collection(colName).insertOne(doc)
    await wait(waitingTime) // wait for syncing
    const r1 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r1[0].field3, 'Tom')

    await mo.collection(colName).updateOne({ _id: r0.insertedId }, { $set: { field3: 'John' } })
    await wait(waitingTime) // wait for syncing
    const r2 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r2[0].field3, 'John')

    await mo.collection(colName).updateOne({ _id: r0.insertedId }, { $unset: { field3: true } })
    await wait(waitingTime) // wait for syncing
    const r3 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r3[0].field3, '')

    await mo.collection(colName).replaceOne({ _id: r0.insertedId }, doc)
    await wait(waitingTime) // wait for syncing
    const r4 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r4[0].field3, 'Tom')
  })

  it('syncs a doc without a specific field which is added later', async function () {
    const colName = 'colBasicTypes'
    const doc = {
      field1: true, // boolean
      field2: 123 // number
    }
    const r0 = await mo.collection(colName).insertOne(doc)
    await wait(waitingTime) // wait for syncing
    const r1 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    console.log(r1[0].field3)

    await mo.collection(colName).updateOne({ _id: r0.insertedId }, { $set: { field3: 'John' } })
    await wait(waitingTime) // wait for syncing
    const r2 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r2[0].field3, 'John')
  })

  it('inserts and remove a doc', async function () {
    const colName = 'colBasicTypes'
    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: 'Tom' // string
    }
    const r0 = await mo.collection(colName).insertOne(doc)
    await wait(waitingTime) // wait for syncing
    const r1 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r1[0].field3, 'Tom')

    await mo.collection(colName).deleteOne({ _id: r0.insertedId })
    await wait(waitingTime) // wait for syncing
    const r2 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    assert.equal(r2.length, 0)
  })

  it('inserts multiple docs', async function () {
    const colName = 'colBasicTypes'
    const docs = Array.from(Array(10)).map((_, i) => ({
      field1: true,
      field2: i,
      field3: `Tom-${i}`
    }))
    for (const doc of docs) {
      const r = await mo.collection(colName).insertOne(doc)
      doc._id = r.insertedId
    }
    await wait(waitingTime) // wait for syncing
    for (const doc of docs) {
      const r = await my.query(`SELECT * FROM ${colName} WHERE _id = "${doc._id}"`)
      assert.equal(r[0].field2, doc.field2)
    }
  })

  it('inserts 100 docs', async function () {
    const colName = 'colBasicTypes'
    const docs = Array.from(Array(100)).map((_, i) => ({
      field1: true,
      field2: i,
      field3: `Tom-${i}`
    }))
    for (const doc of docs) {
      const r = await mo.collection(colName).insertOne(doc)
      doc._id = r.insertedId
    }
    await wait(waitingTime * 10) // wait for syncing
    for (const doc of docs) {
      const r = await my.query(`SELECT * FROM ${colName} WHERE _id = "${doc._id}"`)
      assert.equal(r[0].field2, doc.field2)
    }
  })

  after(async function () {
    tailer.stop()
    my.close()
    client.close()
    await wait(waitingTime * 2)
  })
})
