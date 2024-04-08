/* eslint-env mocha */

import assert from 'assert'
import fs from 'fs'
import { MongoClient } from 'mongodb'
import wait from '../wait.js'

import Tailer from '../../lib/tailer.js'
import { MysqlConnector } from '../mysql-connector.js'
import { getDbNameFromUri } from '../../lib/util.js'

const momyfile = JSON.parse(fs.readFileSync('./test/momyfile.json'))
const waitingTime = 500

describe('Momy: importing', () => {
  it('imports all docs already exist', async function () {
    const dbname = getDbNameFromUri(momyfile.src)
    const client = new MongoClient(momyfile.src)
    const mo = client.db(dbname)
    const colName = 'colBasicTypes'

    // clear existing records
    await mo.collection(colName).deleteMany({})

    const docs = Array.from(Array(10)).map((_, i) => ({
      field1: true,
      field2: i,
      field3: `Tom-${i}`
    }))
    for (const doc of docs) {
      const r = await mo.collection(colName).insertOne(doc)
      doc._id = r.insertedId
    }
    client.close()

    const tailer = new Tailer(momyfile, false)
    tailer.importAndStart(false)
    await wait(waitingTime * 2) // wait for syncing
    tailer.stop()
    await wait(waitingTime * 2)

    const my = new MysqlConnector(momyfile.dist)
    for (const doc of docs) {
      const r = await my.query(`SELECT * FROM ${colName} WHERE _id = "${doc._id}"`)
      assert.equal(r[0].field2, doc.field2)
    }
    my.close()
  })

  it('imports all docs and restart syncing', async function () {
    const dbname = getDbNameFromUri(momyfile.src)
    const client = new MongoClient(momyfile.src)
    const mo = client.db(dbname)
    const colName = 'colBasicTypes'

    // clear existing records
    await mo.collection(colName).deleteMany({})

    const r0 = await mo.collection(colName).insertOne({
      field1: true,
      field2: 1,
      field3: 'Tom'
    })
    const tailer = new Tailer(momyfile, false)
    tailer.importAndStart(false)
    await wait(waitingTime * 2) // wait for syncing
    tailer.stop()
    await wait(waitingTime) // wait for stopping

    tailer.start(false)
    await wait(waitingTime * 2) // wait for starting
    const r1 = await mo.collection(colName).insertOne({
      field1: true,
      field2: 2,
      field3: 'John'
    })
    client.close()
    await wait(waitingTime * 2) // wait for syncing
    tailer.stop()
    await wait(waitingTime) // wait for stopping

    const my = new MysqlConnector(momyfile.dist)
    const q0 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r0.insertedId}"`)
    const q1 = await my.query(`SELECT * FROM ${colName} WHERE _id = "${r1.insertedId}"`)
    my.close()

    assert.equal(q0[0].field3, 'Tom')
    assert.equal(q1[0].field3, 'John')
  })
})
