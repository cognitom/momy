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

describe('Momy: skip', () => {
  it('skips a log MySQL rejects and keeps syncing', async function () {
    const fields = { _id: 'string', field1: 'string' }
    const config = {
      src: momyfile.src,
      dist: momyfile.dist,
      collections: { colSkip1: fields, colSkip2: fields }
    }
    const client = new MongoClient(config.src)
    const mo = client.db(getDbNameFromUri(config.src))
    await mo.collection('colSkip1').deleteMany({})
    await mo.collection('colSkip2').deleteMany({})

    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    await wait(waitingTime * 4) // wait for importing

    // Inserting into colSkip1 fails in MySQL, since its table is gone
    const my = new MysqlConnector(config.dist)
    await my.query('DROP TABLE colSkip1')
    await mo.collection('colSkip1').insertOne({ field1: 'lost' })
    const r = await mo.collection('colSkip2').insertOne({ field1: 'Tom' })
    await wait(waitingTime * 2) // wait for syncing
    tailer.stop()
    client.close()

    const q = await my.query(`SELECT * FROM colSkip2 WHERE _id = "${r.insertedId}"`)
    my.close()

    assert.equal(q[0].field1, 'Tom')
    await wait(waitingTime * 2)
  })

  it('rolls back a log MySQL rejects and saves the timestamp past it', async function () {
    const config = {
      src: momyfile.src,
      dist: momyfile.dist,
      collections: { colSkip3: { _id: 'string', field1: 'string', field2: 'string' } }
    }
    const client = new MongoClient(config.src)
    const mo = client.db(getDbNameFromUri(config.src))
    await mo.collection('colSkip3').deleteMany({})

    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    await wait(waitingTime * 4) // wait for importing

    const my = new MysqlConnector(config.dist)
    const r0 = await mo.collection('colSkip3').insertOne({ field1: 'Tom', field2: 'Cat' })
    await wait(waitingTime) // wait for syncing

    // Updating colSkip3 fails in MySQL, since field2 is gone
    await my.query('ALTER TABLE colSkip3 DROP COLUMN field2')
    await mo.collection('colSkip3').updateOne({ _id: r0.insertedId }, { $set: { field1: 'Jerry', field2: 'Mouse' } })
    await wait(waitingTime * 2) // wait for syncing
    const log = await client.db('local').collection('oplog.rs')
      .findOne({ ns: 'momy.colSkip3', op: 'u', 'o2._id': r0.insertedId })

    // Read from another connection: only committed writes are visible
    const q0 = await my.query(`SELECT * FROM colSkip3 WHERE _id = "${r0.insertedId}"`)
    const q1 = await my.query("SELECT CAST(timestamp AS CHAR) AS ts FROM mongo_to_mysql WHERE service = 'momy'")

    // Keeps syncing after the rejected log
    await mo.collection('colSkip3').updateOne({ _id: r0.insertedId }, { $set: { field1: 'Spike' } })
    await wait(waitingTime * 2) // wait for syncing
    tailer.stop()
    client.close()

    const q2 = await my.query(`SELECT * FROM colSkip3 WHERE _id = "${r0.insertedId}"`)
    my.close()

    assert.equal(q0[0].field1, 'Tom')
    assert.equal(q1[0].ts, log.ts.toString())
    assert.equal(q2[0].field1, 'Spike')
    await wait(waitingTime * 2)
  })
})
