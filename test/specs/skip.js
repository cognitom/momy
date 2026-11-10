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
})
