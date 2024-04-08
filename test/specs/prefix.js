/* eslint-env mocha */

import assert from 'assert'
import fs from 'fs'
import { MongoClient } from 'mongodb'
import wait from '../wait.js'

import Tailer from '../../lib/tailer.js'
import { MysqlConnector } from '../mysql-connector.js'
import { getDbNameFromUri } from '../../lib/util.js'

const momyfilePrefix = JSON.parse(fs.readFileSync('./test/momyfile.prefix.json'))
const waitingTime = 500

describe('Momy: prefix', () => {
  it('adds prefix to collections', async function () {
    const config = momyfilePrefix
    const dbname = getDbNameFromUri(config.src)
    const client = new MongoClient(config.src)
    const mo = client.db(dbname)
    const prefix = 'p_'
    const colName1 = 'colWithPrefix1'
    const colName2 = 'colWithPrefix2'

    // clear existing records
    await mo.collection(colName1).deleteMany({})
    await mo.collection(colName2).deleteMany({})

    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: 'Tom' // string
    }
    const r1 = await mo.collection(colName1).insertOne(doc)
    const r2 = await mo.collection(colName2).insertOne(doc)
    client.close()

    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    await wait(waitingTime * 4) // wait for syncing
    tailer.stop()

    const my = new MysqlConnector(config.dist)
    const q1 = await my.query(`SELECT * FROM ${prefix}${colName1} WHERE _id = "${r1.insertedId}"`)
    const q2 = await my.query(`SELECT * FROM ${prefix}${colName2} WHERE _id = "${r2.insertedId}"`)
    my.close()

    assert.equal(q1[0].field3, 'Tom')
    assert.equal(q2[0].field3, 'Tom')
    await wait(waitingTime * 2)
  })
})
