/* eslint-env mocha */

import assert from 'assert'
import fs from 'fs'
import { MongoClient } from 'mongodb'
import wait from '../wait.js'

import Tailer from '../../lib/tailer.js'
import { MysqlConnector } from '../mysql-connector.js'
import { getDbNameFromUri } from '../../lib/util.js'

const momyfileExclusions = JSON.parse(fs.readFileSync('./test/momyfile.exclusions.json'))
const momyfileInclusions = JSON.parse(fs.readFileSync('./test/momyfile.inclusions.json'))

const waitingTime = 500

describe('Momy: filter', () => {
  it('excludes custom chars', async function () {
    const config = momyfileExclusions
    const dbname = getDbNameFromUri(config.src)
    const client = new MongoClient(config.src)
    const mo = client.db(dbname)
    const colName1 = 'colWithExclusions'

    // clear existing records
    await mo.collection(colName1).deleteMany({})

    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: 'T\uFFFDo\uFFFDm' // string
    }
    const r1 = await mo.collection(colName1).insertOne(doc)
    client.close()

    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    await wait(waitingTime * 4) // wait for syncing
    tailer.stop()

    const my = new MysqlConnector(config.dist)
    const q1 = await my.query(`SELECT * FROM ${colName1} WHERE _id = "${r1.insertedId}"`)
    my.close()

    assert.equal(q1[0].field3, 'Tom')
    await wait(waitingTime * 2)
  })

  it('includes custom chars', async function () {
    const config = momyfileInclusions
    const dbname = getDbNameFromUri(config.src)
    const client = new MongoClient(config.src)
    const mo = client.db(dbname)
    const colName1 = 'colWithInclusions'

    // clear existing records
    await mo.collection(colName1).deleteMany({})

    const doc = {
      field1: true, // boolean
      field2: 123, // number
      field3: '河村Tom奨' // string
    }
    const r1 = await mo.collection(colName1).insertOne(doc)
    client.close()

    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    await wait(waitingTime * 4) // wait for syncing
    tailer.stop()

    const my = new MysqlConnector(config.dist)
    const q1 = await my.query(`SELECT * FROM ${colName1} WHERE _id = "${r1.insertedId}"`)
    my.close()

    assert.equal(q1[0].field3, 'Tom')
    await wait(waitingTime * 2)
  })
})
