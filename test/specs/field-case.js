/* eslint-env mocha */

import assert from 'assert'
import fs from 'fs'
import { MongoClient } from 'mongodb'
import wait from '../wait.js'

import Tailer from '../../lib/tailer.js'
import { MysqlConnector } from '../mysql-connector.js'
import { getDbNameFromUri } from '../../lib/util.js'

const momyfileCamel = JSON.parse(fs.readFileSync('./test/momyfile.camel.json'))
const momyfileSnake = JSON.parse(fs.readFileSync('./test/momyfile.snake.json'))
const waitingTime = 1000

describe('Momy: fieldCase', () => {
  it('camel', async function () {
    const config = momyfileCamel
    const dbname = getDbNameFromUri(config.src)
    const client = new MongoClient(config.src)
    const mo = client.db(dbname)
    const colName = 'colCamelCases'

    // clear existing records
    await mo.collection(colName).deleteMany({})

    const doc = {
      field1: 'abc',
      field2: { sub1: 'def', sub2: 'ghi' },
      field3: { sub1: { sub2: 'jkl' } },
      field4_sub1_sub2: 'mno',
      field5Sub1Sub2: 'pqr'
    }
    const r0 = await mo.collection(colName).insertOne(doc)
    client.close()

    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    await wait(waitingTime * 2) // wait for syncing
    tailer.stop()

    const my = new MysqlConnector(momyfileCamel.dist)
    const r1 = await my.query(`SELECT * FROM ${colName} WHERE id = "${r0.insertedId}"`)
    my.close()

    assert.equal(r1[0].field1, 'abc')
    assert.equal(r1[0].field2Sub1, 'def')
    assert.equal(r1[0].field2Sub2, 'ghi')
    assert.equal(r1[0].field3Sub1Sub2, 'jkl')
    assert.equal(r1[0].field4Sub1Sub2, 'mno')
    assert.equal(r1[0].field5Sub1Sub2, 'pqr')
    await wait(waitingTime * 2)
  })

  it('snake', async function () {
    const config = momyfileSnake
    const dbname = getDbNameFromUri(config.src)
    const client = new MongoClient(config.src)
    const mo = client.db(dbname)
    const colName = 'colSnakeCases'

    // clear existing records
    await mo.collection(colName).deleteMany({})

    const doc = {
      field1: 'abc',
      field2: { sub1: 'def', sub2: 'ghi' },
      field3: { sub1: { sub2: 'jkl' } },
      field4_sub1_sub2: 'mno',
      field5Sub1Sub2: 'pqr'
    }
    const r0 = await mo.collection(colName).insertOne(doc)
    client.close()

    const tailer = new Tailer(config, false)
    tailer.importAndStart(false)
    await wait(waitingTime * 2) // wait for syncing
    tailer.stop()

    const my = new MysqlConnector(config.dist)
    const r1 = await my.query(`SELECT * FROM ${colName} WHERE id = "${r0.insertedId}"`)
    my.close()

    assert.equal(r1[0].field1, 'abc')
    assert.equal(r1[0].field2_sub1, 'def')
    assert.equal(r1[0].field2_sub2, 'ghi')
    assert.equal(r1[0].field3_sub1_sub2, 'jkl')
    assert.equal(r1[0].field4_sub1_sub2, 'mno')
    assert.equal(r1[0].field5_sub1_sub2, 'pqr')
    await wait(waitingTime * 2)
  })
})
