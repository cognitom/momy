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
const count = 500

/**
 * Wait until the number of rows reaches the expected one, or the time is up
 * @returns {Promise} with the last number of rows
 */
async function waitForRows (my, expected, timeout) {
  const until = Date.now() + timeout
  for (;;) {
    const q = await my.query('SELECT COUNT(*) AS n FROM colTimestamp')
    if (q[0].n >= expected || Date.now() > until) return q[0].n
    await wait(waitingTime)
  }
}

describe('Momy: timestamp', () => {
  it('resumes from the saved timestamp without dropping logs in the same second', async function () {
    this.timeout(60000)
    const config = {
      src: momyfile.src,
      dist: momyfile.dist,
      collections: { colTimestamp: { _id: 'string', field1: 'number' } }
    }
    const client = new MongoClient(config.src)
    const mo = client.db(getDbNameFromUri(config.src))
    await mo.collection('colTimestamp').deleteMany({})

    const tailer1 = new Tailer(config, false)
    tailer1.importAndStart(false)
    await wait(waitingTime * 4) // wait for importing

    // Many logs in the same second, with consecutive increments
    const docs = Array.from({ length: count }, (_, i) => ({ field1: i }))
    const r = await mo.collection('colTimestamp').insertMany(docs)
    const ids = Object.values(r.insertedIds)
    const my = new MysqlConnector(config.dist)
    assert.equal(await waitForRows(my, count, 20000), count)
    await wait(waitingTime) // let the tailer save the last timestamp
    tailer1.stop()

    // Pretend that the tailer stopped in the middle: drop the later half and save the timestamp there
    const logs = await client.db('local').collection('oplog.rs')
      .find({ ns: 'momy.colTimestamp', op: 'i', 'o._id': { $in: ids } })
      .sort({ ts: 1 }).toArray()
    assert.equal(logs.length, count)
    const middle = count / 2
    const laterIds = logs.slice(middle).map(log => `"${log.o._id}"`)
    await my.query(`DELETE FROM colTimestamp WHERE _id IN (${laterIds.join(', ')})`)
    await my.query(`UPDATE mongo_to_mysql SET timestamp = ${logs[middle - 1].ts.toString()} WHERE service = 'momy'`)

    const tailer2 = new Tailer(config, false)
    tailer2.start(false)
    const n = await waitForRows(my, count, 20000)
    tailer2.stop()
    client.close()
    my.close()

    assert.equal(n, count)
    await wait(waitingTime * 2)
  })
})
