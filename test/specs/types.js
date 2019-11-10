/* eslint-env mocha */
'use strict'

const assert = require('assert')
// const moment = require('moment')
const NATIVE_TYPES = require('../../lib/types')

describe('Momy Types', () => {
  it('BIGINT', () => {
    const convert = NATIVE_TYPES.BIGINT.convert
    assert.equal(convert(1), 1)
    assert.equal(convert(1.2), 1)
    assert.equal(convert(123456789), 123456789)
    assert.equal(convert(undefined), 0)
  })

  it('DATE', () => {
    const convert = NATIVE_TYPES.DATE.convert
    assert.equal(convert(undefined), 'NULL')
    assert.equal(convert('abcde'), 'NULL')
    assert.equal(convert('2016-11-10'), '"2016-11-10"')
    assert.equal(convert(1478775921696), '"2016-11-10"')
    assert.equal(convert('1478775921696'), '"2016-11-10"')
  })

  it('DATETIME', () => {
    const convert = NATIVE_TYPES.DATETIME.convert
    assert.equal(convert(undefined), 'NULL')
    assert.equal(convert('abcde'), 'NULL')
    assert.equal(convert('2016-11-10'), '"2016-11-10 00:00:00"')
    assert.equal(convert(1478775921696), '"2016-11-10 20:05:21"')
    assert.equal(convert('1478775921696'), '"2016-11-10 20:05:21"')
  })

  it('DOUBLE', () => {
    const convert = NATIVE_TYPES.DOUBLE.convert
    assert.equal(convert(1), 1)
    assert.equal(convert(1.2), 1.2)
    assert.equal(convert(1.23456789), 1.23456789)
    assert.equal(convert(undefined), 0)
  })

  it('TIME', () => {
    const convert = NATIVE_TYPES.TIME.convert
    assert.equal(convert(undefined), 'NULL')
    assert.equal(convert('abcde'), 'NULL')
    assert.equal(convert('1:23'), '"01:23:00"')
    assert.equal(convert('12:34'), '"12:34:00"')
    assert.equal(convert('12:34:56'), '"12:34:56"')
    assert.equal(convert(1478775921696), '"20:05:21"')
  })

  it('TINYINT', () => {
    const convert = NATIVE_TYPES.TINYINT.convert
    assert.equal(convert(true), 1)
    assert.equal(convert(false), 0)
    assert.equal(convert(1), 1)
    assert.equal(convert(0), 0)
    assert.equal(convert(undefined), 0)
  })

  it('VARCHAR', () => {
    const convert = NATIVE_TYPES.VARCHAR.convert
    const allAscii = Array(95).map((_, i) => String.fromCharCode(32 + i)).join('')
    const a1000 = Array(1000).map(() => 'a').join('')
    const a255 = Array(255).map(() => 'a').join('')
    assert.equal(convert(allAscii), `'${allAscii}'`)
    assert.equal(convert(a1000), `'${a255}'`) // truncate
    assert.equal(convert(undefined), "''")
    assert.equal(convert('\x07'), "''") // skip a control char
  })

  it('TEXT', () => {
    const convert = NATIVE_TYPES.TEXT.convert
    const allAscii = Array(95).map((_, i) => String.fromCharCode(32 + i)).join('')
    const a1000 = Array(1000).map(() => 'a').join('')
    assert.equal(convert(allAscii), `'${allAscii}'`)
    assert.equal(convert(a1000), `'${a1000}'`) // truncate
    assert.equal(convert(undefined), "''")
    assert.equal(convert('\x07'), "''") // skip a control char
  })

  it('JSON', () => {
    const convert = NATIVE_TYPES.JSON.convert
    assert.equal(convert(null), null)
    assert.equal(convert(undefined), null)
    assert.equal(convert({a: null, b: undefined}), '\'{\\"a\\":null}\'')
    assert.equal(convert(true), '\'true\'')
    assert.equal(convert(false), '\'false\'')
    assert.equal(convert('string'), '\'\\"string\\"\'')
    assert.equal(convert([1, 2, '3']), '\'[1,2,\\"3\\"]\'')
    assert.equal(convert({age: 1, name: 'John', ids: [5, 10, 15]}), '\'{\\"age\\":1,\\"name\\":\\"John\\",\\"ids\\":[5,10,15]}\'')
    assert.equal(convert('\x07\r\n\t'), '\'\\"\\\\u0007\\\\r\\\\n\\\\t\\"\'') // escape control chars
  })
})
