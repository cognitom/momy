'use strict'

const moment = require('moment')
const sqlstring = require('sqlstring')

const controlRegex = /[\x00-\x1F\x7F]/g // eslint-disable-line no-control-regex
const NATIVE_TYPES = {
  'BIGINT': {
    type: 'BIGINT',
    convert: val => parseInt(val || 0)
  },
  'DATE': {
    type: 'DATE',
    convert: val => {
      if (typeof val === 'string') val = getValueOfDate(val)
      if (typeof val === 'number' || val instanceof Date) {
        val = moment(val).format('YYYY-MM-DD')
        return `"${val}"`
      }
      return 'NULL'
    }
  },
  'DATETIME': {
    type: 'DATETIME',
    convert: val => {
      if (typeof val === 'string') val = getValueOfDate(val)
      if (typeof val === 'number' || val instanceof Date) {
        val = moment(val).format('YYYY-MM-DD HH:mm:ss')
        return `"${val}"`
      }
      return 'NULL'
    }
  },
  'DOUBLE': {
    type: 'DOUBLE(20, 10)',
    convert: val => parseFloat(val || 0)
  },
  'TIME': {
    type: 'TIME',
    convert: val => {
      if (typeof val === 'string') val = normalizeTime(val)
      if (typeof val === 'number' || val instanceof Date) val = moment(val).format('HH:mm:ss')
      if (typeof val !== 'string') return 'NULL'
      return `"${val}"`
    }
  },
  'TINYINT': {
    type: 'TINYINT',
    convert: val => !!val
  },
  'VARCHAR': {
    type: 'VARCHAR(255)',
    convert: val => {
      val = (val || '').toString()
      val = val.substring(0, 255)
      val = sqlstring.escape(val) // escape \0 \b \t \n \r \x1a
      val = val.replace(controlRegex, '')
      return val
    }
  },
  'TEXT': {
    type: 'TEXT',
    convert: val => {
      val = (val || '').toString()
      val = sqlstring.escape(val) // escape \0 \b \t \n \r \x1a
      val = val.replace(controlRegex, '')
      return val
    }
  },
  'JSON': {
    type: 'JSON',
    convert: val => {
      if (val === null || val === undefined) {
        return null
      }
      return sqlstring.escape(JSON.stringify(val))
    }
  }
}

/**
 * Get a number expression from a date string
 * @param {string} str - date value
 * @returns {number|null} Timestamp in msec or null if not a valid value
 */
function getValueOfDate (str) {
  const reIso8601 = /^\d{4}-\d{2}-\d{2}([ T]\d{2}(:\d{2}(:\d{2}(\.\d{3})?)?)?([+-]\d{2}(:?\d{2})?)?)?$/
  const reIso8601Short = /^\d{4}\d{2}\d{2}(T\d{2}(\d{2}(\d{2}(\.\d{3})?)?)?)?$/
  const reTimestamp = /^\d+$/
  if (reIso8601.test(str) || reIso8601Short.test(str)) return moment(str).valueOf()
  if (reTimestamp.test(str)) return parseInt(str)
  return null
}

/**
 * Get a normalized time string
 * @param {string} str - time value
 * @returns {string|null} time or null
 */
function normalizeTime (str) {
  const re = /^(\d{1,2}):(\d{2})(:(\d{2}))?$/
  if (!re.test(str)) return null
  const found = str.match(re)
  const hh = found[1].length === 1 ? '0' + found[1] : found[1]
  const mm = found[2]
  const ss = found[4] || '00'
  return `${hh}:${mm}:${ss}`
}

module.exports = NATIVE_TYPES
