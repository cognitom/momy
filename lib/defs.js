'use strict'

const moment = require('moment')
const changeCase = require('change-case')
const sqlstring = require('sqlstring')

const controlRegex = /[\x00-\x1F\x7F]/g // eslint-disable-line no-control-regex
const TYPE_ALIASES = {
  'boolean': 'TINYINT',
  'number': 'DOUBLE',
  'string': 'VARCHAR'
}
const NATIVE_TYPES = {
  'BIGINT': {
    type: 'BIGINT',
    convert: val => parseInt(val || 0)
  },
  'DATE': {
    type: 'DATE',
    convert: val => {
      if (typeof val === 'string') val = getValueOfDate(val)
      if (typeof val !== 'number') return 'NULL'
      val = moment(val).format('YYYY-MM-DD')
      return `"${val}"`
    }
  },
  'DATETIME': {
    type: 'DATETIME',
    convert: val => {
      if (typeof val === 'string') val = getValueOfDate(val)
      if (typeof val !== 'number') return 'NULL'
      val = moment(val).format('YYYY-MM-DD HH:mm:ss')
      return `"${val}"`
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
      if (typeof val === 'number') val = moment(val).format('HH:mm:ss')
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
  }
}

/**
 * Create definition list from config object
 * @param {object} collections - config object
 * @param {string} dbName - name of database
 * @param {string} prefix - optional prefix for table
 * @param {string} fieldCase - `snake` or `camel`
 * @returns {undefined} void
 */
function createDefs (collections, dbName, prefix, fieldCase) {
  const acceptedTypes = Object.keys(TYPE_ALIASES).concat(Object.keys(NATIVE_TYPES))

  prefix = prefix || ''
  fieldCase = fieldCase || ''
  return Object.keys(collections).map(name => {
    const idName = collections[name]._id ? '_id' : 'id'
    return {
      name: name,
      ns: `${dbName}.${name}`,
      distName: prefix + name,
      // Primary key must be `_id` or `id`
      idName: idName,
      idDistName: fieldCase === 'camel' || fieldCase === 'snake' ? 'id' : idName,
      // `_id` or `id` must be string or number
      idType: collections[name][idName],
      fields: Object.keys(collections[name])
        // Skip unknown types
        .filter(fieldName => acceptedTypes.some(accepted =>
          accepted === collections[name][fieldName]))
        // Build definition
        .map(fieldName => {
          const fieldType = collections[name][fieldName]
          const nativeType = TYPE_ALIASES[fieldType] || fieldType

          let distFieldName = fieldName
          if (fieldCase === 'camel') distFieldName = changeCase.camelCase(distFieldName)
          if (fieldCase === 'snake') distFieldName = changeCase.snakeCase(distFieldName)
          return {
            name: fieldName,
            distName: distFieldName,
            type: NATIVE_TYPES[nativeType].type,
            convert: NATIVE_TYPES[nativeType].convert,
            // set primary for 'id' or '_id'
            primary: !!~['id', '_id'].indexOf(fieldName)
          }
        })
    }
  })
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

// export it for testing
createDefs.NATIVE_TYPES = NATIVE_TYPES

module.exports = createDefs
