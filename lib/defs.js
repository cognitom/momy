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
    convert: val => val || 0
  },
  'DATE': {
    type: 'DATE',
    convert: val => {
      if (!val) return 'NULL'
      if (typeof val === 'string' || typeof val === 'number') {
        const m = moment(val).isValid() ? moment(val) : moment(val, 'x')
        val = m.format('YYYY-MM-DD')
      }
      if (!/\d{4}-\d{2}-\d{2}/.test(val)) return 'NULL'
      return `"${val}"`
    }
  },
  'DATETIME': {
    type: 'DATETIME',
    convert: val => {
      if (!val) return 'NULL'
      if (typeof val === 'string' || typeof val === 'number') {
        const m = moment(val).isValid() ? moment(val) : moment(val, 'x')
        val = m.format('YYYY-MM-DD HH:mm:ss')
      }
      if (!/\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?/.test(val)) return 'NULL'
      return `"${val}"`
    }
  },
  'DOUBLE': {
    type: 'DOUBLE(20, 10)',
    convert: val => val || 0
  },
  'TIME': {
    type: 'TIME',
    convert: val => {
      if (!val) return 'NULL'
      if (typeof val === 'number') val = moment(val).format('HH:mm:ss')
      if (!/\d{2}:\d{2}(:\d{2})?/.test(val)) return 'NULL'
      return `"${val}"`
    }
  },
  'TINYINT': {
    type: 'TINYINT',
    convert: val => val === true ? 1 : 0
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

// export it for testing
createDefs.NATIVE_TYPES = NATIVE_TYPES

module.exports = createDefs
