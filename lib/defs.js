'use strict'

const changeCase = require('change-case')
const NATIVE_TYPES = require('./types')
const TYPE_ALIASES = require('./aliases')

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
    // Primary key must be `_id` or `id`
    const idName = collections[name]._id ? '_id' : 'id'
    return {
      name,
      ns: `${dbName}.${name}`,
      distName: prefix + name,
      idName,
      idDistName: convertCase(idName, fieldCase),
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
          return {
            name: fieldName,
            distName: convertCase(fieldName, fieldCase),
            type: NATIVE_TYPES[nativeType].type,
            convert: NATIVE_TYPES[nativeType].convert,
            primary: /^_?id$/.test(fieldName) // set primary for 'id' or '_id'
          }
        })
    }
  })
}

/**
 * Change case of string
 * @param {string} str - field name
 * @returns {string} converted string
 */
function convertCase (str, fieldCase) {
  return fieldCase === 'camel' ? changeCase.camelCase(str)
    : fieldCase === 'snake' ? changeCase.snakeCase(str)
    : str
}

module.exports = createDefs
