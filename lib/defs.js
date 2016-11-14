'use strict'

const changeCase = require('change-case')
const NATIVE_TYPES = require('./types')
const TYPE_ALIASES = require('./aliases')

/**
 * Create definition list from config object
 * @param {object} collections - config object
 * @param {string} dbName - name of database
 * @param {string} opts - options
 * @returns {undefined} void
 */
function createDefs (collections, dbName, opts) {
  const acceptedTypes = Object.keys(TYPE_ALIASES).concat(Object.keys(NATIVE_TYPES))
  return Object.keys(collections).map(name => {
    // Primary key must be `_id` or `id`
    const idName = collections[name]._id ? '_id' : 'id'
    return {
      name,
      ns: `${dbName}.${name}`,
      distName: opts.prefix + name,
      idName,
      idDistName: convertCase(idName, opts.fieldCase),
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
          const convert = NATIVE_TYPES[nativeType].convert
          return {
            name: fieldName,
            distName: convertCase(fieldName, opts.fieldCase),
            type: NATIVE_TYPES[nativeType].type,
            convert: val => {
              const isText = nativeType === 'VARCHAR' || nativeType === 'TEXT'
              if (isText) val = filterChars(val, opts.exclusions, opts.inclusions)
              return convert(val)
            },
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

/**
 * Exclude or include some chars
 * @param {string} str - string
 * @param {string} exclusions - char set to exclude
 * @param {string} inclusions - char set to include
 * @returns {string} converted string
 */
function filterChars (str, exclusions, inclusions) {
  str = str || ''
  if (typeof str !== 'string') str = str.toString()
  if (exclusions) str = str.replace(new RegExp(`[${exclusions}]`, 'g'), '')
  if (inclusions) str = str.replace(new RegExp(`[^${inclusions}]`, 'g'), '')
  return str
}

module.exports = createDefs
