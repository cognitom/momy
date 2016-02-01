const
  moment = require('moment'),
  changeCase = require('change-case'),

  TYPE_ALIASES = {
    'boolean': 'TINYINT',
    'number': 'BIGINT',
    'string': 'VARCHAR'
  },
  NATIVE_TYPES = {
    'BIGINT': {
      type: 'BIGINT',
      convert: val => val || 0
    },
    'DATE': {
      type: 'DATE',
      convert: val => {
        if (!val) return 'NULL'
        if (typeof val == 'number') val = moment(val).format('YYYY-MM-DD')
        if (!/\d{4}-\d{2}-\d{2}/.test(val)) return 'NULL'
        return `"${ val }"`
      }
    },
    'DATETIME': {
      type: 'DATETIME',
      convert: val => {
        if (!val) return 'NULL'
        if (typeof val == 'number') val = moment(val).format('YYYY-MM-DD HH:mm:ss')
        if (!/\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?/.test(val)) return 'NULL'
        return `"${ val }"`
      }
    },
    'TIME': {
      type: 'TIME',
      convert: val => {
        if (!val) return 'NULL'
        if (typeof val == 'number') val = moment(val).format('HH:mm:ss')
        if (!/\d{2}:\d{2}(:\d{2})?/.test(val)) return 'NULL'
        return `"${ val }"`
      }
    },
    'TINYINT': {
      type: 'TINYINT',
      convert: val => val === true ? 1 : 0
    },
    'VARCHAR': {
      type: 'VARCHAR(255)',
      convert: val => {
        val = (val || '')
          .toString()
          .replace(/\\/g, '\\\\') // escape backslashs
          .replace(/"/g, '\\"') // escape double quotations
        return `"${ val }"`
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
function createDefs(collections, dbName, prefix, fieldCase) {
  const
    acceptedTypes = Object.keys(TYPE_ALIASES).concat(Object.keys(NATIVE_TYPES))

  prefix = prefix || ''
  fieldCase = fieldCase || ''
  return Object.keys(collections).map(name => {
    const idName = collections[name]._id ? '_id' : 'id'
    return {
      name: name,
      ns: `${ dbName }.${ name }`,
      distName: prefix + name,
      // Primary key must be `_id` or `id`
      idName: idName,
      idDistName: fieldCase == 'camel' || fieldCase == 'snake' ? 'id' : idName,
      // `_id` or `id` must be string or number
      idType: collections[name][idName],
      fields: Object.keys(collections[name])
        // Skip unknown types
        .filter(fieldName => acceptedTypes.some(accepted =>
          accepted == collections[name][fieldName]))
        // Build definition
        .map(fieldName => {
          const
            fieldType = collections[name][fieldName],
            nativeType = TYPE_ALIASES[fieldType] || fieldType

          var distFieldName = fieldName
          if (fieldCase == 'camel') distFieldName = changeCase.camelCase(distFieldName)
          if (fieldCase == 'snake') distFieldName = changeCase.snakeCase(distFieldName)
          return {
            name: fieldName,
            distName: distFieldName,
            type: NATIVE_TYPES[nativeType].type,
            convert: NATIVE_TYPES[nativeType].convert
          }
        })
    }
  })
}

// export it for testing
createDefs.NATIVE_TYPES = NATIVE_TYPES

module.exports = createDefs
