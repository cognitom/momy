const
  moment = require('moment'),

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
      type: 'VARCHAR(1000)',
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
 * @returns {undefined} void
 */
function createDefs(collections, dbName, prefix) {
  const
    acceptedTypes = Object.keys(TYPE_ALIASES).concat(Object.keys(NATIVE_TYPES))

  prefix = prefix || ''
  return Object.keys(collections).map(name => ({
    name: name,
    ns: `${ dbName }.${ name }`,
    distName: prefix + name,
    idType: collections[name]._id,
    fields: Object.keys(collections[name])
      // Skip unknown types
      .filter(fieldName => acceptedTypes.some(accepted =>
        accepted == collections[name][fieldName]))
      // Build definition
      .map(fieldName => {
        const
          fieldType = collections[name][fieldName],
          nativeType = TYPE_ALIASES[fieldType] || fieldType

        return {
          name: fieldName,
          type: NATIVE_TYPES[nativeType].type,
          convert: NATIVE_TYPES[nativeType].convert
        }
      })
  }))
}

// export it for testing
createDefs.NATIVE_TYPES = NATIVE_TYPES

module.exports = createDefs
