const
  ACCEPTED_TYPES = ['number', 'string', 'boolean']

/**
 * Create definition list from config object
 * @param {object} collections - config object
 * @param {string} dbName - name of database
 * @param {string} prefix - optional prefix for table
 * @returns {undefined} void
 */
function createDefs(collections, dbName, prefix) {
  prefix = prefix || ''
  return Object.keys(collections).map(name => ({
    name: name,
    ns: `${ dbName }.${ name }`,
    distName: prefix + name,
    idType: collections[name]._id,
    fields: Object.keys(collections[name])
      .map(fieldName => ({
        name: fieldName,
        type: collections[name][fieldName]
      }))
      // Skip unknown types
      .filter(field => ACCEPTED_TYPES.some(type => type == field.type))
  }))
}

module.exports = createDefs
