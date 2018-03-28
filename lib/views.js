'use strict'

/**
 * Create definition list from config object
 * @param {object} views - config object
 * @param {string} dbName q- name of database
 * @returns {undefined} void
 */
function createViewDefs (views, dbName, defs) {
  return Object.keys(views).map(name => {

    var joins = Object.keys(views[name].joins);
    var sourceTableDef;
    defs.forEach(def => {
      if(def.distName == views[name].sourceTableName){
        sourceTableDef = def;
      }
    })
    return {
      name: name,
      ns: `${dbName}.${name}`,
      sourceTablePrefix: views[name].sourceTablePrefix,
      sourceTableName: views[name].sourceTableName,
      joins: joins
        // Build definition
        .map(sourceTable => {
          var targetDef;
          defs.forEach(def => {
            if(def.distName == views[name].joins[sourceTable].tableToJoin){
              targetDef = def;
            }
          })

          return {
            sourceDef: sourceTableDef,
            targetDef: targetDef,
            name: sourceTable,
            targetTablePrefix: views[name].joins[sourceTable].targetTablePrefix,
            joinOn: views[name].joins[sourceTable].joinOn,
            tableToJoin: views[name].joins[sourceTable].tableToJoin,
            joinBy: views[name].joins[sourceTable].joinBy,
            joinIDfrom: views[name].joins[sourceTable].joinIDfrom,
            joinIDto: views[name].joins[sourceTable].joinIDto  
          }
        })
    }
  })
}

module.exports = createViewDefs