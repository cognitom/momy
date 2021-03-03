'use strict'

const mysql = require('mysql')
const util = require('util')
const _ = require('lodash')
const { exit } = require('process')
const { table } = require('console')
const fs = require('fs')

/**
 * MySQL helper
 * @class
 */
class MySQL {
  /**
   * Constructor
   * @param {string} url - database url
   */
  constructor (url) {
    this.url = url || 'mysql://localhost/test?user=root'
    this.dbName = this.url.split(/\/|\?/)[3]
    this.con = null
    const ddl = 'CREATE TABLE IF NOT EXISTS `mongo_to_mysql` (service varchar(20), timestamp BIGINT) ENGINE=MyISAM;'
    const log = `INSERT INTO mongo_to_mysql ` +
      `(service, timestamp) VALUES ("${this.dbName}", 0);`
    this.query(ddl)
      .then(() => this.query(log))
  }

  /**
   * Insert the record
   * @param {object} def - definition of fields
   * @param {object} item - the data of the record to insert
   * @param {boolean} replaceFlag - set true to replace the record
   * @param {function} callback - callback
   */
  insert (def, item, replaceFlag, callback) {
    if (typeof replaceFlag === 'function') {
      callback = replaceFlag
      replaceFlag = false
    }
    const command = replaceFlag ? 'REPLACE' : 'INSERT'
    const fs = def.fields.map(field => '`' + field.distName + '`')
    const vs = def.fields.map(field => field.convert(getFieldVal(field.name, item)))
    const sql = `${command} INTO \`${def.distName}\`` +
      ` (${fs.join(', ')}) VALUES (${vs.join(', ')});`
    const promise = this.query(sql)
      .then(() => this.optimize(def.distName))
      .catch(err => {
        util.log(sql)
        throw err
      })

    if (callback) promise.then(() => callback())
  }

  /**
   * Insert the relation record
   * @param {object} def - definition of fields
   * @param {object} item - the data of the record to insert
   * @param {object} relation - defined relation
   * @param {boolean} replaceFlag - set true to replace the record
   * @param {function} callback - callback
   */
  insertRelation (def, item, relation, replaceFlag, callback) {
    if (typeof replaceFlag === 'function') {
      callback = replaceFlag
      replaceFlag = false
    }
    const command = replaceFlag ? 'REPLACE' : 'INSERT'
    const fs = def.fields.map(field => '`' + field.distName + '`')
    let sql = `${command} INTO \`${def.distName}\` (${fs.join(', ')})`
    let queryBind = null
    if (relation.dataType === "array") {
      queryBind = item
      sql += ` VALUES (${Array(item.length).fill('?').join(', ')})`
    } else if (relation.dataType === "arrayObject") {
      const values = def.fields.map(field => field.convert(getFieldVal(field.name, item)))
      sql += ` VALUES (${values.join(', ')})`
    } else {
      console.error("Unknown data type of relation")
      exit(1)
    }
    const promise = this.query(sql, queryBind)
      .then(() => this.optimize(def.distName))
      .catch(err => {
        util.log(sql)
        throw err
      })
    if (callback) promise.then(() => callback())
  }

  /**
   * Update the record
   * @param {object} def - definition of fields
   * @param {string} id - the id of the record to update
   * @param {object} item - the columns to update
   * @param {object} unsetItems - the columns to drop
   * @param {function} callback - callback
   */
  update (def, id, item, unsetItems, callback) {
    const fields = def.fields.filter(field =>
      !!item && typeof getFieldVal(field.name, item) !== 'undefined' ||
      !!unsetItems && typeof getFieldVal(field.name, unsetItems) !== 'undefined')
    const sets = fields.map(field => {
      const val = field.convert(getFieldVal(field.name, item))
      return `\`${field.distName}\` = ${val}`
    })
    if (!sets.length) return

    const setsStr = sets.join(', ')
    const id2 = def.idType === 'number' ? id : `'${id}'`
    const sql = `UPDATE \`${def.distName}\` SET ${setsStr} WHERE ${def.idDistName} = ${id2};`
    const promise = this.query(sql)
      .then(() => this.optimize(def.distName))
      .catch(err => {
        util.log(sql)
        console.log(`${new Date()}: MySQL.update -> ${err} \n`)
        throw err
      })

    if (callback) promise.then(() => callback())
  }

  /**
   * Update the relation record
   * @param {object} def - definition of fields
   * @param {object} item - the data of the record to insert
   * @param {object} relation - defined relation
   * @param {boolean} deleteFlag - determine removing data existing from table
   * @param {function} callback - callback
   */
  async updateRelation (def, item, relation, replaceFlag, callback) {
    if (typeof replaceFlag === 'function') {
      callback = replaceFlag
      replaceFlag = false
    }
    const command = replaceFlag ? 'REPLACE' : 'INSERT'
    const fs = def.fields.map(field => '`' + field.distName + '`')
    let sql = `${command} INTO \`${def.distName}\` (${fs.join(', ')})`
    let queryBind = null
    if (relation.dataType === "array") {
      queryBind = item
      sql += ` VALUES (${Array(item.length).fill('?').join(', ')})`
      await this.removeRelation(def.distName, item[0])
    } else if (relation.dataType === "arrayObject") {
      const values = def.fields.map(field => field.convert(getFieldVal(field.name, item)))
      sql += ` VALUES (${values.join(', ')})`
      await this.removeRelation(def.distName, item.parent_id)
    } else {
      console.log(`${new Date()}: MySQL.updateRelation -> Unknown data type of relation \n`)
      exit(1)
    }
    const promise = this.query(sql, queryBind)
      .then(() => this.optimize(def.distName))
      .catch(err => {
        util.log(sql)
        throw err
      })
    if (callback) promise.then(() => callback())
  }

  /**
   * Remove the record
   * @param {object} def - definition of fields
   * @param {string} id - the id of the record to remove
   * @param {function} callback - callback
   */
  remove (def, id, callback) {
    const id2 = def.idType === 'number' ? id : `'${id}'`
    const sql = `DELETE FROM \`${def.distName}\` WHERE ${def.idDistName} = ${id2};`
    const promise = this.query(sql)
      .catch(err => {
        util.log(sql)
        console.log(`${new Date()}: MySQL.remove -> ${err} \n`)
        throw err
      })

    if (callback) promise.then(() => callback())
  }
  
  /**
   * Remove the relation record
   * @param {object} def - definition of fields
   * @param {string} id - the id of the record to remove
   * @param {function} callback - callback
   */
  removeRelation (tableName, id, callback) {
    const sql = `DELETE FROM \`${tableName}\` WHERE \`parent_id\` = ?;`
    const promise = this.query(sql, [id])
      .catch(err => {
        util.log(sql)
        console.log(`${new Date()}: MySQL.removeRelation -> ${err} \n`)
        throw err
      })

    if (callback) promise.then(() => callback())
    return promise
  }

  /**
   * Create table
   *
   * @param   {[String]}  name      table name
   * @param   {[Array Object]}  fields    table column
   * @param   {[Function]}  callback  callback function passed to promise
   *
   * @return  {[Promise]}            Promise Query
   */
  createTable(name, fields, callback) {
    const columns = fields.map(field => `\`${field.distName}\` ${field.type} ${field.primary ? 'PRIMARY KEY' : ''}`.trim())
    const ddl = `CREATE TABLE IF NOT EXISTS \`${name}\` (${columns.join(', ')}) ENGINE=MyISAM;`
    return this.query(ddl)
      .then(() => callback && callback())
  }

  /**
   * Bulk create table
   *
   * @param   {[Array Object]}  defs    definition of collection
   *
   * @return  {[Promise]} Promise Query
   */
  bulkCreateTable(defs) {
    const list = _.cloneDeep(defs)
    if (!list.length) return
    const def = _.cloneDeep(list[0])
    list.shift()
    return this.createTable(
      def.distName,
      def.fields,
      () => this.bulkCreateTable(list)
    )
  }

  /**
   * Drop table
   *
   * @param   {[String]}  name      table name
   * @param   {[Function]}  callback  callback function passed to promise
   *
   * @return  {[Promise]}            Promise Query
   */
  dropTable(name, callback) {
    const ddl = `DROP TABLE IF EXISTS \`${name}\`;`
    return this.query(ddl)
      .then(() => callback && callback())
  }

  /**
   * Bulk drop table
   *
   * @param   {[Array Object]}  defs  definition of collection
   *
   * @return  {[Promise]}        Promise Query
   */
  bulkDropTable(defs) {
    const list = _.cloneDeep(defs)
    if (!list.length) return
    const def = _.cloneDeep(list[0])
    list.shift()
    return this.dropTable(
      def.distName,
      () => this.bulkDropTable(list)
    )
  }

  /**
   * Optimize table
   * @return {[Promise]} Query Promise
   */
  optimize(tableName) {
    const sql = `OPTIMIZE TABLE \`${tableName}\``
    return this.query(sql, [])
      .catch(err => {
        util.log(sql)
        console.log(`${new Date()}: MySQL.optimize -> ${err} \n`)
        throw err
      })
  }

  /**
   * Read timestamp
   * @returns {Promise} with timestamp
   */
  readTimestamp () {
    let q = 'SELECT timestamp FROM mongo_to_mysql' +
      ` WHERE service = '${this.dbName}'`
    return this.query(q)
      .then(results => results[0] && results[0].timestamp || 0)
      .catch(err => {
        util.log(q)
        console.log(`${new Date()}: MySQL.readTimestamp -> ${err} \n`)
        throw err
      })
  }

  /**
   * Update timestamp
   * @param {number} ts - a new timestamp
   */
  updateTimestamp (ts) {
    let q = `UPDATE mongo_to_mysql SET timestamp = ${ts}` +
      ` WHERE service = '${this.dbName}';`
    this.getConnection()
      .query(q)
  }

  /**
   * Connect to MySQL
   * @returns {connection} MySQL connection
   */
  getConnection () {
    if (this.con &&
      this.con._socket &&
      this.con._socket.readable &&
      this.con._socket.writable) return this.con

    const params = 'multipleStatements=true'
    const url = this.url + (/\?/.test(this.url) ? '&' : '?') + params
    const con = mysql.createConnection(url)

    util.log('Connect to MySQL...')
    con.connect(function (err) {
      if (err) {
        console.log(`${new Date()}: MySQL.getConnection.onConnect -> ${err} \n`)
        return util.log(`SQL CONNECT ERROR: ${err}`)
      }
    })
    con.on('close', () => {
      return util.log('SQL CONNECTION CLOSED.')
    })
    con.on('error', err => {
      console.log(`${new Date()}: MySQL.getConnection.onError -> ${err} \n`)
      return util.log(`SQL CONNECTION ERROR: ${err}`)
    })

    return (this.con = con)
  }

  /**
   * Query method with promise
   * @param {string} sql - SQL string
   * @returns {Promise} with results
   */
  query (sql, queryBinding = []) {
    // fs.appendFileSync('logs/query.log', `${new Date()}: ${sql} ${(!queryBinding || queryBinding == 0)  ? '' : `with binding ${JSON.stringify(queryBinding)}`} \n`)
    return new Promise((resolve, reject) => {
      this.getConnection()
        .query(sql, queryBinding, (err, results) => {
          if (err) {
            // fs.aappendFileSync('logs/error.log', `${new Date()}: MySQL.query -> ${err} \n`)
            reject(err)
          } else {
            resolve(results)
          }
        })
    })
  }
}

function getFieldVal (name, record) {
  return name.split('.').reduce((p, c) => p && p[c], record)
}

module.exports = MySQL
