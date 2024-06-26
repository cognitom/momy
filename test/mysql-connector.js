import mysql from 'mysql'

export class MysqlConnector {
  constructor (url) {
    this.con = mysql.createConnection(url)
    this.con.connect(function (err) {
      if (err) console.log(`SQL CONNECT ERROR: ${err}`)
    })
  }
  query (sql) {
    return new Promise((resolve, reject) => {
      this.con.query(sql, (err, results) => {
        if (err) return reject(err)
        resolve(results)
      })
    })
  }
  close () {
    this.con.end()
  }
}
