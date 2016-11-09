'use strict'

module.exports = function wait (msec) {
  return new Promise(function (resolve) {
    setTimeout(() => resolve(), msec)
  })
}
