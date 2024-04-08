export default function wait (msec) {
  return new Promise(function (resolve) {
    setTimeout(() => resolve(), msec)
  })
}
