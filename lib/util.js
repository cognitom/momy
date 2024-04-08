function info (message) {
  process.stdout.write(message + '\n')
}
function error (message) {
  process.stderr.write(message + '\n')
}
export const logger = { info, error }

export function getDbNameFromUri (uri) {
  const matches = uri.match(/(?<=\/)\w+(?=\?|$)/)
  if (!matches) {
    throw new Error('No database name is specified in uri string.')
  }
  return matches.shift()
}
