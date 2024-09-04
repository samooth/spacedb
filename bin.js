#!/usr/bin/env node
const p = require('path')
const fs = require('fs')

const HyperDB = require('./builder')

const args = process.argv.slice(2)
const input = args[0]
const output = args[1]
if (!input || !output) {
  console.error('Usage: hyperdb [input.js] [outputDir]')
  process.exit(1)
}

const inputSchemaPath = p.resolve(input)
const outputDirPath = p.resolve(output)

const outputDBPath = p.join(outputDirPath, 'index.js')
const outputJsonPath = p.join(outputDirPath, 'schema.json')
const outputCencPath = p.join(outputDirPath, 'messages.js')

let exists = false
try {
  fs.statSync(outputJsonPath)
  exists = true
} catch (err) {
  if (err.code !== 'ENOENT') throw err
}
if (!exists) {
  fs.mkdirSync(output, { recursive: true })
}

let previousJson = null
let previous = null
if (exists) {
  previousJson = require(outputJsonPath)
  previous = HyperDB.Builder.fromJSON(previousJson)
}

const next = require(inputSchemaPath)({ previous })
const { json, messages, db } = next.compile()

if (previous && (json.version === previousJson.version)) {
  console.log('Schema has not been changed.')
  process.exit(0)
}

fs.writeFileSync(outputJsonPath, JSON.stringify(json, null, 2) + '\n')
fs.writeFileSync(outputCencPath, messages)
fs.writeFileSync(outputDBPath, db)

console.log('Schema JSON snapshot written to ' + outputJsonPath)
console.log('Database description written to ' + outputDBPath)
console.log('Compact encodings written to ' + outputCencPath)
process.exit(0)
