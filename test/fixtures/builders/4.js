const SpaceDB = require('../../../builder')
const Spaceschema = require('spaceschema')
const path = require('path')

const SCHEMA_DIR = path.join(__dirname, '../generated/4/spaceschema')
const DB_DIR = path.join(__dirname, '../generated/4/spacedb')

const schema = Spaceschema.from(SCHEMA_DIR)

const dbSchema = schema.namespace('db')

dbSchema.register({
  name: 'member',
  fields: [
    {
      name: 'id',
      type: 'string',
      required: true
    },
    {
      name: 'age',
      type: 'uint',
      required: true
    }
  ]
})

dbSchema.register({
  name: 'nested',
  fields: [
    {
      name: 'member',
      type: '@db/member',
      required: true
    },
    {
      name: 'fun',
      type: 'bool'
    }
  ]
})

Spaceschema.toDisk(schema)

const db = SpaceDB.from(SCHEMA_DIR, DB_DIR)
const testDb = db.namespace('db')

testDb.collections.register({
  name: 'nested-members',
  schema: '@db/nested',
  key: ['member.id']
})

SpaceDB.toDisk(db)
