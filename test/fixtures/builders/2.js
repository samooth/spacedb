const SpaceDB = require('../../../builder')
const Spaceschema = require('spaceschema')
const path = require('path')

const SCHEMA_DIR = path.join(__dirname, '../generated/2/spaceschema')
const DB_DIR = path.join(__dirname, '../generated/2/spacedb')

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

Spaceschema.toDisk(schema)

const db = SpaceDB.from(SCHEMA_DIR, DB_DIR)
const testDb = db.namespace('db')

testDb.collections.register({
  name: 'members',
  stats: true,
  schema: '@db/member',
  key: ['id']
})

testDb.indexes.register({
  name: 'members-by-age',
  collection: '@db/members',
  key: ['age']
})

SpaceDB.toDisk(db)
