const HyperDB = require('../../builder')
const Hyperschema = require('hyperschema')
const path = require('path')

const SCHEMA_DIR = path.join(__dirname, './generated/1/hyperschema')
const DB_DIR = path.join(__dirname, './generated/1/hyperdb')

const schema = Hyperschema.from(SCHEMA_DIR)

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

Hyperschema.toDisk(schema, SCHEMA_DIR)

const db = HyperDB.from(SCHEMA_DIR, DB_DIR)
const testDb = db.namespace('db')

testDb.collections.register({
  name: 'members',
  schema: '@db/member',
  key: ['id']
})

testDb.indexes.register({
  name: 'members-by-age',
  collection: '@db/members',
  key: ['age']
})

HyperDB.toDisk(db, DB_DIR)
