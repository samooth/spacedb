const Hyperschema = require('hyperschema')
const HyperDB = require('.')

const SCHEMA_DIR = './output/hyperschema'
const DB_DIR = './output/hyperdb'

const schema = Hyperschema.from(SCHEMA_DIR)
const example = schema.namespace('example')

example.register({
  name: 'alias1',
  alias: 'fixed32'
})

example.register({
  name: 'struct1',
  compact: true,
  fields: [
    {
      name: 'field1',
      type: '@example/alias1',
      required: true
    },
    {
      name: 'field1',
      type: 'uint',
      required: true
    }
  ]
})

example.register({
  name: 'struct2',
  fields: [
    {
      name: 'field1',
      type: '@example/struct1',
      required: true
    },
    {
      name: 'field2',
      type: '@example/struct1',
      required: true
    },
    {
      name: 'name',
      type: 'string'
    }
  ]
})

example.register({
  name: 'record1',
  fields: [
    {
      name: 'id1',
      type: 'uint',
      required: true
    },
    {
      name: 'id2',
      type: 'uint',
      required: true
    },
    {
      name: 'struct1',
      type: '@example/struct2',
      required: true
    },
    {
      name: 'text',
      type: 'string'
    }
  ]
})

Hyperschema.toDisk(schema)

const db = HyperDB.from(SCHEMA_DIR, DB_DIR)
const exampleDb = db.namespace('example')

exampleDb.collections.register({
  name: 'collection1',
  schema: '@example/record1',
  key: ['id1', 'id2']
})

exampleDb.indexes.register({
  name: 'collection1-by-struct',
  collection: '@example/collection1',
  key: ['struct1.field1', 'struct1.field2'],
  unique: true
})

HyperDB.toDisk(db)
