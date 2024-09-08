const p = require('path')

const Hyperschema = require('hyperschema')
const HyperDB = require('.')

const OUTPUT_DIR = './output'
const SCHEMA_DIR = p.join(OUTPUT_DIR, 'hyperschema')
const DB_DIR = p.join(OUTPUT_DIR, 'hyperdb')

const schema = Hyperschema.from(SCHEMA_DIR)
const keetSchema = schema.namespace('keet')

keetSchema.register({
  name: 'core-key',
  alias: 'fixed32'
})

keetSchema.register({
  name: 'oplog-message-id',
  compact: true,
  fields: [
    {
      name: 'key',
      type: '@keet/core-key',
      required: true
    },
    {
      name: 'seq',
      type: 'uint',
      required: true
    }
  ]
})

keetSchema.register({
  name: 'device',
  fields: [
    {
      name: 'key',
      type: '@keet/core-key',
      required: true
    },
    {
      name: 'swarmKey',
      type: '@keet/core-key',
      required: true
    },
    {
      name: 'name',
      type: 'string'
    }
  ]
})

keetSchema.register({
  name: 'chat-message',
  fields: [
    {
      name: 'thread',
      type: 'uint',
      required: true
    },
    {
      name: 'seq',
      type: 'uint',
      required: true
    },
    {
      name: 'messageId',
      type: '@keet/oplog-message-id',
      required: true
    },
    {
      name: 'text',
      type: 'string'
    }
  ]
})

Hyperschema.toDisk(schema, SCHEMA_DIR)

const db = HyperDB.from(SCHEMA_DIR, DB_DIR)
const keetDb = db.namespace('keet')

keetDb.collections.register({
  name: 'chat',
  schema: '@keet/chat-message',
  key: ['thread', 'seq']
})

keetDb.indexes.register({
  name: 'chat-by-message-id',
  collection: '@keet/chat',
  key: ['messageId.key', 'messageId.seq'],
  unique: true
})

HyperDB.toDisk(db, DB_DIR)
