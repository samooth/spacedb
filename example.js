const HyperDB = require('./builder')

module.exports = function createKeetDatabase (opts) {
  const spec = new HyperDB.Builder({ ...opts, offset: 2 })

  // Will internally create enums for collections/indexes
  // These two namespaces are legacy prefixed
  const keet = spec.namespace('keet')

  keet.schema.register({
    name: 'core-key',
    alias: 'fixed32'
  })

  keet.schema.register({
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

  keet.schema.register({
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

  keet.schema.register({
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

  keet.collections.register({
    name: 'chat',
    schema: '@keet/chat-message',
    key: ['thread', 'seq']
  })

  keet.indexes.register({
    name: 'chat-by-message-id',
    collection: '@keet/chat',
    key: ['messageId.key', 'messageId.seq'],
    unique: true
  })

  keet.collections.register({
    name: 'devices',
    schema: '@keet/device',
    key: ['key']
  })

  keet.indexes.register({
    name: 'devices-by-swarm-key',
    collection: '@keet/devices',
    key: ['swarmKey'],
    unique: true
  })

  keet.indexes.register({
    name: 'devices-by-name',
    collection: '@keet/devices',
    key: ['name']
  })

  return spec
}
