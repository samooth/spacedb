const Builder = require('../../builder')

module.exports = function createDatabase (opts) {
  const spec = new Builder(opts)

  const db = spec.namespace('db')

  db.schema.register({
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

  db.collections.register({
    name: 'members',
    schema: '@db/member',
    key: ['id']
  })

  db.indexes.register({
    name: 'members-by-age',
    collection: '@db/members',
    key: ['age']
  })

  return spec
}
