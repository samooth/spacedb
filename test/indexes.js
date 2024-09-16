const test = require('brittle')
const { build } = require('./helpers')

test('testing', async function (t) {
  const db = await build(t, createExampleDB)

  await db.insert('@example/members', { name: 'test', age: 10 })
  await db.insert('@example/members', { name: 'Test', age: 11 })

  {
    const all = await db.find('@example/members-by-name').toArray()
    t.alike(all, [{ name: 'Test', age: 11 }])
  }

  await db.flush()

  {
    const all = await db.find('@example/members-by-name').toArray()
    t.alike(all, [{ name: 'Test', age: 11 }])
  }

  await db.close()
})

function createExampleDB (HyperDB, Hyperschema, paths) {
  const schema = Hyperschema.from(paths.schema)
  const example = schema.namespace('example')

  example.register({
    name: 'member',
    fields: [
      {
        name: 'name',
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

  Hyperschema.toDisk(schema)

  const db = HyperDB.from(paths.schema, paths.db)
  const exampleDB = db.namespace('example')

  exampleDB.collections.register({
    name: 'members',
    schema: '@example/member',
    key: ['name']
  })

  exampleDB.indexes.register({
    name: 'members-by-name',
    collection: '@example/members',
    unique: true,
    key: {
      type: {
        fields: [
          {
            name: 'name',
            type: 'string'
          }
        ]
      },
      map (record, context) {
        const name = record.name.toLowerCase().trim()
        return name ? [name] : []
      }
    }
  })

  HyperDB.toDisk(db)
}
