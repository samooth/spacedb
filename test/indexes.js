const test = require('brittle')
const { build } = require('./helpers')

test('members with unique index', async function (t) {
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

test('members with non-unique index', async function (t) {
  const db = await build(t, createExampleDB)

  await db.insert('@example/members', { name: 'test', age: 10 })
  await db.insert('@example/members', { name: 'john', age: 14 })
  await db.insert('@example/members', { name: 'bob', age: 14 })
  await db.insert('@example/members', { name: 'alice', age: 18 })

  {
    const all = await db.find('@example/teenagers').toArray()
    t.alike(all, [
      { name: 'bob', age: 14 },
      { name: 'john', age: 14 },
      { name: 'alice', age: 18 }
    ])
  }

  await db.flush()

  {
    const all = await db.find('@example/teenagers').toArray()
    t.alike(all, [
      { name: 'bob', age: 14 },
      { name: 'john', age: 14 },
      { name: 'alice', age: 18 }
    ])
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

  exampleDB.indexes.register({
    name: 'teenagers',
    collection: '@example/members',
    key: {
      type: {
        fields: [
          {
            name: 'age',
            type: 'uint'
          }
        ]
      },
      map (record, context) {
        if (record.age < 13 || record.age > 19) return []
        return [{ age: record.age }]
      }
    }
  })

  HyperDB.toDisk(db)
}
