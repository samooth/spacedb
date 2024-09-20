const { test } = require('./helpers')

test('simple trigger that makes a manual count', async function ({ build }, t) {
  const db = await build(createExampleDB)

  await db.insert('@example/members', { name: 'test', age: 10 })

  {
    const digest = await db.get('@example/digest')
    t.alike(digest, { count: 1 })
  }

  await db.insert('@example/members', { name: 'Test', age: 11 })

  {
    const digest = await db.get('@example/digest')
    t.alike(digest, { count: 2 })
  }

  await db.insert('@example/members', { name: 'Test', age: 12 })

  {
    const digest = await db.get('@example/digest')
    t.alike(digest, { count: 2 })
  }

  await db.close()
})

function createExampleDB (HyperDB, Hyperschema, paths) {
  const schema = Hyperschema.from(paths.schema)
  const example = schema.namespace('example')

  example.register({
    name: 'digest',
    fields: [
      {
        name: 'count',
        type: 'uint',
        required: true
      }
    ]
  })

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
    name: 'digest',
    schema: '@example/digest',
    key: []
  })

  exampleDB.collections.register({
    name: 'members',
    schema: '@example/member',
    key: ['name'],
    async trigger (db, key, record) {
      const digest = (await db.get('@example/digest')) || { count: 0 }

      const prev = !!(await db.get('@example/members', key))
      const next = !!record

      if (prev === next) return

      digest.count += next ? 1 : -1

      await db.insert('@example/digest', digest)
    }
  })

  HyperDB.toDisk(db)
}
