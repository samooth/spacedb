# spacedb

Database built for P2P and local indexing

```
npm install spacedb
```

## Usage

First generate your definition with the builder.
The definition defines the schemas and collections you wanna use.

```js
// build.js
const Spaceschema = require('spaceschema')
const SpaceDB = require('spacedb/builder')

const SCHEMA_DIR = './spec/spaceschema' // where the schema definitions are written
const DB_DIR = './spec/spacedb' // Where to install db definition

// Spaceschema definitions
const schema = Spaceschema.from(SCHEMA_DIR)
const example = schema.namespace('example')

// Define 'members'
example.register({
  name: 'members',
  compact: true,
  fields: [
    {
      name: 'name',
      type: 'string',
      required: true,
    },
    {
      name: 'age',
      type: 'uint',
      required: true,
    },
  ]
})
Spaceschema.toDisk(schema)

// Spacedb collection definitions
const db = SpaceDB.from(SCHEMA_DIR, DB_DIR)
const exampleDB = db.namespace('example')

// Import helpers (see next step)
exampleDB.require('./helpers.js')

// Define collections of structs
exampleDB.collections.register({
  name: 'members',
  schema: '@example/members',
  key: ['name']
})

// Define index over collection
exampleDB.indexes.register({
  name: 'members-by-name',
  collection: '@example/members',
  unique: true,
  key: {
    type: 'string',
    map: 'mapNameToLowerCase',
  }
})

SpaceDB.toDisk(db)
```

Define helper functions in `helper.js`:

```js
// helper.js
exports.mapNameToLowerCase = (record, context) => {
  const name = record.name.toLowerCase().trim()
  return name ? [name] : []
}
```

Run `node build.js` to build the spec.

Then boot your db. You can use the same definition for a fully local db and a P2P one.

``` js
// run.mjs
import SpaceDB from 'spacedb'
import def from './spec/spacedb/index.js'

// first choose your engine
const db = SpaceDB.rocks('./my-rocks.db', def)

// Add some entries
await db.insert('@example/members', { name: 'maf', age: 37 })
await db.insert('@example/members', { name: 'sean', age: 36 })
await db.flush() // commit changes

// Query collection
const maf = await db.findOne('@example/members', { name: 'maf' })
console.log('maf', maf)
```

It is that simple.

## API

#### `db = Spacedb.bee(spacecore, definition, [options])`

Make a db backed by Spacebee. P2P!

Available `options`:

```
{
  autoUpdate: false, // Whether the database should update when the underlying spacebee does
  writable: true // Whether the database is writable
}
```

For Spacebee backed databases, the database does not update it's state when the bee updates. Setting `autoUpdate` to `true` makes this automatic. Otherwise the database can be manually updated with `db.update()`.

#### `db = Spacedb.rocks(path, definition, [options])`

Make a db backed by RocksDB. Local only!

Available `options`:

```
{
  readOnly: false, // Whether the database be read-only
  writable: true // Whether the database is writable
}
```

#### `queryStream = db.find(collectionOrIndex, query, [options])`

Query the database. `collectionOrIndex` is the identifier you defined in your builder.

The query looks like this

``` js
{
  gt: { ... },
  gte: { ... },
  lt: { ...},
  lte: { ...}
}
```

And options include

```js
{
  limit, // how many max?
  reverse: false // reverse stream?
}
```

See the basic tests for an easy example on how queries look like.

The `queryStream` is a streamx readable stream that yields the documents you search for.

A query is always running on a snapshot, meaning any inserts/deletes you do while this is running
will not impact the query stream itself.

#### `all = await queryStream.toArray()`

Stream helper to simply get all the remaining entries in the stream.

#### `one = await queryStream.one()`

Stream helper to simply get the last entry in the stream.

#### `doc = await db.findOne(collectionOrIndex, query, [options])`

Alias for `await find(...).one()`

#### `doc = await db.get(collection, query)`

Get a document from a collection that matches the `query`. The `query` object will be converted into a key to check against the `collection`.

#### `await db.insert(collection, doc)`

Insert a document into a collection. NOTE: you have to flush the db later for this to be persisted.

#### `await db.delete(collection, query)`

Delete a document from a collection matching the query. NOTE: you have to flush the db later for this to be persisted.

#### `bool = db.updated([collection], [query])`

Returns a boolean indicating if this database was updated. Pass a collection and doc query to know if
a specific record was updated.

#### `const stream = db.changes(range = {})`

Returns a stream of changes to the database. The `range` can have the
following properties:

```
{
  gte: seq, // Start with this seq (inclusive)
  gt: seq, // Start after this index
  lte: seq, // Stop after this index
  lt: seq, // Stop before this index
}
```

`seq` is the sequence number for the underlying Spacebee node.

Only supported with Spacebee engine.

#### `await db.flush()`

Flush all changes to the db

#### `db.watch(cb)`

Register a callback to trigger when database updates.

#### `db.unwatch(cb)`

Remove watch callback.

#### `db.reload()`

Reload the internal snapshot. Clears the memory state.

#### `db = db.snapshot()`

Make a readonly snapshot of the database. All reads/streams are locked in time on a snapshot from the time you call the snapshot method.

#### `db = db.transaction()`

Make a writable snapshot of the database. All reads/streams are locked in time on a snapshot from the time you call the snapshot method.
When you flush this one, it updates the main instance also.

#### `await db.ready()`

Wait for the database to fully open.

#### `await db.close()`

Close the database. You have to close any snapshots you use also.

#### `db.closed`

Whether the database has been closed.

#### `db.writable`

Whether the database can be written to.

#### `db.readable`

Whether the database can be read.

## Builder API

We can define what the schema of collections and the documents they contain using the builder API. A common pattern is to create a `build.js` script that is run whenever the database schema changes. See the [example](./builder/example/example.js).

The builder API can be imported via the `builder` subpath:

```js
const SpacedbBuilder = require('spacedb/builder')
```

#### `const db = SpacedbBuilder.from(SCHEMA_DIR, DB_DIR)`

Load a builder instance `db` with the `spaceschema` definitions (aka `SCHEMA_DIR`) and with existing `spacedb` definitions (aka `DB_DIR`).

`SCHEMA_DIR` is either the `spaceschema` definition object or the directory path to load from disk.

`DB_DIR` is either the existing database definition as a object or the directory path to load from disk.

#### `SpacedbBuilder.toDisk(db, DB_DIR, opts = { esm: false })`

Persist the builder instance `db` to the path passed as `DB_DIR`. If `DB_DIR` is falsy, the builder will use the `db.dbDir` path from `db`. This method is usually called at the end of the `build.js` script.

If the `esm` option is set to true, the code generated will be an ESM module.

#### `const namespacedDb = db.namespace(namespace)`

Create a namespaced db for defining multiple sets of database definitions on one database.

#### `db.require(path)`

Load helper functions to be used in the database as callbacks for collection triggers and index maps.

### Collections

#### `db.collections.register(description)`

Register collection with the `description`.

A `description` has the following form:

```
{
  name: 'collection-name',
  schema: '@schema-ns/struct-name', // identifier for the collections schema
  key?: ['keyField1', 'keyField2'], // the definition of the primary key to lookup collection entries
  derived?: false, // Whether it is derived collection and so shouldnt be versioned
  trigger?: 'triggerFunctionName' // A function loaded via `db.require()` to run when a collection document is updated
}
```

Elements in the `key` array should go from least to most specific, but often contain only one field that uniquely identifies the collection entry. Keys can use a 'dot notation' to specify nested properties of the collection entry's struct, for example:

```js
// Spaceschema
const dbSchema = schema.namespace('db')
dbSchema.register({
  name: 'foo',
  fields: [{ name: 'id', type: 'string', required: true }]
})

dbSchema.register({
  name: 'nested',
  fields: [
    { name: 'foo', type: '@db/foo', required: true }, // nested struct
    { name: 'bar', type: 'bool' }
  ]
})

// Spacedb Builder
const db = SpacedbBuilder.from(SCHEMA_DIR, DB_DIR)
db.collections.register({
  name: 'nested-foo',
  schema: '@db/nested',
  key: ['foo.id'] // uses `foo`'s `id` property as the key
})
```

`trigger` is a callback run when an entry is modified and can be used to update other entries in the database. The callback should be registered via `db.require(path)` and should have the following function signature:

```js
async function triggerCallback (db, query, record) {}
```

Trigger callback arguments:
- `db` is the `spacedb` instance.
- `query` is the query being used to update the database. In the case of `db.insert()` the `query` is the document being inserted.
- `record` is the document being inserted, if `null` the document matching `query` is being deleted.

A trigger that counts the number of members (a collection named `@example/member`) and stores them as the collection `@example/member-info` could be implemented like this:

```js
exports.triggerCount = async (db, query, record) => {
  const info = (await db.get('@example/member-info')) || { count: 0 }
  const existing = await db.get('@example/member', query)
  if (existing && record) return // The record is being updated

  await db.insert('@example/member-info', { count: record ? info.count + 1 : info.count - 1 })
}
```

`@example/member-info` should be a collection with `derived` set to `true` since it is derived from other collections.

### Indexes

#### `db.indexes.register(description)`

Register an index with the given `description`.

A `description` has the following form:

```
{
  name: 'index-name',
  collection: '@ns/collection', // The collection the index is a lookup into.
  unique: false, // whether index keys are unique, aka only return one document
  key: ['other-field'] || {
    type: 'string', // Key schema type
    map: 'indexMapFunctionName' // The function name (registered via db.require()) to map over the collection w/ to create the index
  }
}
```

If `unique` is `false`, the primary key of the document will be added to the index key to ensure entries do not clobber one another.

##### Key Mapping

Indexes support defining `key` as an array of fields just like Collections, but also support a mapping callback to derive keys from indexed entries. This callback has the following function signature:

```js
function keyMap (record, context): Array<any> {}
```

The return type of the mapping callback is always an array but the type of the elements in that array are defined as a `spaceschema` type with the `type` property like so:

```js
// helpers.js
exports.keyMap = (record, context) => [
  { name: record.name, age: record.age }
]

// schema.js
// ... Defining db, it's collections
db.register('./helpers.js')
db.indexes.register({
  name: 'mapped-index',
  collection: '@ns/collection',
  key: {
    type: { // a struct
      fields: [
        {
          name: 'name',
          type: 'string'
        },
        {
          name: 'age',
          type: 'uint'
        }
      ]
    },
    map: 'keyMap' // the callback name from helpers.js
  }
})
```

## License

Apache-2.0
