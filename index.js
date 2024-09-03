const IndexStream = require('./lib/stream')
const c = require('compact-encoding')
const b4a = require('b4a')

// engines
const RocksEngine = require('./lib/engine/rocks')

class HyperDB {
  constructor (engine, definition) {
    this.engine = engine
    this.definition = definition
    this.updates = new Map()
    this.clocked = 0
  }

  static rocksdb (storage, definition) {
    return new HyperDB(new RocksEngine(storage), definition)
  }

  query (indexName, q = {}, options) {
    if (options) q = { ...q, ...options }

    const index = this.definition.resolveIndex(indexName)
    const collection = index === null ? this.definition.resolveCollection(indexName) : index.collection
    const limit = q.limit
    const reverse = !!q.reverse

    const range = index === null ? collection.encodeKeyRange(q) : index.encodeKeyRange(q)
    const engine = this.engine
    const overlay = []

    if (index === null) {
      for (const u of this.updates.values()) {
        if (withinRange(range, u.key)) overlay.push({ key: u.key, value: u.value })
      }
    } else {
      for (const u of this.updates.values()) {
        for (const { key, del } of u.indexes[index.offset]) {
          if (withinRange(range, key)) overlay.push({ key, value: del ? null : u.value })
        }
      }
    }

    overlay.sort(reverse ? reverseSortOverlay : sortOverlay)

    const stream = engine.createReadStream(range, { reverse, limit })

    return new IndexStream(stream, { reverse, limit, restructure: collection.restructure, overlay, map: index === null ? null : map })

    function map (entries) {
      return engine.getRange(entries)
    }
  }

  async queryOne (indexName, q, options) {
    const stream = this.query(indexName, q, { ...options, limit: 1 })

    let result = null
    for await (const data of stream) result = data
    return result
  }

  async get (collectionName, doc) {
    const collection = this.definition.resolveCollection(collectionName)
    if (collection === null) return null

    const key = collection.encodeKey(doc)
    const value = await this._getLatestValue(key)
    return value === null ? null : collection.restructure(key, value)
  }

  _getLatestValue (key) {
    const hex = b4a.toString(key, 'hex')
    const u = this.updates.get(hex)
    if (u) return u.value
    return this.engine.get(key)
  }

  async insert (collectionName, doc) {
    const collection = this.definition.resolveCollection(collectionName)
    if (collection === null) throw new Error('Unknown collection')

    const key = collection.encodeKey(doc)

    const prevValue = await this.engine.get(key)
    const prevDoc = prevValue === null ? null : collection.restructure(key, prevValue)

    const u = {
      key,
      value: collection.encodeValue(doc),
      indexes: []
    }

    for (let i = 0; i < collection.indexes.length; i++) {
      const idx = collection.indexes[i]
      const prevKey = prevDoc && idx.encodeKey(prevDoc)
      const nextKey = idx.encodeKey(doc)

      if (prevKey !== null && b4a.equals(nextKey, prevKey)) continue

      const ups = []
      u.indexes.push(ups)

      if (prevKey !== null) ups.push({ key: prevKey, del: true })
      if (nextKey !== null) ups.push({ key: nextKey, del: false })
    }

    this.updates.set(b4a.toString(u.key, 'hex'), u)
  }

  async flush () {
    if (this.clocked !== this.engine.clock) throw new Error('Database has changed, refusing to commit')
    await this.engine.commit(this.updates)
    this.clocked = this.engine.clock
  }
}

function withinRange (range, key) {
  if (range.gte && b4a.compare(range.gte, key) > 0) return false
  if (range.gt && b4a.compare(range.gt, key) >= 0) return false
  if (range.lte && b4a.compare(range.lte, key) < 0) return false
  if (range.lt && b4a.compare(range.lt, key) <= 0) return false
  return true
}

function sortOverlay (a, b) {
  return b4a.compare(a.key, b.key)
}

function reverseSortOverlay (a, b) {
  return b4a.compare(b.key, a.key)
}

module.exports = HyperDB
