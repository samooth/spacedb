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
  }

  static rocksdb (storage, definition) {
    return new HyperDB(new RocksEngine(storage), definition)
  }

  _getIndex (name) {
    let i = 0
    for (const idx of this.definition.indexes) {
      if (idx.name === name) return [i, idx, this.definition]
      i++
    }

    throw new Error('Unknown index')
  }

  query (indexName, q = {}) {
    const [i, index, def] = this._getIndex(indexName)
    const range = index.encodeRange(q)

    const engine = this.engine
    const overlay = []

    for (const u of this.updates.values()) {
      for (const { key, del } of u.indexes[i]) {
        if (withinRange(range, key)) overlay.push({ key, value: del ? null : u.value })
      }
    }

    overlay.sort(q.reverse ? reverseSortOverlay : sortOverlay)

    // quick hack before backing engine is there
    const stream = engine.createReadStream(range, {})

    return new IndexStream(stream, { decode, overlay, map })

    function decode (entry) {
      return c.decode(def.valueEncoding, entry.value)
    }

    function map (entries) {
      return engine.getRange(entries)
    }
  }

  async queryOne (indexName, q = {}) {
    const stream = this.query(indexName, { ...q, limit: 1 })

    let result = null
    for await (const data of stream) result = data
    return result
  }

  async insert (collectionName, doc) {
    // TODO proper one for a collection etc
    const docDefinition = this.definition
    const key = c.encode(docDefinition.keyEncoding, doc)

    const prevBuffer = await this.engine.get(key)
    const prev = prevBuffer === null ? null : c.decode(docDefinition.valueEncoding, prevBuffer)

    const u = {
      key,
      value: c.encode(docDefinition.valueEncoding, doc),
      indexes: []
    }

    for (let i = 0; i < docDefinition.indexes.length; i++) {
      const d = docDefinition.indexes[i]
      const prevKey = prev && d.encode(prev)
      const nextKey = d.encode(doc)

      if (prevKey !== null && b4a.equals(nextKey, prevKey)) continue

      const ups = []
      u.indexes.push(ups)

      if (prevKey !== null) ups.push({ key: prevKey, del: true })
      if (nextKey !== null) ups.push({ key: nextKey, del: false })
    }

    this.updates.set(b4a.toString(u.key, 'hex'), u)
  }

  async flush () {
    await this.engine.commit(this.updates)
    this.updates.clear()
  }
}

function withinRange (range, key) {
  if (range.gte !== null && b4a.compare(range.gte, key) > 0) return false
  if (range.gt !== null && b4a.compare(range.gt, key) >= 0) return false
  if (range.lte !== null && b4a.compare(range.lte, key) < 0) return false
  if (range.lt !== null && b4a.compare(range.lt, key) <= 0) return false
  return true
}

function sortOverlay (a, b) {
  return b4a.compare(a.key, b.key)
}

function reverseSortOverlay (a, b) {
  return b4a.compare(b.key, a.key)
}

module.exports = HyperDB
