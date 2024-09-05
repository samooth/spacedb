const RocksDB = require('rocksdb-native')

module.exports = class RocksEngine {
  constructor (storage) {
    this.asap = false
    this.clock = 0
    this.db = typeof storage === 'object' ? storage : new RocksDB(storage)
  }

  ready () {
    return this.db.ready()
  }

  close () {
    return this.db.close()
  }

  getRange (entries) {
    const read = this.db.read()
    const promises = new Array(entries.length)

    for (let i = 0; i < promises.length; i++) {
      promises[i] = getWrapped(read, entries[i].key, entries[i].value)
    }

    read.tryFlush()
    return promises
  }

  get (key) {
    return this.db.get(key)
  }

  createReadStream (range, options) {
    return this.db.iterator({ ...range, ...options })
  }

  async commit (updates) {
    this.clock++

    const write = this.db.write()

    for (const u of updates.values()) {
      if (u.value !== null) write.put(u.key, u.value)
      else write.delete(u.key)

      for (const ups of u.indexes) {
        for (const { key, del } of ups) {
          if (del === true) write.delete(key)
          else write.put(key, u.key)
        }
      }
    }

    await write.flush()
    updates.clear()
  }
}

async function getWrapped (read, key, value) {
  return { key, value: [value, await read.get(value)] }
}
