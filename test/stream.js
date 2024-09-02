const test = require('brittle')
const b4a = require('b4a')
const IndexStream = require('../lib/stream')
const { Readable } = require('streamx')

test('basic stream', async function (t) {
  const rs = from([
    { key: b4a.from('a'), value: 'stream' },
    { key: b4a.from('c'), value: 'stream' },
    { key: b4a.from('d'), value: 'stream' },
    { key: b4a.from('f'), value: 'stream' }
  ])

  const overlay = [
    { key: b4a.from('b'), value: 'overlay' },
    { key: b4a.from('f'), value: 'overlay' }
  ]

  const stream = new IndexStream(rs, { overlay })
  const datas = await collect(stream)
  const expected = [
    { key: b4a.from('a'), value: 'stream' },
    { key: b4a.from('b'), value: 'overlay' },
    { key: b4a.from('c'), value: 'stream' },
    { key: b4a.from('d'), value: 'stream' },
    { key: b4a.from('f'), value: 'overlay' }
  ]

  t.alike(datas, expected)
})

test('basic stream with map', async function (t) {
  const rs = from([
    { key: b4a.from('a'), value: 'stream' },
    { key: b4a.from('c'), value: 'stream' },
    { key: b4a.from('d'), value: 'stream' },
    { key: b4a.from('f'), value: 'stream' }
  ])

  const overlay = [
    { key: b4a.from('b'), value: 'overlay' },
    { key: b4a.from('f'), value: 'overlay' }
  ]

  const stream = new IndexStream(rs, { overlay, map })
  const datas = await collect(stream)
  const expected = [
    { key: b4a.from('a'), value: 'mapped-stream' },
    { key: b4a.from('b'), value: 'overlay' },
    { key: b4a.from('c'), value: 'mapped-stream' },
    { key: b4a.from('d'), value: 'mapped-stream' },
    { key: b4a.from('f'), value: 'overlay' }
  ]

  t.alike(datas, expected)

  function map (entries) {
    return entries.map(e => Promise.resolve({ key: e.key, value: 'mapped-' + e.value }))
  }
})

test('basic stream with map and limit', async function (t) {
  const rs = from([
    { key: b4a.from('a'), value: 'stream' },
    { key: b4a.from('c'), value: 'stream' },
    { key: b4a.from('d'), value: 'stream' },
    { key: b4a.from('f'), value: 'stream' }
  ])

  const overlay = [
    { key: b4a.from('b'), value: 'overlay' },
    { key: b4a.from('f'), value: 'overlay' }
  ]

  const stream = new IndexStream(rs, { overlay, limit: 3, map })
  const datas = await collect(stream)
  const expected = [
    { key: b4a.from('a'), value: 'mapped-stream' },
    { key: b4a.from('b'), value: 'overlay' },
    { key: b4a.from('c'), value: 'mapped-stream' }
  ]

  t.alike(datas, expected)

  function map (entries) {
    return entries.map(e => Promise.resolve({ key: e.key, value: 'mapped-' + e.value }))
  }
})

test('basic stream with map in asap mode', async function (t) {
  const rs = from([
    { key: b4a.from('a'), value: 'stream' },
    { key: b4a.from('c'), value: 'stream' },
    { key: b4a.from('d'), value: 'stream' },
    { key: b4a.from('f'), value: 'stream' }
  ])

  const overlay = [
    { key: b4a.from('b'), value: 'overlay' },
    { key: b4a.from('f'), value: 'overlay' }
  ]

  const stream = new IndexStream(rs, { overlay, asap: true, map })
  const datas = await collect(stream)
  const expected = [
    { key: b4a.from('a'), value: 'mapped-stream' },
    { key: b4a.from('b'), value: 'overlay' },
    { key: b4a.from('c'), value: 'mapped-stream' },
    { key: b4a.from('d'), value: 'mapped-stream' },
    { key: b4a.from('f'), value: 'overlay' }
  ]

  t.alike(datas, expected)

  function map (entries) {
    return entries.map(e => Promise.resolve({ key: e.key, value: 'mapped-' + e.value }))
  }
})

function from (datas) {
  const rs = new Readable()
  for (const data of datas) rs.push(data)
  rs.push(null)
  return rs
}

async function collect (stream) {
  const all = []
  for await (const data of stream) all.push(data)
  return all
}
