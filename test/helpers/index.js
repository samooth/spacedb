const tmp = require('test-tmp')
const path = require('path')
const Hyperschema = require('hyperschema')
const Builder = require('../../builder')
const HyperDB = require('../../')

exports.rocks = async function (t, def, opts) {
  if (!HyperDB.isDefinition(def)) return exports.rocks(t, require(`../fixtures/generated/${(def && def.fixture) || 1}/hyperdb`), def)

  const db = HyperDB.rocks(await tmp(t), def, opts)
  const engine = db.engine

  // just to help catch leaks
  t.teardown(function () {
    if (!engine.closed) throw new Error('Test has a leak, engine did not close')
  })

  return db
}

exports.build = async function (t, builder) {
  const dir = await tmp(t, { dir: path.join(__dirname, '../fixtures/tmp') })
  await builder(Builder, Hyperschema, { db: path.join(dir, 'hyperdb'), schema: path.join(dir, 'hyperschema') })
  return HyperDB.rocks(path.join(dir, 'db'), require(path.join(dir, 'hyperdb')))
}
