const Hyperschema = require('hyperschema')

const generateCode = require('./lib/codegen')

const COLLECTION_TYPE = 1
const INDEX_TYPE = 2

class DBType {
  constructor (builder, namespace, description) {
    this.builder = builder
    this.namespace = namespace
    this.description = description
    this.fqn = getFQN(this.namespace, this.description.name)
    this.key = description.key

    this.isIndex = false
    this.isCollection = false

    const { prefix, id } = this.builder._assignId(this)
    this.prefix = prefix
    this.id = id

    this.version = 1
    this.previous = null
    if (this.builder.previous) {
      this.previous = this.builder.previous.typesById.get(this.id)
    }
  }

  _resolveKey (schema, path) {
    const components = path.split('.')

    let current = schema
    for (let i = 0; i < components.length; i++) {
      const field = current.fieldsByName.get(components[i])
      if (!field) throw new Error('Could not resolve path: ' + path)
      current = field.type
    }

    const resolved = this.builder.schema.resolve(current.fqn, { aliases: false })
    if (!resolved) throw new Error('Could not resolve path: ' + path)

    return resolved
  }

  toJSON () {
    return {
      name: this.description.name,
      unsafe: this.description.unsafe,
      namespace: this.namespace,
      id: this.id
    }
  }
}

class Collection extends DBType {
  constructor (builder, namespace, description) {
    super(builder, namespace, description)
    this.isCollection = true
    this.indexes = []

    this.schema = this.builder.schema.resolve(description.schema)
    if (!this.schema) throw new Error('Schema not found: ' + description.schema)

    this.key = description.key
    this.keyEncoding = []
    this.valueEncoding = this.fqn + '/value'
    for (const component of this.key) {
      const field = this.schema.fieldsByName.get(component)
      const resolvedType = this.builder.schema.resolve(field.type.fqn, { aliases: false })
      this.keyEncoding.push(resolvedType.name)
    }

    // Register a value encoding type (the portion of the record that will not be in the primary key)
    const primaryKeySet = new Set(this.key)
    this.builder.schema.register({
      ...this.schema.toJSON(),
      flagsPosition: -1,
      namespace: this.namespace,
      name: this.description.name + '/value',
      fields: this.schema.fields.filter(f => !primaryKeySet.has(f.name)).map(f => f.toJSON())
    })
  }

  toJSON () {
    return {
      ...super.toJSON(),
      type: COLLECTION_TYPE,
      indexes: this.indexes.map(i => i.fqn),
      schema: this.schema.fqn,
      key: this.key
    }
  }
}

class Index extends DBType {
  constructor (builder, namespace, description) {
    super(builder, namespace, description)
    this.isIndex = true
    this.key = description.key
    this.unique = !!description.unique
    this.collection = this.builder.typesByName.get(description.collection)

    if (!this.collection || !this.collection.isCollection) {
      throw new Error('Invalid index target: ' + description.collection)
    }
    this.collection.indexes.push(this)

    this.keyEncoding = []
    this.valueEncoding = this.collection.fqn + '/key'

    // Key encoding will be an IndexEncoder of the secondary index's key fields
    // If the key is not unique, then the primary key should also be included
    for (const component of this.key) {
      const resolvedType = this._resolveKey(this.collection.schema, component)
      this.keyEncoding.push(resolvedType.name)
    }
    if (!this.unique) {
      for (const component of this.collection.keyEncoding) {
        this.keyEncoding.push(component)
      }
    }

    // Value encoding will be the collection's primary key value encoding if unique
    // If non-unique, the value encoding will be empty
    this.valueEncoding = this.unique ? this.collection.fqn + '/key' : null
  }

  toJSON () {
    return {
      ...super.toJSON(),
      type: INDEX_TYPE,
      collection: this.collection,
      unique: this.unique,
      key: this.key
    }
  }
}

class BuilderCollections {
  constructor (namespace) {
    this.builder = namespace.builder
    this.namespace = namespace
  }

  register (description) {
    this.builder.registerCollection(description, this.namespace.name)
  }
}

class BuilderIndexes {
  constructor (namespace) {
    this.builder = namespace.builder
    this.namespace = namespace
  }

  register (description) {
    this.builder.registerIndex(description, this.namespace.name)
  }
}

class BuilderNamespace {
  constructor (builder, name, { prefix = [] } = {}) {
    this.builder = builder
    this.name = name
    this.prefix = prefix

    this.collections = new BuilderCollections(this)
    this.indexes = new BuilderIndexes(this)
    this.schema = this.builder.schema.namespace(this.name)

    this.descriptions = []
  }

  toJSON () {
    return {
      name: this.name,
      prefix: this.prefix
    }
  }
}

class Builder {
  constructor ({ version = 1, offset = 1, previous = null } = {}) {
    this.namespaces = new Map()
    this.typesByName = new Map()
    this.typesById = new Map()
    this.orderedTypes = []
    this.offset = offset

    this.previous = previous
    this.version = previous ? previous.version + 1 : version

    this.schema = new Hyperschema()
    this.pathsByName = new Map()
    this.pathMap = new Map()
    this.currentOffset = this.offset
  }

  _assignId (type) {
    const unsafe = type.description.unsafe
    if (unsafe) {
      if (unsafe.prefix && !Number.isInteger(unsafe.id)) {
        throw new Error('If a type overrides a prefix, it must also specifiy an ID')
      }
      if (unsafe.prefix) return { id: unsafe.id, prefix: unsafe.prefix }
    }
    return { id: this.currentOffset++, prefix: null }
  }

  registerCollection (description, namespace) {
    const collection = new Collection(this, namespace, description)
    this.orderedTypes.push(collection)
    this.typesByName.set(collection.fqn, collection)
  }

  registerIndex (description, namespace) {
    const index = new Index(this, namespace, description)
    this.orderedTypes.push(index)
    this.typesByName.set(index.fqn, index)
  }

  namespace (name, opts) {
    if (this.namespaces.has(name)) throw new Error('Namespace already exists: ' + name)
    const ns = new BuilderNamespace(this, name, opts)
    this.namespaces.set(name, ns)
    return ns
  }

  compile () {
    if (this.previous && !this.changed) {
      this.version -= 1
    }
    return {
      messages: this.schema.toCode({ runtime: 'hyperdb/runtime' }),
      db: generateCode(this),
      changed: this.changed,
      json: this.toJSON()
    }
  }

  toJSON () {
    return {
      version: this.version,
      offset: this.offset,
      types: this.schema.toJSON(),
      db: {
        namespaces: [...this.namespaces.values()].map(ns => ns.toJSON()),
        schema: [...this.typesById.values()].map(type => type.toJSON())
      }
    }
  }

  static fromJSON (json, opts) {
    const builder = new this({ ...opts, version: json.version, unsafe: json.unsafe })
    for (const type of json.types.schema) {
      builder.schema.register(type)
    }
    for (const description of json.db.namespaces) {
      builder.namespace(description.name, description)
    }
    for (const typeDescription of json.db.schema) {
      if (typeDescription.type === INDEX_TYPE) {
        builder.registerIndex(typeDescription, typeDescription.namespace)
      } else if (typeDescription.type === COLLECTION_TYPE) {
        builder.registerCollection(typeDescription, typeDescription.namespace)
      } else {
        throw new Error('Unsupported type: ' + typeDescription.type)
      }
    }
    return builder
  }
}

module.exports = Builder

function getFQN (namespace, name) {
  return '@' + namespace + '/' + name
}
