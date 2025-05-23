// This file is autogenerated by the spaceschema compiler
// Schema Version: 1
/* eslint-disable camelcase */
/* eslint-disable quotes */

const VERSION = 1
const { c } = require('spaceschema/runtime')

// eslint-disable-next-line no-unused-vars
let version = VERSION

// @db/member
const encoding0 = {
  preencode (state, m) {
    c.string.preencode(state, m.id)
    c.uint.preencode(state, m.age)
  },
  encode (state, m) {
    c.string.encode(state, m.id)
    c.uint.encode(state, m.age)
  },
  decode (state) {
    const r0 = c.string.decode(state)
    const r1 = c.uint.decode(state)

    return {
      id: r0,
      age: r1
    }
  }
}

// @db/nested.member
const encoding1_0 = c.frame(encoding0)

// @db/nested
const encoding1 = {
  preencode (state, m) {
    encoding1_0.preencode(state, m.member)
    state.end++ // max flag is 1 so always one byte
  },
  encode (state, m) {
    const flags = m.fun ? 1 : 0

    encoding1_0.encode(state, m.member)
    c.uint.encode(state, flags)
  },
  decode (state) {
    const r0 = encoding1_0.decode(state)
    const flags = c.uint.decode(state)

    return {
      member: r0,
      fun: (flags & 1) !== 0
    }
  }
}

// @db/member/spacedb#0
const encoding2 = {
  preencode (state, m) {
    c.uint.preencode(state, m.age)
  },
  encode (state, m) {
    c.uint.encode(state, m.age)
  },
  decode (state) {
    const r1 = c.uint.decode(state)

    return {
      id: null,
      age: r1
    }
  }
}

// @db/nested/spacedb#0.member
const encoding3_0 = encoding1_0

// @db/nested/spacedb#0
const encoding3 = {
  preencode (state, m) {
    encoding3_0.preencode(state, m.member)
    state.end++ // max flag is 1 so always one byte
  },
  encode (state, m) {
    const flags = m.fun ? 1 : 0

    encoding3_0.encode(state, m.member)
    c.uint.encode(state, flags)
  },
  decode (state) {
    const r0 = encoding3_0.decode(state)
    const flags = c.uint.decode(state)

    return {
      member: r0,
      fun: (flags & 1) !== 0
    }
  }
}

function setVersion (v) {
  version = v
}

function encode (name, value, v = VERSION) {
  version = v
  return c.encode(getEncoding(name), value)
}

function decode (name, buffer, v = VERSION) {
  version = v
  return c.decode(getEncoding(name), buffer)
}

function getEnum (name) {
  switch (name) {
    default: throw new Error('Enum not found ' + name)
  }
}

function getEncoding (name) {
  switch (name) {
    case '@db/member': return encoding0
    case '@db/nested': return encoding1
    case '@db/member/spacedb#0': return encoding2
    case '@db/nested/spacedb#0': return encoding3
    default: throw new Error('Encoder not found ' + name)
  }
}

function getStruct (name, v = VERSION) {
  const enc = getEncoding(name)
  return {
    preencode (state, m) {
      version = v
      enc.preencode(state, m)
    },
    encode (state, m) {
      version = v
      enc.encode(state, m)
    },
    decode (state) {
      version = v
      return enc.decode(state)
    }
  }
}

const resolveStruct = getStruct // compat

module.exports = { resolveStruct, getStruct, getEnum, getEncoding, encode, decode, setVersion, version }
