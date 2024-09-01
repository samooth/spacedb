const { Readable, isEnded } = require('streamx')
const b4a = require('b4a')

module.exports = class IndexStream extends Readable {
  constructor (stream, overlay, map) {
    super()

    this.stream = stream
    this.overlay = overlay
    this.overlayIndex = 0
    this.map = map || null
  }

  _open (cb) {
    const destroy = this.destroy.bind(this)
    const drain = this._drain.bind(this)

    this.stream.on('readable', drain)
    this.stream.on('end', drain)
    this.stream.on('error', destroy)
    this.stream.on('close', destroy)

    cb(null)
  }

  _pushNext (data) {
    if (data === null) {
      while (this.overlayIndex < this.overlay.length) {
        this.push(this.overlay[this.overlayIndex++])
      }

      this.push(null)
      return
    }

    while (true) {
      if (this.overlayIndex >= this.overlay.length) {
        this.push(data)
        return
      }

      const cmp = b4a.compare(data.key, this.overlay[this.overlayIndex].key)

      if (cmp < 0) {
        this.push(data)
        return
      }

      this.push(this.overlay[this.overlayIndex++])
      if (cmp === 0) return
    }
  }

  _process (batch) {
    for (let i = 0; i < batch.length; i++) this._pushNext(batch[i])
    if (isEnded(this.stream)) this._pushNext(null)
  }

  _drain () {
    if (Readable.isBackpressured(this)) return
    this._process(fullyDrain(this.stream))
  }

  _predestroy () {
    this.stream.destroy()
  }

  _destroy (cb) {
    this.stream.destroy()
    cb(null)
  }

  _read (cb) {
    this._drain()
    cb(null)
  }
}

function fullyDrain (stream) {
  const batch = []

  while (true) {
    const data = stream.read()
    if (data === null) return batch
    batch.push(data)
  }
}
