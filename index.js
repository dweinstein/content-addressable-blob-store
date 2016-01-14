var s3blobs = require('s3-blob-store')
var path = require('path')
var crypto = require('crypto')
var stream = require('readable-stream')
var util = require('util')
var eos = require('end-of-stream')
var os = require('os')
var thunky = require('thunky')

var noop = function() {}

var SIGNAL_FLUSH = new Buffer([0])

var toPath = function(base, hash) {
  return hash ? path.join(base, hash.slice(0, 2), hash.slice(2)) : base
}

var Writer = function(backing, prefix, algo, init) {
  this.key = null
  this.size = 0
  this.destroyed = false

  this._tmp = null
  this._ws = null
  this._prefix = prefix
  this._digest = crypto.createHash(algo)
  this._init = init
  this._backing = backing

  stream.Writable.call(this)
}

util.inherits(Writer, stream.Writable)

Writer.prototype._flush = function(cb) {
  var self = this
  var hash = this.key = this._digest.digest('hex')

  self._ws.end(function() {
    setTimeout(function() {
      self._backing.move({ key: self._tmp }, { key: toPath(self._prefix, hash) }, cb)
    }, 1000 /* give s3 time to settle before we waitFor loop the object */)
  })
}

Writer.prototype._setup = function(data, enc, cb) {
  var self = this
  var destroy = function(err) {
    self.destroy(err)
  }

  this._init(function(dir) {
    if (self.destroyed) return cb(new Error('stream destroyed'))
    self._tmp = path.join(dir, Date.now()+'-'+Math.random().toString().slice(2))
    self._ws = self._backing.createWriteStream({key: self._tmp})
    self._ws.on('error', destroy)
    self._ws.on('close', destroy)
    self._write(data, enc, cb)
  })
}

Writer.prototype.destroy = function(err) {
  if (this.destroyed) return
  this.destroyed = true
  if (this._ws) this._ws.destroy()
  if (err) this.emit('error', err)
  this.emit('close')
}

Writer.prototype._write = function(data, enc, cb) {
  if (!this._tmp) return this._setup(data, enc, cb)
  if (data === SIGNAL_FLUSH) return this._flush(cb)
  this.size += data.length
  this._digest.update(data)
  this._ws.write(data, enc, cb)
}

Writer.prototype.end = function(data, enc, cb) {
  if (typeof data === 'function') return this.end(null, null, data)
  if (typeof enc === 'function') return this.end(data, null, enc)
  if (data) this.write(data)
  this.write(SIGNAL_FLUSH)
  stream.Writable.prototype.end.call(this, cb)
}

module.exports = function(opts) {
  if (typeof opts === 'string') opts = {path: opts}
  if (!opts) opts = {}

  var algo = opts.algo
  if (!algo) algo = 'sha256'

  var bucket = opts.bucket
  var s3 = opts.s3
  var prefix = opts.prefix || 'blobs'

  var backing = s3blobs({
    client: s3,
    bucket: bucket
  })

  var that = {}

  var init = thunky(function(cb) {
    cb(path.join(prefix, 'tmp'))
  })

  that.createWriteStream = function(opts, cb) {
    if (typeof opts === 'string') opts = {key:opts}
    if (typeof opts === 'function') return that.createWriteStream(null, opts)

    var ws = new Writer(backing, prefix, algo, init)
    if (!cb) return ws

    eos(ws, function(err) {
      if (err) return cb(err)
      cb(null, {
        key: ws.key,
        size: ws.size
      })
    })

    return ws
  }

  that.createReadStream = function(opts) {
    if (typeof opts === 'string') opts = {key:opts}
    return backing.createReadStream({key: toPath(prefix, opts.key || opts.hash)}, opts)
  }

  that.exists = function(opts, cb) {
    if (typeof opts === 'string') opts = {key:opts}
    return backing.exists(opts, cb)
  }

  that.remove = function(opts, cb) {
    if (!cb) cb = noop
    if (typeof opts === 'string') opts = {key:opts}
    return backing.remove(opts, cb)
  }

  return that
}
