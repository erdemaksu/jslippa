const assert = require('assert')
const debug = require('debug')('client')
const Transport = require('./transport.js')

const protobuf = require('protobufjs')

const root = protobuf.loadSync('./doc/struct.proto')

function type(typeName) {
  return root.lookup(typeName)
}

class Client {
  constructor(opts) {
    assert(opts.username)
    assert(opts.password)
    assert(opts.host)
    assert(opts.port)

    this.transport = new Transport(opts)
    this.cid = 0
    this.messageMap = new Map()
    this.partial = null
    this.conversionConf = {
      enums: String,  // enums as string names
      //longs: String,  // longs as strings (requires long.js)
      bytes: String,  // bytes as base64 encoded strings
      defaults: false, // includes default values
      arrays: false,   // populates empty arrays (repeated fields) even if defaults=false
      objects: false,  // populates empty objects (map fields) even if defaults=false
      oneofs: false    // includes virtual oneof fields set to the present field's name
    }
  }

  connect() {
    return this.transport.connect(this._handleIncomingMessage.bind(this))
    .then(() => debug('Connection is successful!'))
  }

  disconnect() {
    return this.transport.disconnect()
  }

  send(obj) {
    let struct = this._makeStruct(obj)
    if ( type('Struct').verify(struct) ) {
      debug('Verification failed for', struct)
      throw new Error('Verification error')
    }
    return this._send(struct)
  }

  //TODO if response not ok, fail the promise!
  _send(_struct) {
    debug('Encoding before sending %j', _struct)
    const data = type('Struct').encode(_struct).finish()
    const cId = this._get_cid()

    debug('Decode back before sending %j', type('Struct').decode(data))

    const header = Buffer.alloc(6)
    header.writeUInt32BE(data.length + 2, 0)
    header.writeUInt16BE(cId, 4)
    debug('Header', header, data.length)

    const buf = Buffer.concat([header, data])

    debug('Sending buffer', buf)
    return this.transport.send(buf).then(() => this._waitForMessageWithId(cId))
  }

  _get_cid() {
    let cid = this.cid
    if (cid == 65535)
      this.cid = 0
    else
      this.cid = cid + 1
    return cid
  }

  _handleIncomingMessage(message) {
    debug('Incoming Buffer', message)
    this._relay(message)
  }

  _waitForMessageWithId(cId, timeout=1000) {
    return new Promise((resolve, reject) => {
      this.messageMap.set(cId, data => {
        const msg = type('Struct').decode(data)
        debug('Incoming message', msg)
        if (msg) {
          let res = this._stripStruct(msg)
          resolve(res)
        } else {
          reject(msg)
        }
      })
      setTimeout(() => {
        this.messageMap.delete(cId)
        reject(new Error(`Timeout waiting for message with id ${cId}`))
      }, timeout)
    })
  }

  _relay(buf_) {
    const buf = this._concat_partial_buffers(buf_)
    if (buf.length < 6) {
      debug('Invalid incoming buffer', buf)
      this.partial = buf
      return
    }

    let len = buf.readUInt32BE(0) - 2
    if (len < 0) {
      debug('Invalid incoming buf len', len)
      return
    }

    if (buf.length < len+6) {
      debug('Received buffer is shorter than encoded length, %d < %d',
            buf.length, len+6)
      this.partial = buf
      return
    }

    const cId = buf.readUInt16BE(4)
    const data = buf.slice(6, len+6)

    const waitCb = this.messageMap.get(cId)
    if (waitCb) {
      waitCb(data)
    } else {
      debug('Discarding incoming message with cId', cId)
    }
    const rest = buf.slice(len+6)
    debug('Rest of the received buffer is %d bytes', rest.length)
    if (rest.length > 0) {
      this._relay(rest)
    }
  }

  _concat_partial_buffers(buf) {
    if (this.partial == null){
      return buf
    } else {
      return Buffer.concat([this.partial, buf])
    }
  }

  _makeStruct(obj) {
    let fields = Object.entries(obj).reduce((acc, kv) => {
      let value = this._makeValue(kv[1])
      acc[kv[0]] = value
      return acc
    }, {})
    return type('Struct').create({fields: fields})
  }

  _makeValue(val) {
    let type = this._toType(val)
    //debug('type: ', type)
    if(type == 'string')
      return {stringValue: val}
    else if (type == 'number')
      return {numberValue: val}
    else if (type == 'boolean')
      return {boolValue: val}
    else if (type == 'object')
      return {structValue: this._makeStruct(val)}
    else if (type == 'array')
      return {listValue: {values: val.map(e => {return this._makeValue(e)})}}
    else if (type == 'null')
      return {nullValue: 0}
  }

  _stripStruct(msg) {
    let obj = type('Struct').toObject(msg, this.conversionConf)
    let res = Object.entries(obj.fields).reduce((acc, kv) => {
      let value = this._stripValue(kv[1])
      acc[kv[0]] = value
      return acc
    }, {})
    return res
  }

  _stripValue(_val) {
    let value = Object.values(_val)[0]
    if(this._toType(value) == 'object') {
      if (value.fields) {
        return this._stripStruct(value)
      } else if (value.values) {
        return value.values.map(e => { return this._stripValue(e) })
      }
    } else if (value == 'NULL_VALUE') {
      return null
    } else {
      return value
    }
  }

  _toType(obj) {
    return ({}).toString.call(obj).match(/\s([a-zA-Z]+)/)[1].toLowerCase()
  }
}

module.exports = Client

if (require.main === module) {
  const client = new Client({
    //host: '127.0.0.1',
    host: '192.168.211.187',
    port: '8089',
    username: 'admin',
    password: 'admin'
  })

  const co = require('co')
  co(function*() {
    yield client.connect()
    let sample = {
      phrase: 'mota olle i grinden',
      transactionId: 1,
      someDouble: 45.99,
      flag: true,
      someList: [1,2,3],
      anotherList: [{first: 'first'}, {second: true}, {list: [4,5,6]}],
      anUndefinedValue: null,
      nestedValue: {
        deepPhrase: 'it works!',
        deepList1: [{myNumber: 4654}, {myDouble: 987321.321}],
        deepList2: [3,2,1]
      }
    }
    yield client.send( sample )
      .then(r => {
        debug('received %O', r)
        assert.deepEqual(r, sample, ['Data send does not match the data received.'])
        return r
      })
      .catch(err => {
        debug('error', err)
        return err
      })
    client.disconnect()
  })
}
