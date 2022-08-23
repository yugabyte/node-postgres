'use strict'

var EventEmitter = require('events').EventEmitter
var util = require('util')
var utils = require('./utils')
var sasl = require('./sasl')
var pgPass = require('pgpass')
var TypeOverrides = require('./type-overrides')

var ConnectionParameters = require('./connection-parameters')
var Query = require('./query')
var defaults = require('./defaults')
var Connection = require('./connection')
const dns = require('dns')
const YB_SERVERS_QUERY = 'SELECT * FROM yb_servers()'
class ServerInfo {
  constructor(hostName, port, placementInfo, public_ip) {
    this.hostName = hostName
    this.port = port
    this.placementInfo = placementInfo
    this.public_ip = public_ip
  }
}

class Lock {
  constructor() {
    this._locked = false
    this._ee = new EventEmitter().setMaxListeners(0)
  }

  acquire() {
    return new Promise((resolve) => {
      if (!this._locked) {
        this._locked = true
        return resolve()
      }
      const tryAcquire = () => {
        if (!this._locked) {
          this._locked = true
          this._ee.removeListener('release', tryAcquire)
          return resolve()
        }
      }
      this._ee.on('release', tryAcquire)
    })
  }

  release() {
    this._locked = false
    setImmediate(() => this._ee.emit('release'))
  }
}

const lock = new Lock()
class Client extends EventEmitter {
  constructor(config) {
    super()
    this.connectionParameters = new ConnectionParameters(config)
    this.user = this.connectionParameters.user
    this.database = this.connectionParameters.database
    this.port = this.connectionParameters.port
    this.host = this.connectionParameters.host
    this.loadBalance = this.connectionParameters.loadBalance
    this.topologyKeys = this.connectionParameters.topologyKeys
    this.connectionString = config
    // "hiding" the password so it doesn't show up in stack traces
    // or if the client is console.logged
    Object.defineProperty(this, 'password', {
      configurable: true,
      enumerable: false,
      writable: true,
      value: this.connectionParameters.password,
    })

    this.replication = this.connectionParameters.replication

    var c = config || {}

    this._Promise = c.Promise || global.Promise
    this._types = new TypeOverrides(c.types)
    this._ending = false
    this._connecting = false
    this._connected = false
    this._connectionError = false
    this._queryable = true

    this.connection =
      c.connection ||
      new Connection({
        stream: c.stream,
        ssl: this.connectionParameters.ssl,
        keepAlive: c.keepAlive || false,
        keepAliveInitialDelayMillis: c.keepAliveInitialDelayMillis || 0,
        encoding: this.connectionParameters.client_encoding || 'utf8',
      })
    this.queryQueue = []
    this.binary = c.binary || defaults.binary
    this.processID = null
    this.secretKey = null
    this.ssl = this.connectionParameters.ssl || false
    this.config = config
    // prevHostIfUsePublic will store the private host name before replacing
    // it with public IP for making the connection
    this.prevHostIfUsePublic = this.host
    this.urlHost = this.host
    // As with Password, make SSL->Key (the private key) non-enumerable.
    // It won't show up in stack traces
    // or if the client is console.logged
    if (this.ssl && this.ssl.key) {
      Object.defineProperty(this.ssl, 'key', {
        enumerable: false,
      })
    }

    this._connectionTimeoutMillis = c.connectionTimeoutMillis || 0
  }
  // Control Connection
  static controlClient = undefined
  static lastTimeMetaDataFetched = new Date().getTime() / 1000
  // Map of host -> connectionCount
  static connectionMap = new Map()
  // Map of failedHost -> ServerInfo of host
  static failedHosts = new Map()
  // Map of placementInfoOfHost -> list of Hosts
  static placementInfoHostMap = new Map()
  // Map of Host -> ServerInfo
  static hostServerInfo = new Map()
  // Boolean to check if public IP needs to be used or not
  static usePublic = false
  // Set of topology Keys provided in URL
  static topologyKeySet = new Set()
  // time to refresh the ServerMetaData
  static REFRESING_TIME = 300 // secs
  // Boolean to Refresh ServerMetaData manually (for testing purpose).
  static doHardRefresh = false

  _errorAllQueries(err) {
    const enqueueError = (query) => {
      process.nextTick(() => {
        query.handleError(err, this.connection)
      })
    }

    if (this.activeQuery) {
      enqueueError(this.activeQuery)
      this.activeQuery = null
    }

    this.queryQueue.forEach(enqueueError)
    this.queryQueue.length = 0
  }

  getLeastLoadedServer(hostsList) {
    if (hostsList.size === 0) {
      return this.host
    }
    let minConnectionCount = Number.MAX_VALUE
    let leastLoadedHosts = []
    let hosts = hostsList.keys()
    for (let value of hosts) {
      let host = value
      if (this.connectionParameters.topologyKeys !== '') {
        let placementInfoOfHost
        if (!this.checkConnectionMapEmpty()) {
          placementInfoOfHost = Client.hostServerInfo.get(host).placementInfo
        } else {
          placementInfoOfHost = hostsList.get(host).placementInfo
        }
        if (!Client.topologyKeySet.has(placementInfoOfHost)) {
          continue
        }
      }
      let hostCount
      if (typeof hostsList.get(host) === 'object') {
        hostCount = 0
      } else {
        hostCount = hostsList.get(host)
      }
      if (minConnectionCount > hostCount) {
        leastLoadedHosts = []
        minConnectionCount = hostCount
        leastLoadedHosts.push(host)
      } else if (minConnectionCount === hostCount) {
        leastLoadedHosts.push(host)
      }
    }
    if (leastLoadedHosts.length === 0) {
      return this.host
    }
    let randomIdx = Math.floor(Math.random() * leastLoadedHosts.length - 1) + 1
    let leastLoadedHost = leastLoadedHosts[randomIdx]
    return leastLoadedHost
  }

  isValidKey(key) {
    var keyParts = key.split('.')
    if (keyParts.length !== 3) {
      return false
    }
    return Client.placementInfoHostMap.has(key)
  }

  incrementConnectionCount() {
    let prevCount = 0
    let host = this.host
    if (Client.usePublic) {
      host = this.prevHostIfUsePublic
    }
    if (Client.connectionMap.has(host)) {
      prevCount = Client.connectionMap.get(host)
    } else if (Client.failedHosts.has(host)) {
      let serverInfo = Client.failedHosts.get(host)
      Client.hostServerInfo.set(host, serverInfo)
      Client.failedHosts.delete(host)
    }
    Client.connectionMap.set(host, prevCount + 1)
  }

  _connect(callback) {
    var self = this
    if (this.connectionParameters.loadBalance && this._connecting) {
      this.connection =
        this.config.connection ||
        new Connection({
          stream: this.config.stream,
          ssl: this.connectionParameters.ssl,
          keepAlive: this.config.keepAlive || false,
          keepAliveInitialDelayMillis: this.config.keepAliveInitialDelayMillis || 0,
          encoding: this.connectionParameters.client_encoding || 'utf8',
        })
      this._connecting = false
    }
    var con = this.connection
    this._connectionCallback = callback
    if (this._connecting || this._connected) {
      const err = new Error('Client has already been connected. You cannot reuse a client.')
      process.nextTick(() => {
        callback(err)
      })
      return
    }
    this._connecting = true

    this.connectionTimeoutHandle
    if (this._connectionTimeoutMillis > 0) {
      this.connectionTimeoutHandle = setTimeout(() => {
        con._ending = true
        con.stream.destroy(new Error('timeout expired'))
      }, this._connectionTimeoutMillis)
    }
    if (this.connectionParameters.loadBalance) {
      if (!this.checkConnectionMapEmpty() && Client.hostServerInfo.size) {
        this.host = this.getLeastLoadedServer(Client.connectionMap)
        this.port = Client.hostServerInfo.get(this.host).port
      } else if (Client.failedHosts.size) {
        this.host = this.getLeastLoadedServer(Client.failedHosts)
        this.port = Client.failedHosts.get(this.host).port
      }
    }
    if (Client.usePublic) {
      let currentHost = this.host
      let serverInfo = Client.hostServerInfo.get(currentHost)
      this.prevHostIfUsePublic = currentHost
      this.host = serverInfo.public_ip
    }
    if (this.host && this.host.indexOf('/') === 0) {
      con.connect(this.host + '/.s.PGSQL.' + this.port)
    } else {
      con.connect(this.port, this.host)
    }

    // once connection is established send startup message
    con.on('connect', function () {
      if (self.ssl) {
        con.requestSsl()
      } else {
        con.startup(self.getStartupConf())
      }
    })

    con.on('sslconnect', function () {
      con.startup(self.getStartupConf())
    })

    this._attachListeners(con)

    con.once('end', () => {
      const error = this._ending ? new Error('Connection terminated') : new Error('Connection terminated unexpectedly')

      clearTimeout(this.connectionTimeoutHandle)
      this._errorAllQueries(error)

      if (!this._ending) {
        // if the connection is ended without us calling .end()
        // on this client then we have an unexpected disconnection
        // treat this as an error unless we've already emitted an error
        // during connection.
        if (this._connecting && !this._connectionError) {
          if (this._connectionCallback) {
            this._connectionCallback(error)
          } else {
            this._handleErrorEvent(error)
          }
        } else if (!this._connectionError) {
          this._handleErrorEvent(error)
        }
      }

      process.nextTick(() => {
        this.emit('end')
      })
    })
  }

  attachErrorListenerOnClientConnection(client) {
    client.on('error', () => {
      if (Client.hostServerInfo.has(client.host)) {
        Client.failedHosts.set(client.host, Client.hostServerInfo.get(client.host))
        Client.connectionMap.delete(client.host)
        Client.hostServerInfo.delete(client.host)
      }
      Client.controlClient = undefined
    })
  }

  async getConnection() {
    let currConnectionString = this.connectionString
    var client = new Client(currConnectionString)
    this.attachErrorListenerOnClientConnection(client)
    let lookup = util.promisify(dns.lookup)
    let addresses = [{ address: client.host }]
    await lookup(client.host, { family: 0, all: true }).then((res) => {
      addresses = res
    })
    client.host = addresses[0].address // If both resolved then - IPv6 else IPv4
    client.loadBalance = false
    client.connectionParameters.loadBalance = false
    client.topologyKeys = ''
    client.connectionParameters.topologyKeys = ''
    if (Client.failedHosts.has(client.host)) {
      let upHostsList = Client.hostServerInfo.keys()
      let upHost = upHostsList.next()
      let hostIsUp = false
      while (upHost !== undefined && !hostIsUp) {
        client.host = upHost.value
        client.connectionParameters.host = client.host
        await client
          .nowConnect()
          .then((res) => {
            hostIsUp = true
          })
          .catch((err) => {
            client.connection =
              client.config.connection ||
              new Connection({
                stream: client.config.stream,
                ssl: client.connectionParameters.ssl,
                keepAlive: client.config.keepAlive || false,
                keepAliveInitialDelayMillis: client.config.keepAliveInitialDelayMillis || 0,
                encoding: client.connectionParameters.client_encoding || 'utf8',
              })
            client._connecting = false
            upHost = upHostsList.next()
          })
      }
    } else {
      client.connectionParameters.host = client.host
      await client.nowConnect().catch(async (err) => {
        client.connection =
          client.config.connection ||
          new Connection({
            stream: client.config.stream,
            ssl: client.connectionParameters.ssl,
            keepAlive: client.config.keepAlive || false,
            keepAliveInitialDelayMillis: client.config.keepAliveInitialDelayMillis || 0,
            encoding: client.connectionParameters.client_encoding || 'utf8',
          })
        client._connecting = false
        if (addresses.length === 2) {
          // If both resolved
          client.host = addresses[1].address // IPv4
          if (Client.failedHosts.has(client.host)) {
            let upHostsList = Client.hostServerInfo.keys()
            let upHost = upHostsList.next()
            let hostIsUp = false
            while (upHost !== undefined && !hostIsUp) {
              client.host = upHost.value
              client.connectionParameters.host = client.host
              await client
                .nowConnect()
                .then((res) => {
                  hostIsUp = true
                })
                .catch(async (err) => {
                  client.connection =
                    client.config.connection ||
                    new Connection({
                      stream: client.config.stream,
                      ssl: client.connectionParameters.ssl,
                      keepAlive: client.config.keepAlive || false,
                      keepAliveInitialDelayMillis: client.config.keepAliveInitialDelayMillis || 0,
                      encoding: client.connectionParameters.client_encoding || 'utf8',
                    })
                  client._connecting = false
                  upHost = upHostsList.next()
                })
            }
          } else {
            client.connectionParameters.host = client.host
            await client.nowConnect()
          }
        }
      })
    }
    return client
  }

  async getServersInfo() {
    var client = Client.controlClient
    var result
    await client
      .query(YB_SERVERS_QUERY)
      .then((res) => {
        result = res
      })
      .catch((err) => {
        this.getConnection()
          .then(async (res) => {
            Client.controlClient = res
            await this.getServersInfo()
          })
          .catch((err) => {
            return this.nowConnect(callback)
          })
      })
    return result
  }

  createServersList(data) {
    Client.hostServerInfo.clear()
    data.forEach((eachServer) => {
      var placementInfo = eachServer.cloud + '.' + eachServer.region + '.' + eachServer.zone
      var server = new ServerInfo(eachServer.host, eachServer.port, placementInfo, eachServer.public_ip)
      if (Client.placementInfoHostMap.has(placementInfo)) {
        let currentHosts = Client.placementInfoHostMap.get(placementInfo)
        currentHosts.push(eachServer.host)
        Client.placementInfoHostMap.set(placementInfo, currentHosts)
      } else {
        Client.placementInfoHostMap.set(placementInfo, [eachServer.host])
      }
      Client.hostServerInfo.set(eachServer.host, server)
      if (eachServer.public_ip === this.host) {
        Client.usePublic = true
      }
    })
  }

  createConnectionMap(data) {
    Client.connectionMap.clear()
    data.forEach((eachServer) => {
      Client.connectionMap.set(eachServer.host, 0)
    })
  }

  createTopologyKeySet() {
    var seperatedKeys = this.connectionParameters.topologyKeys.split(',')
    for (let idx = 0; idx < seperatedKeys.length; idx++) {
      let key = seperatedKeys[idx]
      if (this.isValidKey(key)) {
        Client.topologyKeySet.add(key)
      } else {
        throw new Error('Bad Topology Key found - ' + key)
      }
    }
  }

  createMetaData(data) {
    this.createServersList(data)
    Client.lastTimeMetaDataFetched = new Date().getTime() / 1000
    this.createConnectionMap(data)
    if (this.connectionParameters.topologyKeys !== '') {
      this.createTopologyKeySet()
    }
  }

  checkConnectionMapEmpty() {
    if (this.connectionParameters.topologyKeys === '') {
      return Client.connectionMap.size === 0
    }
    let hosts = Client.connectionMap.keys()
    for (let value of hosts) {
      let placementInfo = Client.hostServerInfo.get(value).placementInfo
      if (Client.topologyKeySet.has(placementInfo)) {
        return false
      }
    }
    return true
  }

  nowConnect(callback) {
    if (callback) {
      if (this.connectionParameters.loadBalance) {
        this._connect((error) => {
          if (error) {
            if (this.connectionParameters.loadBalance) {
              if (Client.hostServerInfo.has(this.host)) {
                Client.failedHosts.set(this.host, Client.hostServerInfo.get(this.host))
                Client.connectionMap.delete(this.host)
                Client.hostServerInfo.delete(this.host)
              } else if (Client.failedHosts.has(this.host)) {
                Client.failedHosts.delete(this.host)
              }
              if (this.checkConnectionMapEmpty() && Client.failedHosts.size === 0) {
                lock.release()
                // try with url host and mark that connection type as non-loadBalanced
                this.host = this.urlHost
                this.connectionParameters.host = this.host
                this.connectionParameters.loadBalance = false
                this.connection =
                  this.config.connection ||
                  new Connection({
                    stream: this.config.stream,
                    ssl: this.connectionParameters.ssl,
                    keepAlive: this.config.keepAlive || false,
                    keepAliveInitialDelayMillis: this.config.keepAliveInitialDelayMillis || 0,
                    encoding: this.connectionParameters.client_encoding || 'utf8',
                  })
                this._connecting = false
                Client.hostServerInfo.clear()
                Client.connectionMap.clear()
                this.connect(callback)
                return
              }
              lock.release()
              this.connect(callback)
            } else {
              callback(error)
              return
            }
          } else {
            if (this.connectionParameters.loadBalance) {
              lock.release()
              this.incrementConnectionCount()
            }
            callback()
          }
        })
        return
      } else {
        this._connect(callback)
        return
      }
    }

    return new this._Promise((resolve, reject) => {
      this._connect((error) => {
        if (error) {
          if (this.connectionParameters.loadBalance) {
            if (Client.hostServerInfo.has(this.host)) {
              Client.failedHosts.set(this.host, Client.hostServerInfo.get(this.host))
              Client.connectionMap.delete(this.host)
              Client.hostServerInfo.delete(this.host)
            } else if (Client.failedHosts.has(this.host)) {
              Client.failedHosts.delete(this.host)
            }
            if (this.checkConnectionMapEmpty() && Client.failedHosts.size === 0) {
              lock.release()
              this.host = this.urlHost
              this.connectionParameters.host = this.host
              this.connectionParameters.loadBalance = false
              this.connection =
                this.config.connection ||
                new Connection({
                  stream: this.config.stream,
                  ssl: this.connectionParameters.ssl,
                  keepAlive: this.config.keepAlive || false,
                  keepAliveInitialDelayMillis: this.config.keepAliveInitialDelayMillis || 0,
                  encoding: this.connectionParameters.client_encoding || 'utf8',
                })
              this._connecting = false
              Client.hostServerInfo.clear()
              Client.connectionMap.clear()
              this.connect(callback)
              return
            }
            lock.release()
            this.connect(callback)
          } else {
            reject(error)
          }
        } else {
          if (this.connectionParameters.loadBalance) {
            lock.release()
            this.incrementConnectionCount()
          }
          resolve()
        }
      })
    })
  }

  updateConnectionMapAfterRefresh() {
    let hostsInfoList = Client.hostServerInfo.keys()
    for (let value of hostsInfoList) {
      let eachHost = value
      if (!Client.connectionMap.has(eachHost)) {
        Client.connectionMap.set(eachHost, 0)
      }
    }
    let connectionMapHostList = Client.connectionMap.keys()
    for (let value of connectionMapHostList) {
      let eachHost = value
      if (!Client.hostServerInfo.has(eachHost)) {
        Client.connectionMap.delete(eachHost)
      }
    }
  }

  updateMetaData(data) {
    this.createServersList(data)
    Client.lastTimeMetaDataFetched = new Date().getTime() / 1000
    this.updateConnectionMapAfterRefresh()
  }

  isRefreshRequired() {
    let currentTime = new Date().getTime() / 1000
    let diff = Math.floor(currentTime - Client.lastTimeMetaDataFetched)
    return diff >= Client.REFRESING_TIME
  }

  connect(callback) {
    if (!this.connectionParameters.loadBalance) {
      return this.nowConnect(callback)
    }
    lock.acquire().then(() => {
      if (Client.controlClient === undefined) {
        this.getConnection()
          .then(async (res) => {
            Client.controlClient = res
            this.getServersInfo()
              .catch((err) => {
                return this.nowConnect(callback)
              })
              .then((res) => {
                try {
                  this.createMetaData(res.rows)
                  return this.nowConnect(callback)
                } catch (err) {
                  if (err.message.includes('Bad Topology Key found')) {
                    throw err
                  }
                }
              })
          })
          .catch((err) => {
            return this.nowConnect(callback)
          })
      } else {
        if (this.isRefreshRequired() || Client.doHardRefresh) {
          this.getServersInfo()
            .then((res) => {
              this.updateMetaData(res.rows)
              return this.nowConnect(callback)
            })
            .catch((err) => {
              return this.nowConnect(callback)
            })
        } else {
          return this.nowConnect(callback)
        }
      }
    })
  }

  _attachListeners(con) {
    // password request handling
    con.on('authenticationCleartextPassword', this._handleAuthCleartextPassword.bind(this))
    // password request handling
    con.on('authenticationMD5Password', this._handleAuthMD5Password.bind(this))
    // password request handling (SASL)
    con.on('authenticationSASL', this._handleAuthSASL.bind(this))
    con.on('authenticationSASLContinue', this._handleAuthSASLContinue.bind(this))
    con.on('authenticationSASLFinal', this._handleAuthSASLFinal.bind(this))
    con.on('backendKeyData', this._handleBackendKeyData.bind(this))
    con.on('error', this._handleErrorEvent.bind(this))
    con.on('errorMessage', this._handleErrorMessage.bind(this))
    con.on('readyForQuery', this._handleReadyForQuery.bind(this))
    con.on('notice', this._handleNotice.bind(this))
    con.on('rowDescription', this._handleRowDescription.bind(this))
    con.on('dataRow', this._handleDataRow.bind(this))
    con.on('portalSuspended', this._handlePortalSuspended.bind(this))
    con.on('emptyQuery', this._handleEmptyQuery.bind(this))
    con.on('commandComplete', this._handleCommandComplete.bind(this))
    con.on('parseComplete', this._handleParseComplete.bind(this))
    con.on('copyInResponse', this._handleCopyInResponse.bind(this))
    con.on('copyData', this._handleCopyData.bind(this))
    con.on('notification', this._handleNotification.bind(this))
  }

  // TODO(bmc): deprecate pgpass "built in" integration since this.password can be a function
  // it can be supplied by the user if required - this is a breaking change!
  _checkPgPass(cb) {
    const con = this.connection
    if (typeof this.password === 'function') {
      this._Promise
        .resolve()
        .then(() => this.password())
        .then((pass) => {
          if (pass !== undefined) {
            if (typeof pass !== 'string') {
              con.emit('error', new TypeError('Password must be a string'))
              return
            }
            this.connectionParameters.password = this.password = pass
          } else {
            this.connectionParameters.password = this.password = null
          }
          cb()
        })
        .catch((err) => {
          con.emit('error', err)
        })
    } else if (this.password !== null) {
      cb()
    } else {
      pgPass(this.connectionParameters, (pass) => {
        if (undefined !== pass) {
          this.connectionParameters.password = this.password = pass
        }
        cb()
      })
    }
  }

  _handleAuthCleartextPassword(msg) {
    this._checkPgPass(() => {
      this.connection.password(this.password)
    })
  }

  _handleAuthMD5Password(msg) {
    this._checkPgPass(() => {
      const hashedPassword = utils.postgresMd5PasswordHash(this.user, this.password, msg.salt)
      this.connection.password(hashedPassword)
    })
  }

  _handleAuthSASL(msg) {
    this._checkPgPass(() => {
      this.saslSession = sasl.startSession(msg.mechanisms)
      this.connection.sendSASLInitialResponseMessage(this.saslSession.mechanism, this.saslSession.response)
    })
  }

  _handleAuthSASLContinue(msg) {
    sasl.continueSession(this.saslSession, this.password, msg.data)
    this.connection.sendSCRAMClientFinalMessage(this.saslSession.response)
  }

  _handleAuthSASLFinal(msg) {
    sasl.finalizeSession(this.saslSession, msg.data)
    this.saslSession = null
  }

  _handleBackendKeyData(msg) {
    this.processID = msg.processID
    this.secretKey = msg.secretKey
  }

  _handleReadyForQuery(msg) {
    if (this._connecting) {
      this._connecting = false
      this._connected = true
      clearTimeout(this.connectionTimeoutHandle)

      // process possible callback argument to Client#connect
      if (this._connectionCallback) {
        this._connectionCallback(null, this)
        // remove callback for proper error handling
        // after the connect event
        this._connectionCallback = null
      }
      this.emit('connect')
    }
    const { activeQuery } = this
    this.activeQuery = null
    this.readyForQuery = true
    if (activeQuery) {
      activeQuery.handleReadyForQuery(this.connection)
    }
    this._pulseQueryQueue()
  }

  // if we receieve an error event or error message
  // during the connection process we handle it here
  _handleErrorWhileConnecting(err) {
    if (this._connectionError) {
      // TODO(bmc): this is swallowing errors - we shouldn't do this
      if (this.connectionParameters.loadBalance) {
        if (this._connectionCallback) {
          return this._connectionCallback(err)
        }
        this.emit('error', err)
        return
      }
      return
    }
    this._connectionError = true
    clearTimeout(this.connectionTimeoutHandle)
    if (this._connectionCallback) {
      return this._connectionCallback(err)
    }
    this.emit('error', err)
  }

  // if we're connected and we receive an error event from the connection
  // this means the socket is dead - do a hard abort of all queries and emit
  // the socket error on the client as well
  _handleErrorEvent(err) {
    if (this._connecting) {
      return this._handleErrorWhileConnecting(err)
    }
    this._queryable = false
    this._errorAllQueries(err)
    this.emit('error', err)
  }

  // handle error messages from the postgres backend
  _handleErrorMessage(msg) {
    if (this._connecting) {
      return this._handleErrorWhileConnecting(msg)
    }
    const activeQuery = this.activeQuery

    if (!activeQuery) {
      this._handleErrorEvent(msg)
      return
    }

    this.activeQuery = null
    activeQuery.handleError(msg, this.connection)
  }

  _handleRowDescription(msg) {
    // delegate rowDescription to active query
    this.activeQuery.handleRowDescription(msg)
  }

  _handleDataRow(msg) {
    // delegate dataRow to active query
    this.activeQuery.handleDataRow(msg)
  }

  _handlePortalSuspended(msg) {
    // delegate portalSuspended to active query
    this.activeQuery.handlePortalSuspended(this.connection)
  }

  _handleEmptyQuery(msg) {
    // delegate emptyQuery to active query
    this.activeQuery.handleEmptyQuery(this.connection)
  }

  _handleCommandComplete(msg) {
    // delegate commandComplete to active query
    this.activeQuery.handleCommandComplete(msg, this.connection)
  }

  _handleParseComplete(msg) {
    // if a prepared statement has a name and properly parses
    // we track that its already been executed so we don't parse
    // it again on the same client
    if (this.activeQuery.name) {
      this.connection.parsedStatements[this.activeQuery.name] = this.activeQuery.text
    }
  }

  _handleCopyInResponse(msg) {
    this.activeQuery.handleCopyInResponse(this.connection)
  }

  _handleCopyData(msg) {
    this.activeQuery.handleCopyData(msg, this.connection)
  }

  _handleNotification(msg) {
    this.emit('notification', msg)
  }

  _handleNotice(msg) {
    this.emit('notice', msg)
  }

  getStartupConf() {
    var params = this.connectionParameters

    var data = {
      user: params.user,
      database: params.database,
    }

    var appName = params.application_name || params.fallback_application_name
    if (appName) {
      data.application_name = appName
    }
    if (params.replication) {
      data.replication = '' + params.replication
    }
    if (params.statement_timeout) {
      data.statement_timeout = String(parseInt(params.statement_timeout, 10))
    }
    if (params.idle_in_transaction_session_timeout) {
      data.idle_in_transaction_session_timeout = String(parseInt(params.idle_in_transaction_session_timeout, 10))
    }
    if (params.options) {
      data.options = params.options
    }

    return data
  }

  cancel(client, query) {
    if (client.activeQuery === query) {
      var con = this.connection

      if (this.host && this.host.indexOf('/') === 0) {
        con.connect(this.host + '/.s.PGSQL.' + this.port)
      } else {
        con.connect(this.port, this.host)
      }

      // once connection is established send cancel message
      con.on('connect', function () {
        con.cancel(client.processID, client.secretKey)
      })
    } else if (client.queryQueue.indexOf(query) !== -1) {
      client.queryQueue.splice(client.queryQueue.indexOf(query), 1)
    }
  }

  setTypeParser(oid, format, parseFn) {
    return this._types.setTypeParser(oid, format, parseFn)
  }

  getTypeParser(oid, format) {
    return this._types.getTypeParser(oid, format)
  }

  // Ported from PostgreSQL 9.2.4 source code in src/interfaces/libpq/fe-exec.c
  escapeIdentifier(str) {
    return '"' + str.replace(/"/g, '""') + '"'
  }

  // Ported from PostgreSQL 9.2.4 source code in src/interfaces/libpq/fe-exec.c
  escapeLiteral(str) {
    var hasBackslash = false
    var escaped = "'"

    for (var i = 0; i < str.length; i++) {
      var c = str[i]
      if (c === "'") {
        escaped += c + c
      } else if (c === '\\') {
        escaped += c + c
        hasBackslash = true
      } else {
        escaped += c
      }
    }

    escaped += "'"

    if (hasBackslash === true) {
      escaped = ' E' + escaped
    }

    return escaped
  }

  _pulseQueryQueue() {
    if (this.readyForQuery === true) {
      this.activeQuery = this.queryQueue.shift()
      if (this.activeQuery) {
        this.readyForQuery = false
        this.hasExecuted = true

        const queryError = this.activeQuery.submit(this.connection)
        if (queryError) {
          process.nextTick(() => {
            this.activeQuery.handleError(queryError, this.connection)
            this.readyForQuery = true
            this._pulseQueryQueue()
          })
        }
      } else if (this.hasExecuted) {
        this.activeQuery = null
        this.emit('drain')
      }
    }
  }

  query(config, values, callback) {
    // can take in strings, config object or query object
    var query
    var result
    var readTimeout
    var readTimeoutTimer
    var queryCallback

    if (config === null || config === undefined) {
      throw new TypeError('Client was passed a null or undefined query')
    } else if (typeof config.submit === 'function') {
      readTimeout = config.query_timeout || this.connectionParameters.query_timeout
      result = query = config
      if (typeof values === 'function') {
        query.callback = query.callback || values
      }
    } else {
      readTimeout = this.connectionParameters.query_timeout
      query = new Query(config, values, callback)
      if (!query.callback) {
        result = new this._Promise((resolve, reject) => {
          query.callback = (err, res) => (err ? reject(err) : resolve(res))
        })
      }
    }

    if (readTimeout) {
      queryCallback = query.callback

      readTimeoutTimer = setTimeout(() => {
        var error = new Error('Query read timeout')

        process.nextTick(() => {
          query.handleError(error, this.connection)
        })

        queryCallback(error)

        // we already returned an error,
        // just do nothing if query completes
        query.callback = () => {}

        // Remove from queue
        var index = this.queryQueue.indexOf(query)
        if (index > -1) {
          this.queryQueue.splice(index, 1)
        }

        this._pulseQueryQueue()
      }, readTimeout)

      query.callback = (err, res) => {
        clearTimeout(readTimeoutTimer)
        queryCallback(err, res)
      }
    }

    if (this.binary && !query.binary) {
      query.binary = true
    }

    if (query._result && !query._result._types) {
      query._result._types = this._types
    }

    if (!this._queryable) {
      process.nextTick(() => {
        query.handleError(new Error('Client has encountered a connection error and is not queryable'), this.connection)
      })
      return result
    }

    if (this._ending) {
      process.nextTick(() => {
        query.handleError(new Error('Client was closed and is not queryable'), this.connection)
      })
      return result
    }

    this.queryQueue.push(query)
    this._pulseQueryQueue()
    return result
  }

  ref() {
    this.connection.ref()
  }

  unref() {
    this.connection.unref()
  }

  end(cb) {
    this._ending = true

    // if we have never connected, then end is a noop, callback immediately
    if (!this.connection._connecting) {
      if (cb) {
        cb()
      } else {
        return this._Promise.resolve()
      }
    }

    if (this.activeQuery || !this._queryable) {
      // if we have an active query we need to force a disconnect
      // on the socket - otherwise a hung query could block end forever
      this.connection.stream.destroy()
    } else {
      this.connection.end()
    }

    lock.acquire().then(() => {
      if (this.connectionParameters.loadBalance) {
        let prevCount = Client.connectionMap.get(this.host)
        if (prevCount > 0) {
          Client.connectionMap.set(this.host, prevCount - 1)
        }
        lock.release()
      }
    })

    if (cb) {
      this.connection.once('end', cb)
    } else {
      return new this._Promise((resolve) => {
        this.connection.once('end', resolve)
      })
    }
  }
}

// expose a Query constructor
Client.Query = Query

module.exports = Client
