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
const { logger } = require('./logger')
const YB_SERVERS_QUERY = 'SELECT * FROM yb_servers()'
const DEFAULT_FAILED_HOST_TTL_SECONDS = 5

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
    logger.silly("Received connection string " + config)
    super()
    this.connectionParameters = new ConnectionParameters(config)
    this.user = this.connectionParameters.user
    this.database = this.connectionParameters.database
    this.port = this.connectionParameters.port
    this.host = this.connectionParameters.host
    this.loadBalance = this.connectionParameters.loadBalance
    this.topologyKeys = this.connectionParameters.topologyKeys
    this.ybServersRefreshInterval = this.connectionParameters.ybServersRefreshInterval
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
  // Map of failedHost -> Time at which host was added to failedHosts Map
  static failedHostsTime = new Map()
  // Map of placementInfoOfHost -> list of Hosts
  static placementInfoHostMap = new Map()
  // Map of Host -> ServerInfo
  static hostServerInfo = new Map()
  // Boolean to check if public IP needs to be used or not
  static usePublic = false
  // Map of topology Keys provided in URL
  static topologyKeyMap = new Map()

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
    logger.silly("getLeaseLoadedServer() is called")
    if (hostsList.size === 0) {
      return this.host
    }
    let minConnectionCount = Number.MAX_VALUE
    let leastLoadedHosts = []
    for (var i = 1; i <= Client.topologyKeyMap.size; i++) {
      let hosts = hostsList.keys()
      for (let host of hosts) {
        let placementInfoOfHost
        if (Client.hostServerInfo.has(host)) {
          placementInfoOfHost = Client.hostServerInfo.get(host).placementInfo
        } else {
          placementInfoOfHost = hostsList.get(host).placementInfo
        }
        var toCheckStar = placementInfoOfHost.split('.')
        var starPlacementInfoOfHost = toCheckStar[0]+"."+toCheckStar[1]+".*"
        if (!Client.topologyKeyMap.get(i).includes(placementInfoOfHost) && !Client.topologyKeyMap.get(i).includes(starPlacementInfoOfHost)) {
          continue
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
      if(leastLoadedHosts.length != 0){
        break
      }
    }

    if (leastLoadedHosts.length === 0) {
      let hosts = hostsList.keys()
      for (let value of hosts) {
        let hostCount
        if (typeof hostsList.get(value) === 'object') {
          hostCount = 0
        } else {
          hostCount = hostsList.get(value)
        }
        if (minConnectionCount > hostCount) {
          leastLoadedHosts = []
          minConnectionCount = hostCount
          leastLoadedHosts.push(value)
        } else if (minConnectionCount === hostCount) {
          leastLoadedHosts.push(value)
        }
      }
    }
    if (leastLoadedHosts.length === 0) {
      return this.host
    }
    let randomIdx = Math.floor(Math.random() * leastLoadedHosts.length - 1) + 1
    let leastLoadedHost = leastLoadedHosts[randomIdx]
    logger.silly("Least loaded servers are " + leastLoadedHosts)
    logger.debug("Returning " + leastLoadedHost + "as the least loaded host")
    return leastLoadedHost
  }

  isValidKey(key) {
    var zones = key.split(':')
    if (zones.length == 0 || zones.length >2) {
      return false
    }
    var keyParts = zones[0].split('.')
    if (keyParts.length !== 3) {
      return false
    }
    if (zones[1]==undefined) {
      zones[1]='1'
    }
    zones[1]=Number(zones[1])
    if (zones[1]<1 || zones[1]>10 || isNaN(zones[1]) || !Number.isInteger(zones[1])) {
      return false
    }
    if (keyParts[2]!="*") {
      return Client.placementInfoHostMap.has(zones[0])
    } else {
      var allPlacementInfo = Client.placementInfoHostMap.keys();
      for(let placeInfo of allPlacementInfo){
        var placeInfoParts = placeInfo.split('.')
        if(keyParts[0]==placeInfoParts[0] && keyParts[1]==placeInfoParts[1]){
          return true
        }
      }
    }
    logger.warn("Given topology-key " + key + " is invalid")
    return false
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
      logger.debug("Removing " + host + " from failed host list")
      let serverInfo = Client.failedHosts.get(host)
      Client.hostServerInfo.set(host, serverInfo)
      Client.failedHosts.delete(host)
      Client.failedHostsTime.delete(host)
    }
    Client.connectionMap.set(host, prevCount + 1)
    logger.debug("Increasing connection count of " + host + " by 1")
  }

  _connect(callback) {
    logger.silly("connect() is called")
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
      if (Client.connectionMap.size && Client.hostServerInfo.size) {
        this.host = this.getLeastLoadedServer(Client.connectionMap)
        this.port = Client.hostServerInfo.get(this.host).port
        logger.silly("Least loaded host received " + this.host + " port " + this.port)
      }
    }
    if (Client.usePublic) {
      let currentHost = this.host
      let serverInfo = Client.hostServerInfo.get(currentHost)
      this.prevHostIfUsePublic = currentHost
      this.host = serverInfo.public_ip
      logger.silly("Using public ips, host " + this.host)
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
        logger.debug("Not able to connect to host " + client.host + " adding it to failedHosts")
        Client.failedHosts.set(client.host, Client.hostServerInfo.get(client.host))
        let start = new Date().getTime();
        Client.failedHostsTime.set(client.host, start)
        Client.connectionMap.delete(client.host)
        Client.hostServerInfo.delete(client.host)
      }
      Client.controlClient = undefined
    })
  }

  async iterateHostList(client) {
    let upHostsList = Client.hostServerInfo.keys()
    let upHost = upHostsList.next()
    let hostIsUp = false
    while (upHost.value !== undefined && !hostIsUp && !Client.failedHosts.has(upHost.value)) {
      client.host = upHost.value
      client.connectionParameters.host = client.host
      logger.debug("Trying to create control connection to " + client.host)
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
            logger.debug("Not able to create control connection to host " + client.host + " adding it to failedHosts")
            Client.failedHosts.set(client.host, Client.hostServerInfo.get(client.host))
            let start = new Date().getTime();
            Client.failedHostsTime.set(client.host, start)
            Client.connectionMap.delete(client.host)
            Client.hostServerInfo.delete(client.host)
          client._connecting = false
          upHost = upHostsList.next()
        })
    }
    if(!hostIsUp) {
      logger.debug("Not able to create control connection to any host in the cluster")
      throw new Error('Not able to create control connection to any host in the cluster')
    }
  }

  async getConnection() {
    logger.silly("Creating control connection...")
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
      await this.iterateHostList(client)
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
            await this.iterateHostList(client)
          } else {
            client.connectionParameters.host = client.host
            await client.nowConnect()
          }
        }
      })
    }
    logger.debug("Created control connection to host " + client.host)
    return client
  }

  async getServersInfo() {
    logger.silly("Refreshing server info")
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
    logger.silly("Creating servers list")
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
      logger.debug("Updated placementInfoHost Map " + [...Client.placementInfoHostMap])
      Client.hostServerInfo.set(eachServer.host, server)
      if (eachServer.public_ip === this.host) {
        Client.usePublic = true
      }
      logger.debug("Updated hostServerInfo to " + [...Client.hostServerInfo] + " and usePublic to " + Client.usePublic)
    })
  }

  createConnectionMap(data) {
    logger.silly("Creating connection map")
    const currConnectionMap = new Map(Client.connectionMap)
    Client.connectionMap.clear()
    data.forEach((eachServer) => {
      if(!Client.failedHosts.has(eachServer.host)){
        if(currConnectionMap.has(eachServer.host)){
          Client.connectionMap.set(eachServer.host, currConnectionMap.get(eachServer.host))
        } else {
          Client.connectionMap.set(eachServer.host, 0)
        }
      } else {
        let start = new Date().getTime();
        if (start - Client.failedHostsTime.get(eachServer.host) > (DEFAULT_FAILED_HOST_TTL_SECONDS * 1000)) {
          logger.debug("Removing " + eachServer.host + " from failed host list")
          Client.connectionMap.set(eachServer.host, 0)
          Client.failedHosts.delete(eachServer.host)
          Client.failedHostsTime.delete(eachServer.host)
        }
      }
    })
    logger.debug("Updated connection map " + [...Client.connectionMap])
  }

  createTopologyKeyMap() {
    logger.silly("Creating Topology key map")
    var seperatedKeys = this.connectionParameters.topologyKeys.split(',')
    for (let idx = 0; idx < seperatedKeys.length; idx++) {
      let key = seperatedKeys[idx]
      if (this.isValidKey(key)) {
        var zones = key.split(':')
        if (zones[1]==undefined) {
          zones[1]='1'
        }
        zones[1]=parseInt(zones[1])
        if (Client.topologyKeyMap.has(zones[1])) {
          let currentzones = Client.topologyKeyMap.get(zones[1])
          currentzones.push(zones[0])
          Client.topologyKeyMap.set(zones[1],currentzones)
        } else {
          Client.topologyKeyMap.set(zones[1],[zones[0]])
        }
      } else {
        throw new Error('Bad Topology Key found - ' + key)
      }
    }
    logger.debug("Updated topologyKey Map " + [...Client.topologyKeyMap])
  }

  createMetaData(data) {
    logger.silly("Creating metadata ...")
    this.createServersList(data)
    Client.lastTimeMetaDataFetched = new Date().getTime() / 1000
    this.createConnectionMap(data)
    if (this.connectionParameters.topologyKeys !== '') {
      this.createTopologyKeyMap()
    }
  }

  nowConnect(callback) {
    logger.silly("nowConnect() is called...")
    if (callback) {
      if (this.connectionParameters.loadBalance) {
        this._connect((error) => {
          if (error) {
            if (this.connectionParameters.loadBalance) {
              if (Client.hostServerInfo.has(this.host)) {
                logger.debug("Adding " + this.host + " to failed host list")
                Client.failedHosts.set(this.host, Client.hostServerInfo.get(this.host))
                let start = new Date().getTime();
                Client.failedHostsTime.set(this.host, start)
                Client.connectionMap.delete(this.host)
                Client.hostServerInfo.delete(this.host)
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
          if (this.connectionParameters.loadBalance && Client.hostServerInfo.size !== 0) {
            if (Client.hostServerInfo.has(this.host)) {
              logger.debug("Adding " + this.host + " to failed host list")
              Client.failedHosts.set(this.host, Client.hostServerInfo.get(this.host))
              let start = new Date().getTime();
              Client.failedHostsTime.set(this.host, start)
              Client.connectionMap.delete(this.host)
              Client.hostServerInfo.delete(this.host)
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
    logger.silly("Updating connection map after refresh")
    let hostsInfoList = Client.hostServerInfo.keys()
    for (let eachHost of hostsInfoList) {
      if (!Client.connectionMap.has(eachHost)) {
        if(!Client.failedHosts.has(eachHost)){
          Client.connectionMap.set(eachHost, 0)
        } else {
          let start = new Date().getTime();
          if (start - Client.failedHostsTime.get(eachHost) > (DEFAULT_FAILED_HOST_TTL_SECONDS * 1000)) {
            logger.debug("Removing" + eachHost + " from failed host list")
            Client.connectionMap.set(eachHost, 0)
            Client.failedHosts.delete(eachHost)
            Client.failedHostsTime.delete(eachHost)
          }
        }
      }
    }
    let connectionMapHostList = Client.connectionMap.keys()
    for (let eachHost of connectionMapHostList) {
      if (!Client.hostServerInfo.has(eachHost)) {
        Client.connectionMap.delete(eachHost)
      }
    }
    logger.debug("Updated connection Map after refresh " + [...Client.connectionMap]);
    logger.debug("Updated failed host list after refresh " + [...Client.failedHosts]);
  }

  updateMetaData(data) {
    logger.silly("Updating MetaData")
    this.createServersList(data)
    Client.lastTimeMetaDataFetched = new Date().getTime() / 1000
    this.updateConnectionMapAfterRefresh()
  }

  isRefreshRequired() {
    let currentTime = new Date().getTime() / 1000
    let diff = Math.floor(currentTime - Client.lastTimeMetaDataFetched)
    if (diff >= this.connectionParameters.ybServersRefreshInterval) {
      logger.silly("Refresh is required")
      return true
    } else {
      logger.silly("Refresh is not required")
      return false
    }
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
        if (this.isRefreshRequired()) {
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
      if (this.connectionParameters.loadBalance || Client.controlClient === undefined) {
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
          logger.debug("Decreasing connection count (" + prevCount + ") of " + this.host + " by 1")
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
