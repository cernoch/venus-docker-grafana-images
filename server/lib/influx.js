const {InfluxDB, Point, HttpError} = require('@influxdata/influxdb-client')
const _ = require('lodash')
const debug = require('debug')('venus-server:influxdb')

function Influx (app) {
  this.app = app
  this.logger = app.getLogger('influxdb')
  this.debug = this.logger.debug.bind(this.logger)
  this.info = this.logger.info.bind(this.logger)
  this.connected = false

  this.accumulatedPoints = []
  this.lastWriteTime = Date.now()
  this.batchWriteInterval =
    (_.isUndefined(app.config.settings.influxdb.batchWriteInterval)
      ? 10
      : app.config.settings.influxdb.batchWriteInterval) * 1000

  app.on('settingsChanged', settings => {
    this.batchWriteInterval =
      (_.isUndefined(app.config.settings.influxdb.batchWriteInterval)
        ? 10
        : app.config.settings.influxdb.batchWriteInterval) * 1000

    if (!this.connected) {
      return
    }

    const { host, port, token, organization, bucket, retention } = settings.influxdb

    if (
      this.host !== host ||
      this.port !== port ||
      this.token !== token ||
      this.organization !== organization ||
      this.bucket !== bucket
    ) {
      this.connected = false
      this.connect()
        .then(() => {})
        .catch(err => {
          this.logger.error(err)
          const interval = setInterval(() => {
            app.influxdb
              .connect()
              .then(() => {
                clearInterval(interval)
              })
              .catch(err => {
                this.logger.error(err)
              })
          }, 5000)
        })
    } else if (!_.isUndefined(this.retention) && retention != this.retention) {
      this.client.then(client => {
        this.setRetentionPolicy(client, retention)
      })
    }
  })
}

Influx.prototype.setRetentionPolicy = function (client, retention) {
  const opts = {
    duration: retention,
    replication: 1,
    isDefault: true
  }
  return new Promise((resolve, reject) => {
    client
      .createRetentionPolicy('venus_default', opts)
      .then(() => {
        this.logger.info(`Set retention policy to ${retention}`)
        this.retention = retention
        resolve()
      })
      .catch(err => {
        client
          .alterRetentionPolicy('venus_default', opts)
          .then(() => {
            this.retention = retention
            this.logger.info(`Set retention policy to ${retention}`)
            resolve()
          })
          .catch(reject)
      })
  })
}

Influx.prototype.connect = function () {
  const { host, port, token, organization, bucket, retention } = this.app.config.settings.influxdb
  this.host = host
  this.port = port
  this.token = token
  this.organization = organization
  this.bucket = bucket
  this.info(`Attempting connection to ${host}:${port} org=${organization} bucket=${bucket}`)
  this.client = new Promise((resolve, reject) => {
    const url = 'http://' + host + ':' + port;
    const client = new InfluxDB({url, token}).getWriteApi(organization, bucket, 'ns')
    this.connected = true
    resolve(client)

    /*
    client
      .getDatabaseNames()
      .then(names => {
        this.info('Connected')
        if (names.includes(database)) {
          this.setRetentionPolicy(client, retention)
            .then(() => {
              resolve(client)
            })
            .catch(reject)
        } else {
          client.createDatabase(database).then(result => {
            this.info('Created InfluxDb database ' + database)
            this.setRetentionPolicy(client, retention)
              .then(() => {
                resolve(client)
              })
              .catch(reject)
          })
        }
      })
      .catch(reject)
      */
  })
  return this.client
}

Influx.prototype.store = function (
  portalId,
  name,
  instanceNumber,
  measurement,
  value
) {
  if (this.connected === false || _.isUndefined(value) || value === null) {
    return
  }

  let point = {};
  if (typeof value === 'string') {
    if (value.length === 0) {
      //influxdb won't allow empty strings
      return
    }

    point = new Point(measurement)
        .tag('portalId', portalId)
        .tag('instanceNumber', instanceNumber)
        .tag('name', name || portalId)
        .stringField('valueString', value)
        .timestamp(new Date())
  } else if (typeof value === 'number') {
    point = new Point(measurement)
        .tag('portalId', portalId)
        .tag('instanceNumber', instanceNumber)
        .tag('name', name || portalId)
        .floatField('value', value)
        .timestamp(new Date())
  } else {
    return
  }

  this.client
  .then(client => {
    client.writePoint(point).catch(err => {
      //this.app.emit('error', err)
      this.debug(err)
    })
  })
  .catch(error => {
    //this.app.emit('error', error)
    this.debug(error)
  })

  /*
  this.accumulatedPoints.push(point)
  const now = Date.now()
  if (
    this.batchWriteInterval === 0 ||
    now - this.lastWriteTime > this.batchWriteInterval
  ) {
    this.lastWriteTime = now

    this.client
      .then(client => {
        client.writePoints(this.accumulatedPoints).catch(err => {
          //this.app.emit('error', err)
          this.debug(err)
        })
        this.info(this.accumulatedPoints)
        this.accumulatedPoints = []
      })
      .catch(error => {
        //this.app.emit('error', error)
        this.debug(error)
        this.accumulatedPoints = []
      })
  }
  */
}

module.exports = Influx
