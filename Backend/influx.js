'use strict';

const { InfluxDB } = require('@influxdata/influxdb-client');
const config = require('./config');


module.exports = new InfluxDB({ url: config.INFLUX_URL, token: config.INFLUX_TOKEN });
