'use strict';

const mqtt             = require('mqtt');
const { addToBuffer }  = require('./batch-processor');
const { broadcast }    = require('./websocket');
const config           = require('./config');
const { createLogger } = require('./logger');

const log = createLogger('MQTT');

let client    = null;
let connected = false;
let msgCount  = 0;

function initMQTTSubscriber() {
  log.info(`Connecting to broker: ${config.MQTT_BROKER}`);

  client = mqtt.connect(config.MQTT_BROKER, {
    clientId:        `iot-backend-${process.pid}`,
    keepalive:       60,
    reconnectPeriod: 3_000,
    connectTimeout:  10_000,
    clean:           true,
  });

  client.on('connect', () => {
    connected = true;
    client.subscribe(config.MQTT_TOPIC, { qos: 1 }, (err, granted) => {
      if (err) { log.error(`Subscribe failed: ${err.message}`); return; }
      log.info(`Subscribed: ${granted.map(g => `${g.topic} (qos${g.qos})`).join(', ')}`);
    });
  });

  client.on('message', (topic, rawBuffer) => {
    msgCount++;

    if (msgCount % 100 === 0) log.debug(`Received ${msgCount} messages total`);

    let payload;
    try {
      payload = JSON.parse(rawBuffer.toString());
    } catch {
      log.warn(`JSON parse error on topic "${topic}" — message dropped`);
      return;
    }

    if (!payload.tenantID || !payload.deviceID) {
      log.warn(`Missing tenantID or deviceID — message dropped`);
      return;
    }


    addToBuffer(payload);
    broadcast(payload);
  });

  client.on('error',      err    => log.error(`Broker error: ${err.message}`));
  client.on('reconnect',  ()     => log.warn('Reconnecting to broker...'));
  client.on('disconnect', packet => log.warn(`Broker DISCONNECT: ${packet?.reasonCode ?? ''}`));
  client.on('close',      ()     => { connected = false; log.warn('Connection closed'); });
  client.on('offline',    ()     => log.warn('Client offline — broker unreachable'));
}

function closeMQTTClient() {
  return new Promise(resolve => {
    if (!client) return resolve();
    client.end(false, {}, () => {
      log.info('MQTT client closed');
      resolve();
    });
  });
}

function getMQTTStatus() {
  return connected ? 'connected' : 'disconnected';
}

module.exports = { initMQTTSubscriber, closeMQTTClient, getMQTTStatus };
