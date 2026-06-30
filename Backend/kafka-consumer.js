'use strict';

const fs                  = require('fs');
const { Kafka, logLevel } = require('kafkajs');
const { addToBuffer }     = require('./batch-processor');
const { broadcast }       = require('./websocket');
const config              = require('./config');
const { createLogger }    = require('./logger');

const log = createLogger('Kafka');

let consumer  = null;
let connected = false;
let msgCount  = 0;

// Suppress kafkajs 2.2.x cosmetic bug: Node emits TimeoutNegativeWarning when kafkajs
// passes (requestTimeout - Date.now()) to setTimeout instead of a relative duration.
// The warning type is the second argument to process.emitWarning, not part of the message.
const _origEmitWarning = process.emitWarning.bind(process);
process.emitWarning = (warning, ...args) => {
  if (args[0] === 'TimeoutNegativeWarning') return;
  if (typeof warning === 'string' && warning.includes('is a negative number')) return;
  _origEmitWarning(warning, ...args);
};

// ── Admin: create topic if it does not exist ─────────────────────────────────
async function ensureTopic(kafka) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = await admin.listTopics();
    if (existing.includes(config.KAFKA_TOPIC)) {
      log.info(`Kafka topic "${config.KAFKA_TOPIC}" already exists`);
      return;
    }
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{
        topic:             config.KAFKA_TOPIC,
        numPartitions:     1,
        replicationFactor: 1,   // increase to match your Aiven broker count if needed
      }],
    });
    log.info(`Created Kafka topic: ${config.KAFKA_TOPIC}`);
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

// ── Consumer ─────────────────────────────────────────────────────────────────
async function initKafkaConsumer() {
  const kafka = new Kafka({
    clientId: `iot-backend-${process.pid}`,
    brokers:  config.KAFKA_BROKERS.split(',').map(b => b.trim()),
    ssl: {
      rejectUnauthorized: true,
      ca:   [fs.readFileSync(config.KAFKA_SSL_CA_PATH)],
      cert: fs.readFileSync(config.KAFKA_SSL_CERT_PATH),
      key:  fs.readFileSync(config.KAFKA_SSL_KEY_PATH),
    },
    logLevel: logLevel.WARN,
    retry: {
      initialRetryTime: 3_000,
      retries:          10,
    },
  });

  await ensureTopic(kafka);

  consumer = kafka.consumer({ groupId: 'iot-backend-consumer' });

  consumer.on(consumer.events.DISCONNECT, () => {
    connected = false;
    log.warn('Kafka consumer disconnected — reconnecting...');
  });

  consumer.on(consumer.events.CRASH, ({ payload: { error, restart } }) => {
    connected = false;
    log.error(`Kafka consumer crashed: ${error.message} | auto-restart: ${restart}`);
  });

  await consumer.connect();
  connected = true;
  log.info(`Connected to Kafka: ${config.KAFKA_BROKERS}`);

  await consumer.subscribe({ topic: config.KAFKA_TOPIC, fromBeginning: false });
  log.info(`Subscribed to topic: ${config.KAFKA_TOPIC}`);

  await consumer.run({
    // Synchronous O(1) dispatch — no await, no DB calls, mirrors original MQTT handler
    eachMessage: ({ topic, partition, message }) => {
      msgCount++;
      if (msgCount % 1000 === 0) {
        log.debug(`Processed ${msgCount} Kafka messages`);
      }

      let payload;
      try {
        payload = JSON.parse(message.value.toString());
      } catch {
        log.warn(`JSON parse error on ${topic}[${partition}]@${message.offset} — dropped`);
        return;
      }

      if (!payload.tenantID || !payload.deviceID) {
        log.warn(`Missing tenantID/deviceID at ${topic}[${partition}]@${message.offset} — dropped`);
        return;
      }

      addToBuffer(payload);
      broadcast(payload);
    },
  });
}

async function closeKafkaConsumer() {
  if (!consumer) return;
  try {
    await consumer.disconnect();
    log.info('Kafka consumer disconnected gracefully');
  } catch (err) {
    log.error(`Kafka consumer close error: ${err.message}`);
  } finally {
    connected = false;
  }
}

function getKafkaStatus() {
  return connected ? 'connected' : 'disconnected';
}

module.exports = { initKafkaConsumer, closeKafkaConsumer, getKafkaStatus };
