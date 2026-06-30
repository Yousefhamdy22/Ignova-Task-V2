'use strict';

const fs                           = require('fs');
const { Kafka, logLevel, Partitioners } = require('kafkajs');
const config                       = require('./config');
const { createLogger }             = require('./logger');

const log = createLogger('KafkaProducer');

// Reuse the same mTLS SSL config pattern as kafka-consumer.js
const kafka = new Kafka({
  clientId: `iot-backend-producer-${process.pid}`,
  brokers:  config.KAFKA_BROKERS.split(',').map(b => b.trim()),
  ssl: {
    rejectUnauthorized: true,
    ca:   [fs.readFileSync(config.KAFKA_SSL_CA_PATH)],
    cert: fs.readFileSync(config.KAFKA_SSL_CERT_PATH),
    key:  fs.readFileSync(config.KAFKA_SSL_KEY_PATH),
  },
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 1_000, retries: 5 },
});

const producer = kafka.producer({
  createPartitioner:    Partitioners.LegacyPartitioner,
  allowAutoTopicCreation: false,
});

let connected = false;

async function initKafkaProducer() {
  await producer.connect();
  connected = true;
  log.info(`Producer connected: ${config.KAFKA_BROKERS}`);

  producer.on(producer.events.DISCONNECT, () => {
    connected = false;
    log.warn('Kafka producer disconnected');
  });
}

// O(1) from the caller's perspective: the awaited network I/O happens inside
// but publishTelemetry itself is only called fire-and-forget from the webhook
async function publishTelemetry(payload) {
  if (!connected) {
    log.error(`Producer not connected — message dropped (${payload.tenantID}/${payload.deviceID})`);
    return;
  }
  await producer.send({
    topic:    config.KAFKA_TOPIC,
    messages: [{ value: JSON.stringify(payload) }],
  });
}

async function closeKafkaProducer() {
  if (!connected) return;
  try {
    await producer.disconnect();
    log.info('Kafka producer disconnected gracefully');
  } catch (err) {
    log.error(`Kafka producer close error: ${err.message}`);
  } finally {
    connected = false;
  }
}

function getProducerStatus() {
  return connected ? 'connected' : 'disconnected';
}

module.exports = { initKafkaProducer, publishTelemetry, closeKafkaProducer, getProducerStatus };
