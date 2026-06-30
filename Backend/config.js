'use strict';

require('dotenv').config();

const REQUIRED = [
  'MONGO_URI',
  'INFLUX_URL', 'INFLUX_TOKEN', 'INFLUX_ORG', 'INFLUX_BUCKET',
  'KAFKA_BROKERS', 'KAFKA_TOPIC',
  'KAFKA_SSL_KEY_PATH', 'KAFKA_SSL_CERT_PATH', 'KAFKA_SSL_CA_PATH',
];

function validate() {
  const missing = REQUIRED.filter(k => !process.env[k]);
  if (missing.length) {
    console.error(`[Config] FATAL — missing required env vars: ${missing.join(', ')}`);
    console.error('[Config] Check your .env file and restart.');
    process.exit(1);
  }
}

module.exports = {
  validate,
  PORT:                parseInt(process.env.PORT,    10) || 3000,
  WS_PORT:             parseInt(process.env.WS_PORT, 10) || 8080,
  MONGO_URI:           process.env.MONGO_URI,
  INFLUX_URL:          process.env.INFLUX_URL,
  INFLUX_TOKEN:        process.env.INFLUX_TOKEN,
  INFLUX_ORG:          process.env.INFLUX_ORG,
  INFLUX_BUCKET:       process.env.INFLUX_BUCKET,
  KAFKA_BROKERS:       process.env.KAFKA_BROKERS,
  KAFKA_TOPIC:         process.env.KAFKA_TOPIC,
  KAFKA_SSL_KEY_PATH:  process.env.KAFKA_SSL_KEY_PATH,
  KAFKA_SSL_CERT_PATH: process.env.KAFKA_SSL_CERT_PATH,
  KAFKA_SSL_CA_PATH:   process.env.KAFKA_SSL_CA_PATH,
};
