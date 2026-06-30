'use strict';

/**
 * Quick test: publishes 10 messages directly to Aiven Kafka.
 * Use this to verify the full Kafka → kafka-consumer → WebSocket → frontend flow
 * without needing the EMQX bridge to be configured first.
 *
 * Run from Backend/: node test-publish.js
 */

const fs             = require('fs');
const { Kafka }      = require('kafkajs');
require('dotenv').config();

const kafka = new Kafka({
  clientId: 'test-publisher',
  brokers:  process.env.KAFKA_BROKERS.split(',').map(b => b.trim()),
  ssl: {
    rejectUnauthorized: true,
    ca:   [fs.readFileSync(process.env.KAFKA_SSL_CA_PATH)],
    cert: fs.readFileSync(process.env.KAFKA_SSL_CERT_PATH),
    key:  fs.readFileSync(process.env.KAFKA_SSL_KEY_PATH),
  },
});

async function run() {
  const producer = kafka.producer();
  await producer.connect();
  console.log('Connected to Kafka — sending 10 test messages...');

  for (let i = 0; i < 10; i++) {
    const payload = {
      tenantID:    'tenant_A_123',
      deviceID:    'sensor_node_01',
      temperature: (Math.random() * (85 - 20) + 20).toFixed(2),
      humidity:    (Math.random() * (90 - 30) + 30).toFixed(2),
      timestamp:   new Date().toISOString(),
    };

    await producer.send({
      topic:    process.env.KAFKA_TOPIC,
      messages: [{ value: JSON.stringify(payload) }],
    });

    console.log(`Sent [${i + 1}/10]: temp=${payload.temperature} hum=${payload.humidity}`);
    await new Promise(r => setTimeout(r, 200));
  }

  await producer.disconnect();
  console.log('Done. Check your backend logs for [Kafka] and [Batch] lines, and the dashboard for live data.');
}

run().catch(err => { console.error(err.message); process.exit(1); });
