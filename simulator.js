'use strict';

const fs   = require('fs');
const path = require('path');
const mqtt = require('mqtt');

const BROKER   = process.env.EMQX_BROKER   || 'vfc9ad18.ala.eu-central-1.emqxsl.com';
const PORT     = parseInt(process.env.EMQX_PORT, 10) || 8883;
const USERNAME = process.env.EMQX_USERNAME || 'User';
const PASSWORD = process.env.EMQX_PASSWORD || 'Pass@12345';
const CA_PATH  = process.env.EMQX_CA_PATH;

const options = {
  username:        USERNAME,
  password:        PASSWORD,
  clientId:        `simulator-${process.pid}`,
  keepalive:       60,
  connectTimeout:  10_000,
  reconnectPeriod: 3_000,
  clean:           true,
};

// Load custom CA cert if provided; otherwise rely on system CA store (Let's Encrypt is trusted by default)
if (CA_PATH) {
  options.ca = fs.readFileSync(path.resolve(CA_PATH));
}

const client = mqtt.connect(`mqtts://${BROKER}:${PORT}`, options);

const tenantID = 'tenant_A_123';
const deviceID = 'sensor_node_01';

client.on('connect', () => {
  console.log(`Connected to EMQX Cloud at ${BROKER}:${PORT}. Publishing at 100 msg/sec...`);

  setInterval(() => {
    const payload = {
      tenantID,
      deviceID,
      temperature: (Math.random() * (85 - 20) + 20).toFixed(2),
      humidity:    (Math.random() * (90 - 30) + 30).toFixed(2),
      timestamp:   new Date().toISOString(),
    };

    client.publish(
      `telemetry/${tenantID}/${deviceID}`,
      JSON.stringify(payload),
      { qos: 0 }
    );
  }, 10);
});

client.on('error',     err => console.error('MQTT Error:', err.message));
client.on('reconnect', ()  => console.log('Reconnecting to EMQX Cloud...'));
client.on('close',     ()  => console.log('Connection closed'));
