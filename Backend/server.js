'use strict';

const config = require('./config');
config.validate();

const express  = require('express');
const cors     = require('cors');
const mongoose = require('mongoose');

const { initWebSocket, closeWebSocket, getConnectedClients } = require('./websocket');
const { startBatchWriter, closeBatchProcessor, getCircuitBreakerState } = require('./batch-processor');
const { initKafkaConsumer, closeKafkaConsumer, getKafkaStatus } = require('./kafka-consumer');
const { initKafkaProducer, closeKafkaProducer }                = require('./kafka-producer');
const { createLogger } = require('./logger');
const rateLimiter      = require('./middleware/rateLimiter');

const log = createLogger('Server');
const app = express();

app.use(cors());
app.use(express.json());



app.use('/api', rateLimiter({ windowMs: 60_000, max: 100 }));

// Routes
app.use('/api/tenants',   require('./routes/tenants'));
app.use('/api/devices',   require('./routes/devices'));
app.use('/api/telemetry', require('./routes/telemetry'));
app.use('/webhook',       require('./routes/emqx-webhook'));


app.get('/health', (req, res) => {
  const mongoStates = ['disconnected', 'connected', 'connecting', 'disconnecting'];
  const services = {
    mongodb:   mongoStates[mongoose.connection.readyState] ?? 'unknown',
    influxdb:  getCircuitBreakerState(), // CLOSED = healthy, OPEN = down
    kafka:     getKafkaStatus(),
    websocket: getConnectedClients(),
  };

  const healthy = services.mongodb === 'connected' && services.influxdb !== 'OPEN';
  res.status(healthy ? 200 : 503).json({
    status:    healthy ? 'ok' : 'degraded',
    timestamp: new Date().toISOString(),
    services,
  });
});


app.use((req, res) => res.status(404).json({ error: 'Route not found' }));


app.use((err, req, res, _next) => {
  log.error(`Express error on ${req.method} ${req.path}: ${err.message}`);
  res.status(500).json({ error: 'Internal server error' });
});


let httpServer;

async function main() {

  await mongoose.connect(config.MONGO_URI, { serverSelectionTimeoutMS: 10_000 });
  log.info('MongoDB connected');

  httpServer = app.listen(config.PORT, () =>
    log.info(`REST API listening on http://localhost:${config.PORT}`)
  );


  initWebSocket(config.WS_PORT);
  startBatchWriter();
  await initKafkaProducer();
  await initKafkaConsumer();
}


async function shutdown(signal) {
  log.info(`${signal} received — shutting down gracefully`);

  httpServer?.close();
  await closeWebSocket();
  await closeKafkaProducer();
  await closeKafkaConsumer();
  await closeBatchProcessor();
  await mongoose.connection.close();

  log.info('Shutdown complete');
  process.exit(0);
}

process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));


process.on('unhandledRejection', (reason) => {
  log.error(`Unhandled rejection: ${reason}`);
  shutdown('unhandledRejection').catch(() => process.exit(1));
});

main().catch(err => {
  log.error(`Startup failed: ${err.message}`);
  process.exit(1);
});
