'use strict';

const { Point } = require('@influxdata/influxdb-client');
const influxClient     = require('./influx');
const config           = require('./config');
const { createLogger } = require('./logger');

const log = createLogger('Batch');

const writeApi = influxClient.getWriteApi(config.INFLUX_ORG, config.INFLUX_BUCKET, 'ms', {
  batchSize:     5000,
  flushInterval: 0,       
  maxRetries:    3,
  maxRetryTime:  30_000,
});


const CB_STATE = { CLOSED: 'CLOSED', OPEN: 'OPEN', HALF_OPEN: 'HALF_OPEN' };
const FAILURE_THRESHOLD = 3;
const RECOVERY_MS       = 30_000;

const cb = {
  state:       CB_STATE.CLOSED,
  failures:    0,
  nextAttempt: 0,

  canAttempt() {
    if (this.state === CB_STATE.CLOSED) return true;
    if (this.state === CB_STATE.OPEN && Date.now() >= this.nextAttempt) {
      this.state = CB_STATE.HALF_OPEN;
      log.warn('Circuit HALF_OPEN — testing InfluxDB recovery');
      return true;
    }
    return this.state === CB_STATE.HALF_OPEN;
  },

  onSuccess() {
    if (this.state !== CB_STATE.CLOSED) log.info('Circuit CLOSED — InfluxDB recovered');
    this.state    = CB_STATE.CLOSED;
    this.failures = 0;
  },

  onFailure() {
    this.failures++;
    if (this.failures >= FAILURE_THRESHOLD) {
      this.state       = CB_STATE.OPEN;
      this.nextAttempt = Date.now() + RECOVERY_MS;
      log.warn(`Circuit OPEN — InfluxDB down. Next attempt in ${RECOVERY_MS / 1000}s`);
    }
  },
};



const MAX_BUFFER  = 10_000;
let buffer        = [];
let droppedTotal  = 0;
let intervalHandle = null;

function addToBuffer(payload) {
  if (buffer.length >= MAX_BUFFER) {
    droppedTotal++;
    if (droppedTotal % 500 === 0) {
      log.warn(`Buffer full — dropped ${droppedTotal} total (circuit: ${cb.state}). Reset consumer group offset in Kafka to replay.`);
    }
    return;
  }
  buffer.push(payload);
}

async function flush() {
  if (buffer.length === 0) return;
  if (!cb.canAttempt()) return;


  const batch = buffer;
  buffer = [];

  let skipped = 0;
  for (const msg of batch) {
    const temp = parseFloat(msg.temperature);
    const hum  = parseFloat(msg.humidity);


    if (isNaN(temp) || isNaN(hum)) {
      skipped++;
      continue;
    }

    writeApi.writePoint(
      new Point('telemetry')
        .tag('tenantID', msg.tenantID)
        .tag('deviceID', msg.deviceID)
        .floatField('temperature', temp)
        .floatField('humidity',    hum)
        .timestamp(new Date(msg.timestamp))
    );
  }

  if (skipped > 0) log.warn(`Skipped ${skipped} records with NaN numeric values`);

  try {
    await writeApi.flush();
    cb.onSuccess();
    log.debug(`Flushed ${batch.length - skipped} records to InfluxDB`);
  } catch (err) {
    cb.onFailure();
    log.error(`InfluxDB flush failed (circuit: ${cb.state}): ${err.message}`);

    const merged = batch.concat(buffer);
    buffer = merged.length > MAX_BUFFER ? merged.slice(0, MAX_BUFFER) : merged;
  }
}

function startBatchWriter() {
  intervalHandle = setInterval(flush, 1000);
  log.info('Batch writer started — 1-second flush interval');
}

async function closeBatchProcessor() {
  if (intervalHandle) {
    clearInterval(intervalHandle);
    intervalHandle = null;
  }
  if (buffer.length > 0) {
    log.info(`Final flush of ${buffer.length} buffered records...`);
    await flush();
  }
  await writeApi.close().catch(err => log.error(`writeApi close error: ${err.message}`));
  log.info('Batch processor closed');
}

function getCircuitBreakerState() {
  return cb.state;
}

module.exports = { addToBuffer, startBatchWriter, closeBatchProcessor, getCircuitBreakerState };
