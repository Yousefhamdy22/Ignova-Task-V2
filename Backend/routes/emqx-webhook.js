'use strict';

const express              = require('express');
const { publishTelemetry } = require('../kafka-producer');
const { createLogger }     = require('../logger');

const router = express.Router();
const log    = createLogger('Webhook');

// Set once at startup — if blank/absent, secret check is skipped (dev convenience)
const WEBHOOK_SECRET = process.env.EMQX_WEBHOOK_SECRET;

router.post('/emqx-telemetry', (req, res) => {
  // ── Auth: shared-secret header sent by EMQX on every request ────────────
  if (WEBHOOK_SECRET && req.headers['x-emqx-secret'] !== WEBHOOK_SECRET) {
    log.warn(`Unauthorized webhook request from ${req.ip}`);
    return res.status(401).json({ error: 'Unauthorized' });
  }

  // ── Validate ─────────────────────────────────────────────────────────────
  const payload = req.body;
  if (!payload || !payload.tenantID || !payload.deviceID) {
    log.warn(`Invalid payload — missing tenantID/deviceID: ${JSON.stringify(payload)}`);
    return res.status(400).json({ error: 'Missing tenantID or deviceID' });
  }

  // ── Respond immediately so EMQX does not retry or back-pressure ──────────
  res.status(200).json({ ok: true });

  // ── Fire-and-forget publish — errors logged, never block the response ────
  publishTelemetry(payload).catch(err =>
    log.error(`Kafka publish failed (${payload.tenantID}/${payload.deviceID}): ${err.message}`)
  );
});

module.exports = router;
