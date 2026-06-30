'use strict';

const router          = require('express').Router();
const influxClient    = require('../influx');
const config          = require('../config');
const tenantMiddleware = require('../middleware/tenant');


const queryApi = influxClient.getQueryApi(config.INFLUX_ORG);

router.use(tenantMiddleware);


function fluxSafe(value) {
  return String(value).replace(/[\\"\n\r]/g, '');
}

router.get('/', async (req, res) => {
  const startMin = Math.min(Math.max(parseInt(req.query.start,  10) || 60,  1), 1440);
  const limit    = Math.min(Math.max(parseInt(req.query.limit,  10) || 100, 1), 1000);

  // Build optional device filter — sanitised before interpolation
  const deviceLine = req.query.deviceID
    ? `|> filter(fn: (r) => r["deviceID"] == "${fluxSafe(req.query.deviceID)}")`
    : '';

  const query = `
    from(bucket: "${config.INFLUX_BUCKET}")
      |> range(start: -${startMin}m)
      |> filter(fn: (r) => r["_measurement"] == "telemetry")
      |> filter(fn: (r) => r["tenantID"] == "${req.tenantID}")
      ${deviceLine}
      |> limit(n: ${limit})
  `;

  try {
   
    const rows = await queryApi.collectRows(query, (row, meta) => meta.toObject(row));
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: `InfluxDB query failed: ${err.message}` });
  }
});


router.get('/latest', async (req, res) => {
  const query = `
    from(bucket: "${config.INFLUX_BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r["_measurement"] == "telemetry")
      |> filter(fn: (r) => r["tenantID"] == "${req.tenantID}")
      |> last()
  `;

  try {
    const rows = await queryApi.collectRows(query, (row, meta) => meta.toObject(row));
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: `InfluxDB query failed: ${err.message}` });
  }
});

module.exports = router;
