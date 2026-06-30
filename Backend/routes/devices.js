'use strict';

const router = require('express').Router();
const Device = require('../models/device.model');
const tenantMiddleware = require('../middleware/tenant');


router.use(tenantMiddleware);


router.get('/', async (req, res) => {
  try {
    const devices = await Device.find({ tenantID: req.tenantID }, '-__v').lean();
    res.json(devices);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/:deviceID', async (req, res) => {
  try {
    const device = await Device
      .findOne({ tenantID: req.tenantID, deviceID: req.params.deviceID }, '-__v')
      .lean();
    if (!device) return res.status(404).json({ error: 'Device not found' });
    res.json(device);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


router.post('/', async (req, res) => {
  const { deviceID, type, location } = req.body;

  if (!deviceID || typeof deviceID !== 'string' || !deviceID.trim()) {
    return res.status(400).json({ error: 'deviceID is required' });
  }

  try {
    const device = await Device.create({
      deviceID:  deviceID.trim(),
      tenantID:  req.tenantID,        
      type:      (type     || 'temperature_humidity').trim(),
      location:  (location || '').trim(),
    });
    res.status(201).json(device);
  } catch (err) {
    if (err.code === 11000) {
      return res.status(409).json({
        error: `Device '${deviceID}' already exists for tenant '${req.tenantID}'`,
      });
    }
    res.status(400).json({ error: err.message });
  }
});


router.patch('/:deviceID', async (req, res) => {
  const update = {};
  if (req.body.location !== undefined) update.location = String(req.body.location).trim();
  if (req.body.active   !== undefined) update.active   = Boolean(req.body.active);

  if (Object.keys(update).length === 0) {
    return res.status(400).json({ error: 'Provide at least one field to update: location, active' });
  }

  try {
    const device = await Device.findOneAndUpdate(
      { tenantID: req.tenantID, deviceID: req.params.deviceID },
      update,
      { new: true, runValidators: true, projection: '-__v' }
    );
    if (!device) return res.status(404).json({ error: 'Device not found' });
    res.json(device);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

module.exports = router;
