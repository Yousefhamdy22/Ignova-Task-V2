'use strict';

const router = require('express').Router();
const Tenant = require('../models/tenant.model');

const VALID_TENANT_ID = /^[a-zA-Z0-9_-]{1,64}$/;

function validateBody(body, res) {
  const { tenantID, name, email } = body;
  if (!tenantID || !VALID_TENANT_ID.test(tenantID)) {
    res.status(400).json({ error: 'tenantID required — 1–64 chars: letters, digits, _ or -' });
    return false;
  }
  if (!name || typeof name !== 'string' || !name.trim()) {
    res.status(400).json({ error: 'name is required' });
    return false;
  }
  if (!email || typeof email !== 'string' || !email.trim()) {
    res.status(400).json({ error: 'email is required' });
    return false;
  }
  return true;
}


router.get('/', async (req, res) => {
  try {
    const tenants = await Tenant.find({}, '-__v').lean();
    res.json(tenants);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


router.get('/:tenantID', async (req, res) => {
  try {
    const tenant = await Tenant.findOne({ tenantID: req.params.tenantID }, '-__v').lean();
    if (!tenant) return res.status(404).json({ error: 'Tenant not found' });
    res.json(tenant);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


router.post('/', async (req, res) => {
  if (!validateBody(req.body, res)) return;
  try {
    const tenant = await Tenant.create({
      tenantID: req.body.tenantID.trim(),
      name:     req.body.name.trim(),
      email:    req.body.email.trim(),
    });
    res.status(201).json(tenant);
  } catch (err) {
    if (err.code === 11000) {
      return res.status(409).json({ error: `Tenant '${req.body.tenantID}' already exists` });
    }
    res.status(400).json({ error: err.message });
  }
});


router.patch('/:tenantID/active', async (req, res) => {
  if (typeof req.body.active !== 'boolean') {
    return res.status(400).json({ error: '"active" must be a boolean' });
  }
  try {
    const tenant = await Tenant.findOneAndUpdate(
      { tenantID: req.params.tenantID },
      { active: req.body.active },
      { new: true, runValidators: true, projection: '-__v' }
    );
    if (!tenant) return res.status(404).json({ error: 'Tenant not found' });
    res.json(tenant);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

module.exports = router;
