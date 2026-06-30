'use strict';

const mongoose = require('mongoose');

const tenantSchema = new mongoose.Schema({
  tenantID: {
    type:     String,
    required: true,
    unique:   true,
    trim:     true,
    match:    [/^[a-zA-Z0-9_-]{1,64}$/, 'Invalid tenantID format'],
  },
  name:   { type: String, required: true, trim: true },
  email:  { type: String, required: true, trim: true },
  active: { type: Boolean, default: true },
}, { timestamps: true });

module.exports = mongoose.model('Tenant', tenantSchema);
