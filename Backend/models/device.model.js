'use strict';

const mongoose = require('mongoose');

const deviceSchema = new mongoose.Schema({
  deviceID:  { type: String, required: true, trim: true },
  tenantID:  { type: String, required: true, trim: true },
  type:      { type: String, default: 'temperature_humidity', trim: true },
  location:  { type: String, default: '',                     trim: true },
  active:    { type: Boolean, default: true },
}, { timestamps: true });


deviceSchema.index({ tenantID: 1, deviceID: 1 }, { unique: true });

module.exports = mongoose.model('Device', deviceSchema);
