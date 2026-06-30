'use strict';


const VALID = /^[a-zA-Z0-9_-]{1,64}$/;

function tenantMiddleware(req, res, next) {
  const raw = (req.headers['x-tenant-id'] || req.query.tenantID || '').trim();

  if (!raw) {
    return res.status(400).json({
      error: 'tenantID is required (header: x-tenant-id  or query: ?tenantID=)',
    });
  }

  if (!VALID.test(raw)) {
    return res.status(400).json({
      error: 'tenantID must be 1–64 characters: letters, digits, underscore, or hyphen',
    });
  }

  req.tenantID = raw;
  next();
}

module.exports = tenantMiddleware;
