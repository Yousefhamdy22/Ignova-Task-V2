'use strict';


const buckets = new Map();


setInterval(() => {
  const now = Date.now();
  for (const [key, rec] of buckets) {
    if (now > rec.resetAt) buckets.delete(key);
  }
}, 5 * 60_000).unref(); 

function rateLimiter({ windowMs = 60_000, max = 100 } = {}) {
  return (req, res, next) => {
    const key = req.ip || 'unknown';
    const now = Date.now();

    let rec = buckets.get(key);
    if (!rec || now > rec.resetAt) {
      rec = { count: 0, resetAt: now + windowMs };
      buckets.set(key, rec);
    }

    rec.count++;
    if (rec.count > max) {
      const retryAfter = Math.ceil((rec.resetAt - now) / 1000);
      res.set('Retry-After', retryAfter);
      return res.status(429).json({
        error: `Rate limit exceeded — retry after ${retryAfter}s`,
      });
    }

    next();
  };
}

module.exports = rateLimiter;
