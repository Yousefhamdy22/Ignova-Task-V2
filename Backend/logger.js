'use strict';

const LEVELS = { error: 0, warn: 1, info: 2, debug: 3 };
const current = LEVELS[process.env.LOG_LEVEL?.toLowerCase()] ?? LEVELS.info;

function log(level, namespace, message) {
  if (LEVELS[level] > current) return;
  const ts    = new Date().toISOString();
  const label = level.toUpperCase().padEnd(5);
  console.log(`${ts} [${label}] [${namespace}] ${message}`);
}

function createLogger(namespace) {
  return {
    error: msg => log('error', namespace, msg),
    warn:  msg => log('warn',  namespace, msg),
    info:  msg => log('info',  namespace, msg),
    debug: msg => log('debug', namespace, msg),
  };
}

module.exports = { createLogger };
