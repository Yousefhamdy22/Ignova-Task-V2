'use strict';

const { WebSocketServer, WebSocket } = require('ws');
const { createLogger } = require('./logger');

const log = createLogger('WS');
const HEARTBEAT_MS = 30_000;
const VALID_TENANT = /^[a-zA-Z0-9_-]{1,64}$/;

let wss = null;

function initWebSocket(port) {
  wss = new WebSocketServer({ port });

  wss.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      log.error(`Port ${port} already in use. Kill the old process first.`);
      process.exit(1);
    }
    log.error(`Server error: ${err.message}`);
  });

  wss.on('connection', (ws, req) => {
  
    const url      = new URL(req.url, 'http://localhost');
    const tenantID = url.searchParams.get('tenantID') || '';

    if (!VALID_TENANT.test(tenantID)) {
      log.warn(`Rejected connection — invalid or missing tenantID (ip: ${req.socket.remoteAddress})`);
      ws.close(1008, 'tenantID query param required: ws://host:port?tenantID=your_id');
      return;
    }

    ws.tenantID = tenantID;
    ws.isAlive  = true;
    const ip = req.socket.remoteAddress;

    log.info(`Client connected — tenant: ${tenantID}, ip: ${ip}`);

    ws.on('pong',    ()    => { ws.isAlive = true; });
    ws.on('close',   ()    => log.info(`Client disconnected — tenant: ${tenantID}, ip: ${ip}`));
    ws.on('error',   err   => log.error(`Client error (tenant: ${tenantID}): ${err.message}`));
    // This is a server-push channel — ignore any messages the client sends
    ws.on('message', ()    => ws.close(1003, 'This is a read-only push channel'));
  });


  const heartbeat = setInterval(() => {
    wss.clients.forEach(ws => {
      if (!ws.isAlive) {
        log.debug(`Terminating dead connection (tenant: ${ws.tenantID})`);
        ws.terminate();
        return;
      }
      ws.isAlive = false;
      ws.ping();
    });
  }, HEARTBEAT_MS);

  wss.on('close', () => clearInterval(heartbeat));
  log.info(`WebSocket server listening on port ${port}`);
}


function broadcast(data) {
  if (!wss) return 0;
  const { tenantID } = data;
  if (!tenantID) return 0;

  const message = JSON.stringify(data);
  let sent = 0;

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client.tenantID === tenantID) {
      // Pass an error callback so a failed send doesn't throw into the event loop
      client.send(message, err => {
        if (err) log.error(`Send error (tenant: ${tenantID}): ${err.message}`);
      });
      sent++;
    }
  });

  return sent;
}

function getConnectedClients() {
  if (!wss) return { total: 0, byTenant: {} };
  const byTenant = {};
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      byTenant[ws.tenantID] = (byTenant[ws.tenantID] || 0) + 1;
    }
  });
  return { total: wss.clients.size, byTenant };
}

function closeWebSocket() {
  return new Promise(resolve => {
    if (!wss) return resolve();
    wss.close(resolve);
  });
}

module.exports = { initWebSocket, broadcast, closeWebSocket, getConnectedClients };
