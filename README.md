# High-Throughput Edge-to-Cloud IoT Pipeline

A SaaS SCADA micro-slice: MQTT telemetry ingested at 100 msg/sec from EMQX Cloud Serverless, bridged to Aiven Kafka via a webhook, consumed and persisted via a non-blocking pipeline to InfluxDB, streamed live to a multi-tenant Angular dashboard.

![Node.js](https://img.shields.io/badge/Node.js-v20%2B-339933?logo=nodedotjs&logoColor=white)
![Angular](https://img.shields.io/badge/Angular-21-DD0031?logo=angular&logoColor=white)
![InfluxDB](https://img.shields.io/badge/InfluxDB-2.7-22ADF6?logo=influxdb&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-47A248?logo=mongodb&logoColor=white)
![EMQX](https://img.shields.io/badge/EMQX-Cloud%20Serverless-660066?logo=mqtt&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Aiven-231F20?logo=apachekafka&logoColor=white)

---

## Architecture

```
simulator.js  (100 msg/sec, mqtts://)
      │  MQTT publish  telemetry/{tenantID}/{deviceID}
      ▼
EMQX Cloud Serverless  (eu-central-1, deployment-vfc9ad18)
      │  Rule Engine: SELECT * FROM "telemetry/#"
      │  Action: HTTP POST to webhook URL
      ▼
ngrok tunnel  ◄── DEV ONLY — production replaces with a stable public endpoint
      │
      ▼
Backend/routes/emqx-webhook.js  (POST /webhook/emqx-telemetry)
      │  validate X-EMQX-Secret header, respond 200 immediately, fire-and-forget
      ▼
Backend/kafka-producer.js  (kafkajs + mTLS client certs)
      │
      ▼
Aiven Kafka  topic: "telemetry"
      │
      ▼
Backend/kafka-consumer.js  (consumer group: iot-backend-consumer)
      │  O(1) dispatch — synchronous, no await, no DB calls
      ├─→ addToBuffer()  ──1-second flush──►  batch-processor.js  ──►  InfluxDB
      └─→ broadcast()    ──────────────────►  WebSocket (tenant-filtered)
                                                      │
                                                      ▼
                                              Angular Dashboard (RxJS bufferTime)

REST API :3000  ──CRUD──►  MongoDB Atlas  (Tenants / Devices)
```

The webhook hop is the structural hinge of this architecture. Instead of a direct MQTT subscriber inside the backend, EMQX forwards each matching message to the backend via HTTP. The backend validates it, responds with `200 OK` immediately so EMQX never back-pressures or retries, then publishes to Kafka asynchronously. Kafka is the durable store; the consumer on the other side is a simple O(1) dispatcher identical in structure to the old MQTT handler.

---

## EMQX → Kafka Bridge via Webhook

### Why not the native EMQX Kafka connector?

EMQX Cloud Serverless includes a native Kafka Producer connector in its Rule Engine. We attempted to use it — it would have been the simplest possible bridge with no extra hop. It failed with a hard authentication mismatch:

```
{tls_alert,{certificate_required, ...}}
```

**Root cause:** EMQX Cloud Serverless's Kafka connector supports only Basic Auth (username + password). Aiven Kafka enforces mTLS client certificates exclusively — it does not offer a Basic Auth path. These two requirements are incompatible at the managed-service level with no configuration workaround.

### The solution: backend as the mTLS bridge

The backend already had working mTLS Kafka connectivity via `kafkajs` in the consumer (`kafka-consumer.js`). Rather than fighting the managed-service auth mismatch, the same `kafkajs` producer pattern was reused:

```
EMQX Rule Engine
  → HTTP action  (EMQX sends, no client-cert requirement on EMQX's side)
  → emqx-webhook.js  (validates shared-secret header)
  → kafka-producer.js  (kafkajs + the same mTLS certs already used by the consumer)
  → Aiven Kafka
```

This is a defensible architectural decision: the mTLS capability was already present and tested in the consumer. Adding a producer reuses the same three cert files (`service.key`, `service.cert`, `ca.pem`) with zero new dependencies. The backend acts as a stateless bridge — receive, validate, forward. The existing `kafkajs` retry and reconnect logic handles transient Kafka availability issues.

---

## Kafka as the Durable Buffer

In the old architecture, an in-memory bounded array (`MAX_BUFFER = 10,000`) was the only mechanism preventing unbounded memory growth if InfluxDB slowed down or went offline. That buffer had no durability — a process restart lost everything queued inside it.

Kafka now provides what the old buffer never could: **durability across restarts**. Messages are retained on the broker until the consumer commits an offset. If the backend crashes mid-batch, it resumes from the last committed offset on restart — no data gap in InfluxDB.

The in-memory buffer in `batch-processor.js` still exists and is still executed on every message, but its role has shifted:

| | Old architecture | New architecture |
|---|---|---|
| **Primary backpressure** | In-memory buffer — only protection against MQTT outpacing InfluxDB | Kafka — messages persist on the broker independent of backend state |
| **Durability on restart** | None — buffer was lost | Full — consumer resumes from last committed offset |
| **In-memory buffer role** | Primary buffer between ingestion and InfluxDB | InfluxDB-side staging only — protects against InfluxDB being slow or temporarily down |
| **Overflow consequence** | Data permanently lost | Data dropped from staging; log message instructs resetting consumer group offset in Kafka to replay |

The circuit breaker around InfluxDB writes is still active: repeated write failures open the breaker, halting flushes during an outage rather than hammering a downed database. If InfluxDB recovers before the 10k staging buffer fills, no data is lost. If it is down long enough to fill the buffer, data is droppable from staging but replayable from Kafka within Kafka's retention window.

---

## Why The Event Loop Never Blocks

The Kafka message handler has exactly the same structure as the old MQTT handler — two O(1) operations and a synchronous return:

```js
// kafka-consumer.js — eachMessage callback
eachMessage: ({ topic, partition, message }) => {
  const payload = JSON.parse(message.value.toString());

  addToBuffer(payload);  // array push — O(1), <0.01 ms
  broadcast(payload);    // WebSocket fan-out — O(1) per connected client, <0.01 ms
  // no await, no DB call — returns synchronously
}

// batch-processor.js — decoupled flush, runs on its own event-loop tick
setInterval(() => flush(), 1000);
```

This is a producer-consumer split: ingestion (fast, synchronous) is decoupled from persistence (slower, async, batched). **100 InfluxDB writes/sec collapse into 1 batch write/sec** — a 100× reduction in write connection pressure. The event loop is never blocked by I/O inside the hot path, which means REST API endpoints and WebSocket connections stay responsive regardless of InfluxDB write latency or Kafka publish latency.

---

## Why Two Databases

| Database | Role | Why this one |
|---|---|---|
| **MongoDB Atlas** | Tenant + device configuration | Document-oriented, low write frequency, CRUD access pattern. Schema flexibility for tenant metadata without a fixed migration path. |
| **InfluxDB** | Telemetry (temperature, humidity) | Optimized for time-series: sustained high write throughput, efficient range and aggregation queries, native timestamp indexing. `tenantID` is stored as an indexed tag so tenant-scoped time-range queries stay O(log n) at scale. |

Putting telemetry in MongoDB would mean either unbounded collection growth with no native downsampling, or reimplementing what InfluxDB provides natively. Putting config data in InfluxDB would mean shoehorning relational data into a measurement/tag model it was not designed for. The split matches the access pattern of each data class.

---

## Multi-Tenancy

Tenant isolation is enforced at every layer, not just the API:

- **MQTT topic:** `telemetry/{tenantID}/{deviceID}` — tenant boundary starts at the message source
- **MongoDB:** every query filtered by `tenantID`
- **InfluxDB:** `tenantID` stored as an indexed tag, not a field — tenant-scoped time-range queries stay fast at scale
- **WebSocket:** broadcasts filtered server-side so a client only ever receives its own tenant's data

Next isolation step at scale: per-tenant InfluxDB buckets for hard storage-level separation instead of tag-level filtering.

---

## Known Trade-offs

| Decision | Accepted cost |
|---|---|
| Webhook bridge instead of native EMQX Kafka connector | Extra network hop (EMQX → backend → Kafka vs. EMQX → Kafka directly). Latency is negligible at 100 msg/sec; the alternative was blocked by a hard auth incompatibility between EMQX Cloud Serverless (Basic Auth only) and Aiven Kafka (mTLS only). |
| ngrok tunnel for local dev webhook | **Real operational fragility:** if the ngrok tunnel restarts, the URL changes. The EMQX Rule Engine HTTP action URL must then be updated manually in the EMQX Cloud console before the pipeline will forward messages again. Every backend restart that triggers a new ngrok session breaks the pipeline until the URL is updated. This is a dev-only workaround; production requires a stable public endpoint. |
| In-memory staging buffer for InfluxDB | If InfluxDB is persistently down and the 10k staging buffer fills, messages consumed from Kafka are dropped from staging. Those messages remain in Kafka within its retention window and can be replayed by resetting the consumer group offset. |
| Header-based tenant ID, not JWT | `x-tenant-id` is not cryptographically verified — adequate for demo scope only. |

---

## Prerequisites

- **Node.js** v20+
- **InfluxDB** 2.7 — local install or Docker; org `my-org`, bucket `telemetry`
- **MongoDB Atlas** — free tier works; connection string goes in `.env`
- **EMQX Cloud Serverless** — free tier available. Create a deployment, create a Rule Engine rule (`SELECT * FROM "telemetry/#"`), add an HTTP action pointing at your ngrok URL + `/webhook/emqx-telemetry`, and set a custom request header `X-EMQX-Secret` matching `EMQX_WEBHOOK_SECRET` in your `.env`. [EMQX Cloud docs →](https://docs.emqx.com/en/cloud/latest/)
- **Aiven Kafka** — free trial available. Create a Kafka service, download the three mTLS cert files (`service.key`, `service.cert`, `ca.pem`) from the service Overview tab, place them in `Backend/certs/`. Ensure `Backend/certs/` is in `.gitignore` — do not commit private keys. [Aiven Kafka docs →](https://docs.aiven.io/docs/products/kafka)
- **ngrok** — dev-only. `ngrok http 3000` exposes the backend for EMQX to reach. Update the EMQX Rule Engine action URL each time the tunnel restarts. [ngrok docs →](https://ngrok.com/docs)

---

## How to Run Locally

```bash
# 1. InfluxDB
./influxd.exe
# First run: open localhost:8086 → create org "my-org", bucket "telemetry", copy the token

# 2. ngrok  (DEV ONLY — exposes :3000 so EMQX Cloud can POST to the webhook)
ngrok http 3000
# Copy the https://9242-154-178-217-83.ngrok-free.app
# Paste it into EMQX Cloud → Rule Engine → your rule → HTTP action URL:
#  https://9242-154-178-217-83.ngrok-free.app/webhook/emqx-telemetry
# Do this every time the ngrok tunnel restarts.

# 3. Backend
cd Backend
npm install
# Edit .env — see template below
node server.js

# 4. Seed tenant + device  (required — simulator hard-codes tenant_A_123 / sensor_node_01)
curl -X POST localhost:3000/api/tenants \
  -H "Content-Type: application/json" \
  -d '{"tenantID":"tenant_A_123","name":"Acme","email":"acme@example.com"}'

curl -X POST localhost:3000/api/devices \
  -H "Content-Type: application/json" \
  -H "x-tenant-id: tenant_A_123" \
  -d '{"deviceID":"sensor_node_01"}'

# 5. Simulator  (connects to EMQX Cloud Serverless via mqtts://)
node simulator.js

# 6. Frontend
cd frontend && npm install && ng serve
# → http://localhost:4200
```

`.env` template (fill in real values from your EMQX Cloud and Aiven dashboards):

```env
# Server
PORT=3000
WS_PORT=8080

# MongoDB
MONGO_URI=mongodb+srv://yousefhamdy1141_db_user:password12345@cluster0.oxewkvg.mongodb.net/iot_db?retryWrites=true&w=majority&appName=Cluster0

# InfluxDB
INFLUX_URL=http://localhost:8086
INFLUX_TOKEN=my-secret-token-123
INFLUX_ORG=my-org
INFLUX_BUCKET=telemetry

# Suppress kafkajs v2 partitioner migration warning
KAFKAJS_NO_PARTITIONER_WARNING=1

# Aiven Kafka (mTLS — no username/password, auth is via client certificates)
KAFKA_BROKERS=kafka-3f85027e-yousefhamdy1141-9eac.d.aivencloud.com:28602
KAFKA_TOPIC=telemetry
KAFKA_SSL_KEY_PATH=./certs/service.key
KAFKA_SSL_CERT_PATH=./certs/service.cert
KAFKA_SSL_CA_PATH=./certs/ca.pem

# EMQX Webhook shared secret — EMQX sends this in every X-EMQX-Secret header
# Change this to any random string; must match what you set in the EMQX action header
EMQX_WEBHOOK_SECRET=IgnovaIoT-secret-2026

```

Verify: `curl localhost:3000/health` — all four services should show connected/CLOSED:

```json
{
  "status": "ok",
  "services": {
    "mongodb": "connected",
    "influxdb": "CLOSED",
    "kafka": "connected",
    "websocket": { "total": 0, "byTenant": {} }
  }
}
```

---

## API

| Method | Endpoint | Scope |
|---|---|---|
| `GET/POST` | `/api/tenants` | global |
| `GET/POST/PATCH` | `/api/devices` | tenant (`x-tenant-id` header) |
| `GET` | `/api/telemetry?start=&limit=` | tenant |
| `GET` | `/api/telemetry/latest` | tenant |
| `GET` | `/health` | service status |
| `POST` | `/webhook/emqx-telemetry` | internal — EMQX Rule Engine only |

Rate limit: 100 req/min/IP on `/api/*`. The `/webhook` route is not rate-limited by the middleware — it is protected by the `X-EMQX-Secret` header check instead.

---

## Frontend

- `webSocket()` (rxjs) → `bufferTime(100ms)` → `share()`: one connection, 100 frames/sec collapsed to 10 renders/sec
- `OnPush` change detection, `takeUntilDestroyed()` — no leaked subscriptions
- HMI widget: SVG thermometer, color-coded by temperature threshold
- Chart: 60-point rolling window via `push`/`shift` on the source buffer; a shallow-copied array is passed to ngx-charts per update so its reference-based diffing renders incrementally instead of resetting the scale

---

## Production Roadmap

| Area | Current state | Production target |
|---|---|---|
| Broker | **EMQX Cloud Serverless — live** (deployment-vfc9ad18, eu-central-1, Rule Engine verified working) | EMQX dedicated tier or cluster for higher message-rate SLAs and static IPs |
| Webhook ingress | ngrok tunnel (dev-only, URL changes on restart) | Stable public HTTPS endpoint — load balancer or reverse proxy in front of the backend |
| Buffer / durability | **Aiven Kafka — live** (mTLS, consumer group offset tracking, survives restarts) | Kafka with replication factor > 1 for high availability; retention policy tuned to replay window requirements |
| Auth | `x-tenant-id` header (unverified) | JWT / OAuth2 signed claims |
| Tenant isolation | Row/tag filtering in shared MongoDB + InfluxDB | Per-tenant InfluxDB buckets for hard storage-level separation |
| Circuit breaker / rate limit | In-process state (resets on restart) | Redis-backed, shared across replicas |
| Observability | Console logs | OpenTelemetry → Grafana / Loki |

The Broker and Buffer rows are no longer purely aspirational — EMQX Cloud and Aiven Kafka are both live and verified working. The remaining gap is operational hardening: the deployment is single-node/single-partition with a fragile local ingress tunnel. The production target is the same architecture with HA replication, a stable public endpoint, and shared-state infrastructure for the cross-cutting concerns.
