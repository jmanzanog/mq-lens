# MQ Lens

MQ Lens (`com.jmanzano.mqlens`) is a local-first inspector for ActiveMQ Classic. It captures messages from explicit audit queues, stores them in SQLite, and shows broker status, destinations, topology and payload details in a Svelte UI.

It is designed for local debugging. It does not purge, delete, requeue, resend or consume business queues.

## Architecture

- Go backend with `net/http` and `chi`.
- STOMP audit consumers for `LENS.AUDIT.*` queues.
- Jolokia HTTP polling for broker and destination snapshots.
- Advisory topic monitoring for best-effort live topology changes.
- SQLite storage at `LENS_DB_PATH`.
- SSE at `/api/events` for live browser updates.
- Svelte + Vite frontend served by the Go binary.

## Run with Docker Compose

```sh
docker compose up --build
```

Open:

```text
http://localhost:8088
```

ActiveMQ Classic is bound locally:

```text
OpenWire: http://127.0.0.1:61616
STOMP:    127.0.0.1:61613
Console:  http://127.0.0.1:8161
```

## Send a Test Message

After Compose is running:

```sh
chmod +x scripts/send-test-message.sh
./scripts/send-test-message.sh ORDER.CREATED
```

The script uses local `nc` when available. If `nc` is not installed, it falls back to the running `activemq-classic` Docker container.

ActiveMQ copies the message from `ORDER.CREATED` to `LENS.AUDIT.ORDER.CREATED`. MQ Lens consumes only the audit copy.

## Configuration

Important environment variables:

```text
LENS_HTTP_ADDR=0.0.0.0:8080
LENS_MODE=hybrid
ACTIVEMQ_STOMP_ADDR=activemq:61613
ACTIVEMQ_STOMP_USER=admin
ACTIVEMQ_STOMP_PASSWORD=admin
ACTIVEMQ_JOLOKIA_URL=http://activemq:8161/api/jolokia
LENS_AUDIT_PREFIX=LENS.AUDIT.
LENS_AUDIT_QUEUES=ORDER.CREATED,PAYMENT.EVENTS
LENS_ADVISORY_ENABLED=true
LENS_DB_PATH=/data/mq-lens.db
LENS_MAX_BODY_BYTES=262144
LENS_MAX_MESSAGES=10000
LENS_RETENTION_HOURS=24
LENS_ENABLE_REDACTION=true
```

## The "Magic" Broker (ActiveMQ Plugin)

The easiest way to use MQ Lens is by using our opinionated ActiveMQ image which comes with the **MQ Lens Broker Plugin** pre-installed. 
This plugin automatically intercepts all messages sent to any business queue or Virtual Topic, and asynchronously routes a copy to `LENS.AUDIT.ALL` with the `LENS_OriginalDestination` header.
This eliminates the need to configure `activemq.xml` composites or manage `LENS_AUDIT_QUEUES`.

### Docker Compose Example

```yaml
services:
  # The Magic Broker
  message-broker:
    build: 
      context: ./
      dockerfile: docker/activemq/Dockerfile
    # Or use the pre-built image: image: ghcr.io/jmanzanog/mq-lens-activemq:latest
    ports:
      - "8161:8161"
      - "61616:61616"
      - "61613:61613"

  # MQ Lens Sidecar
  mq-lens:
    image: ghcr.io/jmanzanog/mq-lens:latest
    ports:
      - "8088:8080" # UI port
    environment:
      # By default LENS_AUDIT_QUEUES=ALL, so it connects to LENS.AUDIT.ALL automatically
      - ACTIVEMQ_STOMP_ADDR=message-broker:61613
      - ACTIVEMQ_JOLOKIA_URL=http://message-broker:8161/api/jolokia
    depends_on:
      - message-broker
```

## Manual Configuration (Without Plugin)

If you cannot replace your ActiveMQ image, you can still manually configure composites:

1. **Virtual Topic Mode**
   By setting `LENS_VIRTUAL_TOPIC_MODE=true` and `LENS_AUDIT_QUEUES=Foo,Bar`, the system assumes your audit destinations are derived from `VirtualTopic.<destination>`.
2. **Configuration Generator**
   Generate the required `activemq.xml` snippet using the CLI:
   ```sh
   mq-lens generate-activemq-config --output ./activemq-audit.xml
   ```

### Migrating from manual composite setup
If you are upgrading from a previous version where you used `LENS_AUDIT_QUEUES=Foo,Bar` manually:
1. Switch your `message-broker` image to `ghcr.io/jmanzanog/mq-lens-activemq:latest` (or build it locally).
2. Remove the `LENS_AUDIT_QUEUES` configuration from `mq-lens` (it now defaults to `ALL`).
3. You can safely remove your custom `activemq-audit.xml` volume mount.

### Important Notes
* **Redaction in Broker**: The plugin copies messages exactly as they are. This means the `LENS.AUDIT.ALL` queue in ActiveMQ stores the raw, unredacted message body. MQ Lens will redact the message upon reading it (if `LENS_ENABLE_REDACTION=true`), but the broker queue must be secured at the ActiveMQ level.
* **Reserved Properties**: The plugin uses the `LENS_` namespace for properties (e.g., `LENS_OriginalDestination`). Ensure your application does not use custom properties starting with `LENS_`.

## API

- `GET /api/health`
- `GET /api/broker/status`
- `GET /api/destinations`
- `GET /api/topology`
- `GET /api/messages`
- `GET /api/messages/{id}`
- `GET /api/messages/{id}/download`
- `GET /api/events`
- `POST /api/dev/send-test-message` when `LENS_DEV_TOOLS_ENABLED=true`

The dev send endpoint is disabled by default. It is intended for local smoke testing only.

## Local Development

Backend:

```sh
go run ./cmd/mq-lens
```

Frontend:

```sh
cd web
npm install
npm run dev
```

Build one binary with embedded UI:

```sh
make build
```

## Container Publishing

Every commit pushed to `main` publishes a Docker image to GitHub Container Registry after the `CI` workflow completes successfully:

```text
ghcr.io/<owner>/mq-lens:<YYYY.MM.DD.N>
```

Example:

```text
ghcr.io/jmanzanog/mq-lens:2026.06.02.1
```

The workflow also publishes:

```text
ghcr.io/<owner>/mq-lens:latest
ghcr.io/<owner>/mq-lens:main
ghcr.io/<owner>/mq-lens:sha-<short-commit-sha>
```

`latest` points to the most recent image published after a successful `CI` run on `main`.

The daily counter is calculated from existing GHCR tags for the same date, so the next successful publish on the same UTC day increments the final number.

## Dev Tools

Local message sending is disabled unless explicitly enabled:

```text
LENS_DEV_TOOLS_ENABLED=true
```

When enabled, the Settings view can send a JSON message to a configured destination through STOMP. Keep it disabled for normal inspection sessions.

## Safety Notes

- MQ Lens subscribes only to configured audit queues.
- No destructive broker endpoints are implemented.
- Payloads are size-limited and sensitive fields are redacted by default.
- The Docker Compose ports bind to `127.0.0.1`.
- UI auth is outside the MVP scope.

## Troubleshooting

- If Jolokia is unavailable, the app still runs and shows broker status as unavailable.
- If STOMP is disconnected, verify port `61613` and credentials.
- If no bodies appear, verify `activemq/conf/activemq.xml` contains composite queues for each audited destination.
- If topology does not live-update, Jolokia polling still refreshes snapshots; advisory topics are best-effort.
- If `scripts/send-test-message.sh` fails, verify Docker Compose is running or install `nc` and retry.
