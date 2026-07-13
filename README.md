# AionQA Helper (Docker Engine Pool)

`aionqa-docker-engine-pool` is the helper service that manages crawler containers on a Docker host.

It is responsible for:

1. Registering a Docker engine with backend
2. Sending engine heartbeats and metrics
3. Polling for assigned jobs
4. Starting/stopping crawler containers
5. Reporting job lifecycle back to backend
6. Running helper-managed maintenance/update actions

## Requirements

- Node.js 20+ (if running outside Docker)
- Docker daemon access (`/var/run/docker.sock`)
- Valid `ENGINE_TOKEN` from AionQA admin/UI

## Quick Start (Local)

```bash
npm install
```

PowerShell example:

```powershell
$env:ENGINE_TOKEN="your-engine-token"
$env:BACKEND_URL="https://api.aionqa.com"
$env:POLL_INTERVAL="5000"
npm start
```

## Quick Start (Docker)

```bash
docker build -t aionqa-helper .
docker run -d \
  --name aionqa-helper \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e ENGINE_TOKEN=your-engine-token \
  -e BACKEND_URL=https://api.aionqa.com \
  -e POLL_INTERVAL=5000 \
  aionqa-helper
```

## Configuration

### Required

- `ENGINE_TOKEN`: auth token for register/poll/heartbeat/job reporting

### Common Optional

- `BACKEND_URL`: backend base URL (default: `https://api.aionqa.com`)
- `ENGINE_ID`: explicit engine identifier (auto-generated if omitted)
- `RUNNER_ID`: runner identifier used in logs/metrics (defaults to `ENGINE_ID`)
- `RUNNER_NAME`: if set, crawler containers use deterministic names (`aionqa-crawler-runner-{name}`)
- `POLL_INTERVAL`: job polling interval in milliseconds (default: `5000`)
- `DOCKER_SOCKET`: Docker socket path (default: `/var/run/docker.sock`)
- `IMAGE_CHECK_INTERVAL_MS`: managed image version check interval (default: `30000`)

### Advanced / Integration

- `SERVICE_KEY`: optional key used for backend image URL retrieval when enabled
- `HELPER_CONTAINER_NAME`, `HELPER_IMAGE`: override helper self-reference metadata
- `CURE_*`, `ADMIN_*`, `AI_SERVICE_*`, `OLLAMA_*`: optional feature toggles/integration overrides for cure/AI/container orchestration workflows

## Operational Notes

- The service is stateless and horizontally scalable.
- Each helper handles only jobs assigned to its engine ID/token scope.
- Self-update flows can pull new managed images and recreate containers when configured by backend.

## NPM Scripts

- `npm start` - start helper engine manager
- `npm run dev` - watch mode
- `npm test` - run Node test files in `src/*.test.js`
