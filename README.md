# Docker Engine Pool

A lightweight Docker engine manager that connects to the AionQA backend and manages remote Docker containers for crawling jobs.

## Overview

This service allows you to run crawling jobs on remote Docker engines instead of ECS or local Docker. The engine:

1. Connects to the backend and registers itself
2. Polls for jobs assigned to it
3. Starts Docker containers with the crawler image
4. Monitors container lifecycle
5. Reports status back to the backend

## Configuration

### Environment Variables

- `BACKEND_URL` - Backend API URL (default: `http://back:4000`)
- `ENGINE_TOKEN` - Authentication token (required, generate in UI)
- `ENGINE_ID` - Unique engine identifier (auto-generated if not provided)
- `RUNNER_NAME` - Optional. If set, crawler containers are named `aionqa-crawler-runner-{name}` (sanitized) so the helper can start a container with a specific name and replace any existing one. Useful for one-runner-per-slot setups.
- `POLL_INTERVAL` - Poll interval in milliseconds (default: 5000)
- `DOCKER_SOCKET` - Docker socket path (default: `/var/run/docker.sock`)

**Self-update (no extra env required):** When an update is requested, the app pulls the new image and then recreates the container automatically. It discovers the compose project from the container’s own labels (`com.docker.compose.project.working_dir`) when run via Docker Compose, and runs `docker compose up -d --force-recreate`. If not run via Compose, it uses the Docker API to create and start a new container from the new image (same config) and exits so the old container stops. Optional: `COMPOSE_PROJECT_DIR` and `COMPOSE_FILE` override auto-discovery.

### Running

```bash
npm install
npm start
```

### Docker

The engine needs access to the Docker daemon. Run with:

```bash
docker run -d \
  --name docker-engine-pool \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e BACKEND_URL=https://api.aionqa.com \
  -e ENGINE_TOKEN=your_token_here \
  docker-engine-pool:latest
```

## Architecture

```
Backend (Job Queue)
  ↑ Register / Heartbeat
Docker Engine Pool
  ↑ Poll for Jobs
  ↓ Start Container
Docker Container (Crawler)
  ↑ Connect to Backend
  ↓ Execute Job
```

The engine is stateless and can be scaled horizontally. Each engine polls for jobs assigned to it and manages its own containers.







