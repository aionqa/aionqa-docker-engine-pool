/**
 * Docker Engine Pool Manager
 * Connects to backend and manages Docker containers for crawling jobs
 */

import Docker from 'dockerode';
import axios from 'axios';
import { getMetricImageFields } from './metricImageFields.js';
import { hostname, cpus, platform, release, arch, networkInterfaces } from 'os';
import { randomUUID } from 'crypto';
import { readFileSync } from 'fs';

// Default to 'back' service name for docker-compose networks
// In Docker Compose, services are resolved by service name, not container name
const BACKEND_URL = process.env.BACKEND_URL || 'https://api.aionqa.com';
const ENGINE_TOKEN = process.env.ENGINE_TOKEN; // Token for authenticating with backend
const SERVICE_KEY = process.env.SERVICE_KEY; // Optional service key for image URL retrieval
const ENGINE_ID = process.env.ENGINE_ID || `docker-engine-${hostname()}-${randomUUID().substring(0, 8)}`;
/** Optional. If set, crawler containers are named aionqa-crawler-runner-{name} so the helper can replace existing container by name. */
const RUNNER_NAME = process.env.RUNNER_NAME || null;
/** For logging; backend runner_id comes from token. */
const RUNNER_ID = process.env.RUNNER_ID || ENGINE_ID;
const POLL_INTERVAL = parseInt(process.env.POLL_INTERVAL || '5000'); // 5 seconds
/** How often to compare local digests to the registry and run auto-pull/restart. Default 30s so updates apply soon after publish; override with IMAGE_CHECK_INTERVAL_MS if too chatty. */
const IMAGE_CHECK_INTERVAL_MS = parseInt(process.env.IMAGE_CHECK_INTERVAL_MS || '30000'); // 30 seconds
const DOCKER_SOCKET = process.env.DOCKER_SOCKET || '/var/run/docker.sock';
// ECR repositories are public - no AWS credentials needed
// Helper uses ENGINE_TOKEN (project credentials) for backend authentication only

/** Sanitize runner name for Docker container name segment: [a-zA-Z0-9][a-zA-Z0-9_.-]* (max 50 chars). */
function sanitizeRunnerName(name) {
  if (!name || typeof name !== 'string') return 'default';
  const s = name.trim().toLowerCase().replace(/[^a-z0-9_.-]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
  return (s || 'default').slice(0, 50);
}

// Validate engine token (required for job polling)
if (!ENGINE_TOKEN) {
  console.error('❌ ENGINE_TOKEN environment variable is required');
  console.error('    Generate a token in the UI: Settings > Docker Engines > Generate Token');
  process.exit(1);
}

// SERVICE_KEY is optional - if provided, will request image URL from backend
// If not provided, image URL comes from job response

console.log(`🐳 Docker Engine ID: ${ENGINE_ID}`);
if (RUNNER_NAME) console.log(`🏷️  Runner name: ${RUNNER_NAME} (crawler container will be aionqa-crawler-runner-${sanitizeRunnerName(RUNNER_NAME)})`);
console.log(`🔗 Backend URL: ${BACKEND_URL}`);
console.log(`⏱️  Poll interval: ${POLL_INTERVAL}ms`);
console.log(`🔌 Docker socket: ${DOCKER_SOCKET}`);
// Version info for image update verification (GIT_COMMIT baked in at docker build from CI)
const HELPER_GIT_COMMIT = (
  process.env.GIT_COMMIT ||
  process.env.HELPER_GIT_COMMIT ||
  process.env.SOURCE_COMMIT ||
  'unknown'
).trim();
const HELPER_VERSION = process.env.HELPER_VERSION || process.env.IMAGE_TAG || HELPER_GIT_COMMIT || 'unknown';
const HELPER_BUILD_DATE = process.env.BUILD_DATE || new Date().toISOString();
const HELPER_CONTAINER_NAME = (process.env.HELPER_CONTAINER_NAME || process.env.CONTAINER_NAME || 'aionqa-helper').toLowerCase().replace(/^\//, '');
const HELPER_IMAGE = process.env.HELPER_IMAGE || 'public.ecr.aws/k0f6y3u1/aionqa-docker-engine-pool:latest';
console.log(`📌 HELPER_GIT_COMMIT=${HELPER_GIT_COMMIT}`);
console.log(`📌 HELPER_GIT_REF=${String(process.env.GIT_REF || '').trim() || 'unknown'}`);
console.log(`🏷️  Helper Version: ${HELPER_VERSION}`);
console.log(`📅 Helper Build Date: ${HELPER_BUILD_DATE}`);

// Initialize Docker client
// Note: We'll set authconfig per-pull, not globally
const docker = new Docker({ socketPath: DOCKER_SOCKET });

/** Monorepo root inside cure: sibling repos (submodules) live here — never default to /app (cure image app dir). */
const DEFAULT_CURE_WORKSPACE_CONTAINER = '/workspace';

// Log driver that supports reading (docker logs). Required so we can read container logs
// when a container exits; daemon default may be journald/awslogs which don't support reading.
const LOG_CONFIG_JSON_FILE = {
  Type: 'json-file',
  Config: { 'max-size': '10m', 'max-file': '3' }
};

// Track running containers (containerId -> jobId)
const runningContainers = new Map();
let helperRestartPendingFromImageUpdate = false;
let helperPendingImageUrl = null;
/** Latest registry vs local digest comparison (refreshed on IMAGE_CHECK_INTERVAL_MS). Sent with heartbeats for admin UI. */
let cachedImageVersionChecks = null;

function normalizeSha(value) {
  return String(value || '').trim().replace(/^sha256:/, '').toLowerCase();
}

function imageIdMatchesSha(imageId, expectedSha) {
  const idNorm = normalizeSha(imageId);
  const shaNorm = normalizeSha(expectedSha);
  if (!idNorm || !shaNorm) return false;
  return idNorm === shaNorm || idNorm.startsWith(shaNorm) || shaNorm.startsWith(idNorm);
}

async function localImageDigestMatchesSha(imageRef, expectedSha) {
  const shaNorm = normalizeSha(expectedSha);
  if (!imageRef || !shaNorm) return false;
  try {
    const image = docker.getImage(imageRef);
    const info = await image.inspect();
    const repoDigests = Array.isArray(info?.RepoDigests) ? info.RepoDigests : [];
    const fromRepo = repoDigests.some((digestRef) => {
      const digest = String(digestRef || '').split('@').pop();
      return normalizeSha(digest) === shaNorm;
    });
    if (fromRepo) return true;
    // Some daemons leave RepoDigests empty for public-ecr :tag pulls; image Id is often the config digest
    // and matches ECR DescribeImages for single-arch images.
    if (info?.Id && imageIdMatchesSha(info.Id, shaNorm)) return true;
    return false;
  } catch {
    return false;
  }
}

function normalizeContainerName(name) {
  return String(name || '').replace(/^\//, '').trim();
}

function findContainerByName(containers, requestedName) {
  const target = normalizeContainerName(requestedName).toLowerCase();
  if (!target) return null;
  return containers.find(c =>
    (c.Names || []).some(n => {
      const normalized = normalizeContainerName(n).toLowerCase();
      return normalized === target || normalized.includes(target);
    })
  ) || null;
}

/** After `container.start()`, poll state for this long before treating a non-zero exit as immediate failure (cold start / post-deploy). */
const CRAWLER_IMMEDIATE_EXIT_GRACE_MS = parseInt(process.env.CRAWLER_IMMEDIATE_EXIT_GRACE_MS || '12000', 10);
const CRAWLER_IMMEDIATE_EXIT_POLL_MS = parseInt(process.env.CRAWLER_IMMEDIATE_EXIT_POLL_MS || '500', 10);
/** When the crawler exits during the startup grace window, retry create/start (ALB/ECS churn, cold image, brief daemon flakes). */
const CRAWLER_CONTAINER_START_ATTEMPTS = Math.max(1, parseInt(process.env.CRAWLER_CONTAINER_START_ATTEMPTS || '3', 10));
const CRAWLER_CONTAINER_START_RETRY_BASE_MS = Math.max(0, parseInt(process.env.CRAWLER_CONTAINER_START_RETRY_BASE_MS || '2500', 10));

/**
 * Heuristic: failures likely to succeed on a fresh container start (post-deploy race),
 * vs. permanent misconfig (missing token) or code errors we should not spin on.
 */
function isTransientCrawlerStartupExit({ exitCode, oom, logTail }) {
  if (oom) return false;
  const t = (logTail || '').toLowerCase();
  if (t.includes('runner_token') && t.includes('required')) return false;
  if (t.includes('cannot find module')) return false;
  if (t.includes('syntaxerror')) return false;
  if (t.includes('err_module_not_found')) return false;
  // Common crash codes during resource pressure / browser init; worth one or two fresh attempts.
  return exitCode === 1 || exitCode === 134 || exitCode === 137 || exitCode === 139;
}

/**
 * Decode Docker multiplexed log stream (8-byte header per chunk) to UTF-8 text.
 * @param {Buffer} buffer
 * @returns {string}
 */
function decodeDockerLogsBuffer(buffer) {
  if (!buffer || buffer.length === 0) return '';
  const buf = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);
  let out = '';
  let pos = 0;
  while (pos + 8 <= buf.length) {
    const size = buf.readUInt32BE(pos + 4);
    if (!Number.isFinite(size) || size < 0 || pos + 8 + size > buf.length) break;
    out += buf.subarray(pos + 8, pos + 8 + size).toString('utf8');
    pos += 8 + size;
  }
  const trimmed = out.trim();
  if (trimmed) return out;
  return buf.toString('utf8');
}

async function fetchCrawlerContainerLogTail(container, tailLines = 80) {
  try {
    const raw = await container.logs({ stdout: true, stderr: true, tail: tailLines, timestamps: false });
    const text = decodeDockerLogsBuffer(raw);
    const max = 4000;
    return text.length > max ? `${text.slice(-max)}\n…(truncated)` : text;
  } catch (e) {
    return `(logs unavailable: ${e.message || e})`;
  }
}

/**
 * Poll until the crawler container is running, exits, or grace elapses.
 * @returns {Promise<object>} Last `docker inspect` JSON
 */
async function waitForCrawlerStartupOrExit(container) {
  const deadline = Date.now() + Math.max(1000, CRAWLER_IMMEDIATE_EXIT_GRACE_MS);
  let last = await container.inspect();
  while (Date.now() < deadline) {
    last = await container.inspect();
    const st = last.State?.Status;
    if (st === 'running' || st === 'restarting') return last;
    if (st === 'exited' || st === 'dead') return last;
    await new Promise((r) => setTimeout(r, CRAWLER_IMMEDIATE_EXIT_POLL_MS));
  }
  return container.inspect();
}

/** Backend retry delays: 10s, 30s, 3min. Runners continue without interruption when backend restarts; only admin explicit cleanup clears state. */
const BACKEND_RETRY_DELAYS_MS = [10 * 1000, 30 * 1000, 3 * 60 * 1000];
const BACKEND_RETRY_ATTEMPTS = 1 + BACKEND_RETRY_DELAYS_MS.length; // 4 total
const BACKEND_RETRY_AFTER_CAP_MS = 3 * 60 * 1000; // never honor a Retry-After larger than 3 minutes

function isRetryableBackendError(error) {
  if (!error) return false;
  const code = error.code || '';
  const status = error.response?.status;
  if (['ECONNREFUSED', 'ETIMEDOUT', 'ECONNRESET', 'ENOTFOUND', 'ENETUNREACH'].includes(code)) return true;
  if (status >= 500 || status === 408 || status === 429) return true;
  return false;
}

/**
 * Parse a Retry-After header value (seconds OR HTTP-date) into milliseconds.
 * Returns null if the header is absent or unparseable. Capped at
 * BACKEND_RETRY_AFTER_CAP_MS so a misconfigured backend can't stall us forever.
 */
function parseRetryAfterMs(error) {
  const headers = error?.response?.headers;
  if (!headers) return null;
  // axios lower-cases header keys
  const raw = headers['retry-after'] ?? headers['Retry-After'];
  if (raw == null) return null;
  const str = String(raw).trim();
  if (!str) return null;
  const asInt = Number(str);
  let ms;
  if (Number.isFinite(asInt)) {
    ms = Math.max(0, asInt) * 1000;
  } else {
    const dateMs = Date.parse(str);
    if (!Number.isFinite(dateMs)) return null;
    ms = Math.max(0, dateMs - Date.now());
  }
  return Math.min(ms, BACKEND_RETRY_AFTER_CAP_MS);
}

/**
 * Run a backend request with retries: 10s, 30s, 3min by default; if the
 * backend returns 503/429 with a `Retry-After` header, that value is honored
 * instead (capped at 3 min). Helpers/runners must keep going across backend
 * restarts and overload events.
 *
 * @param {() => Promise<T>} fn - Async function that performs the request (no args).
 * @param {string} label - Label for logging (e.g. "poll", "heartbeat", "job/123/complete").
 * @returns {Promise<T|null>} Resolved value or null if all attempts failed (caller can treat null as no-op where appropriate).
 */
async function withBackendRetry(fn, label) {
  let lastError;
  for (let attempt = 1; attempt <= BACKEND_RETRY_ATTEMPTS; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (!isRetryableBackendError(error) || attempt === BACKEND_RETRY_ATTEMPTS) {
        break;
      }
      const retryAfterMs = parseRetryAfterMs(error);
      const defaultDelayMs = BACKEND_RETRY_DELAYS_MS[attempt - 1];
      const delayMs = retryAfterMs != null ? retryAfterMs : defaultDelayMs;
      const reason = retryAfterMs != null ? `(Retry-After: ${Math.round(retryAfterMs / 1000)}s)` : '';
      console.warn(`⚠️ [BACKEND-RETRY] ${label} attempt ${attempt}/${BACKEND_RETRY_ATTEMPTS} failed: ${error.message} ${reason}. Retrying in ${Math.round(delayMs / 1000)}s...`);
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
  console.error(`❌ [BACKEND-RETRY] Request failed after ${BACKEND_RETRY_ATTEMPTS} attempts: ${label}: ${lastError?.message || lastError}`);
  if (lastError?.response) {
    console.error(`   Status: ${lastError.response.status}; Response:`, lastError.response.data);
  }
  throw lastError;
}

/**
 * Get axios headers with authentication
 */
function getAuthHeaders() {
  return {
    'Authorization': `Bearer ${ENGINE_TOKEN}`
  };
}

/**
 * Get axios headers with service key authentication
 */
function getServiceKeyHeaders() {
  if (!SERVICE_KEY) {
    return null;
  }
  return {
    'Authorization': `Bearer ${SERVICE_KEY}`
  };
}

// getImageUrlFromBackend removed - backend now provides full image_url in job response

/**
 * Get ECR pull token from backend
 * Returns token for authenticating Docker with ECR
 * @param {string} imageUrl - Full ECR image URL (optional, but recommended)
 */
async function getECRPullToken(imageUrl = null) {
  try {
    console.log(`🔑 Requesting temporary ECR pull token from backend...`);
    const response = await axios.post(
      `${BACKEND_URL}/api/docker-engines/ecr-token`,
      imageUrl ? { image_url: imageUrl } : {},
      {
        headers: getAuthHeaders(),
        timeout: 10000
      }
    );
    
    if (response.data && response.data.token) {
      console.log(`✅ Got temporary ECR pull token (expires in ${response.data.expires_in}s)`);
      return response.data;
    }
    
    return null;
  } catch (error) {
    console.warn(`⚠️ Failed to get ECR token from backend:`, error.message);
    return null;
  }
}

/**
 * Get ECR auth config for Dockerode
 * Returns auth config object to pass to docker.pull()
 */
function getECRAuthConfig(tokenData) {
  if (!tokenData || !tokenData.token || !tokenData.registry) {
    throw new Error('Missing registry or token for ECR authentication');
  }
  
  console.log(`🔐 Preparing ECR auth config for registry: ${tokenData.registry}`);
  
  // Dockerode expects auth config in format for authconfig:
  // { username: 'AWS', password: '<token>', serveraddress: '<registry>' }
  // But we also need to encode it properly for the Docker API
  const authConfig = {
    username: 'AWS',
    password: tokenData.token,
    serveraddress: tokenData.registry,
    // Also include auth field for Docker API compatibility
    auth: Buffer.from(`AWS:${tokenData.token}`).toString('base64')
  };
  
  console.log(`✅ ECR auth config prepared (username: ${authConfig.username}, registry: ${authConfig.serveraddress})`);
  return authConfig;
}

/**
 * Gather system identification info for admin display (OS, arch, hostname, all host IPs)
 * Returns sync-only fields; caller can merge docker/version_sha from async getters.
 */
function getSystemInfo() {
  const host_ips = [];
  try {
    const ifaces = networkInterfaces();
    for (const name of Object.keys(ifaces || {})) {
      for (const iface of ifaces[name] || []) {
        if (iface.family === 'IPv4' && !iface.internal && iface.address) {
          host_ips.push(iface.address);
        }
      }
    }
  } catch (_) {
    // ignore
  }
  // Keep single ip for backward compatibility; prefer first host IP
  const ip = host_ips.length > 0 ? host_ips[0] : null;
  return {
    hostname: hostname(),
    os_platform: platform(),
    os_release: release(),
    os_arch: arch(),
    ip,
    host_ips: host_ips.length > 0 ? host_ips : null
  };
}

/**
 * Get Docker daemon info for admin display (version, OS, etc.)
 */
async function getDockerDaemonInfo() {
  try {
    const info = await docker.info();
    return {
      server_version: info.ServerVersion || null,
      operating_system: info.OperatingSystem || null,
      architecture: info.Architecture || null,
      driver: info.Driver || null,
      name: info.Name || null
    };
  } catch (_) {
    return null;
  }
}

/**
 * Normalize image inspect to a sha256:… digest for heartbeats (version_sha) and comparisons.
 */
async function digestFromDockerImageRef(imageRef) {
  if (!imageRef) return null;
  try {
    const img = docker.getImage(imageRef);
    const inspect = await img.inspect();
    const digests = inspect.RepoDigests || [];
    if (digests.length > 0) {
      const d = String(digests[0]);
      if (d.includes('@')) return d.split('@').pop();
      return d.startsWith('sha256:') ? d : `sha256:${d}`;
    }
    const id = String(inspect.Id || '').trim();
    if (id.startsWith('sha256:') && /^sha256:[a-f0-9]{64}$/i.test(id)) return id.toLowerCase();
    const bare = id.replace(/^sha256:/i, '').toLowerCase();
    if (/^[a-f0-9]{64}$/.test(bare)) return `sha256:${bare}`;
    return null;
  } catch (_) {
    return null;
  }
}

/**
 * Get helper image digest (SHA) - from env or by inspecting current container's image
 */
async function getHelperImageDigest() {
  if (process.env.IMAGE_DIGEST || process.env.HELPER_IMAGE_SHA) {
    const d = (process.env.IMAGE_DIGEST || process.env.HELPER_IMAGE_SHA).trim();
    return d.startsWith('sha256:') ? d : `sha256:${d}`;
  }
  try {
    const myId = getOwnContainerId();
    if (myId) {
      try {
        const cont = docker.getContainer(myId);
        const cinsp = await cont.inspect();
        const fromSelf = await digestFromDockerImageRef(cinsp.Image);
        if (fromSelf) return fromSelf;
      } catch (_) {}
    }

    const containers = await docker.listContainers({ all: true });
    const selfName = String(hostname() || '').toLowerCase();
    const helperName = String(HELPER_CONTAINER_NAME || '').toLowerCase();
    const self = containers.find(c =>
      (c.Names || []).some(n => {
        const nn = String(n).replace(/^\//, '').toLowerCase();
        return (
          (selfName && nn.includes(selfName)) ||
          (helperName && nn.includes(helperName)) ||
          nn.includes('aionqa-docker-engine') ||
          nn.includes('docker-engine-pool')
        );
      })
    );
    if (!self) return null;
    const imageRef = self.ImageID || self.Image;
    return await digestFromDockerImageRef(imageRef);
  } catch (_) {
    return null;
  }
}

/**
 * Register this engine with the backend
 */
async function registerEngine() {
  let retries = 5;
  let delay = 2000; // Start with 2 seconds
  
  while (retries > 0) {
    try {
      const system_info = getSystemInfo();
      try {
        system_info.docker = await getDockerDaemonInfo();
      } catch (_) {}
      const version_sha = await getHelperImageDigest();
      const response = await axios.post(
        `${BACKEND_URL}/api/docker-engines/register`,
        {
          engine_id: ENGINE_ID,
          hostname: hostname(),
          docker_socket: DOCKER_SOCKET,
          status: 'active',
          system_info,
          version_sha: version_sha || undefined,
          image_url: HELPER_IMAGE
        },
        { headers: getAuthHeaders() }
      );
      
      console.log(`✅ Engine registered: ${ENGINE_ID}`);
      return response.data;
    } catch (error) {
      retries--;
      if (retries === 0) {
        console.error(`❌ Failed to register engine after 5 attempts:`, error.message);
        if (error.response) {
          console.error(`   Status: ${error.response.status}`);
          console.error(`   Response:`, error.response.data);
        }
        throw error;
      }
      
      console.warn(`⚠️ Failed to register engine (${retries} retries left):`, error.message);
      console.log(`   Retrying in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
      delay *= 2; // Exponential backoff
    }
  }
}

/**
 * Check if a container is AionQA-related (crawler, AI service, Ollama, or helper)
 */
function isAionqaRelatedContainer(containerInfo) {
  const names = (containerInfo.Names || []).join(' ');
  const labels = containerInfo.Labels || {};
  if (labels['aionqa.engine_id'] || labels['aionqa.job_id']) return true;
  if (/aionqa|ollama|unified-crawler|aionqa-ai-service|ai-service-|aionqa-docker-engine/.test(names)) return true;
  return false;
}

/**
 * Get one frame of container stats (CPU %, memory usage/limit). Returns null on error.
 * Docker API with stream: false returns a single JSON object (may be delivered as stream).
 */
function getContainerStatsOnce(container) {
  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      if (s) s.destroy();
      resolve(null);
    }, 5000);
    let s;
    container.stats({ stream: false }, (err, streamOrData) => {
      if (err) {
        clearTimeout(timeout);
        return resolve(null);
      }
      if (!streamOrData) {
        clearTimeout(timeout);
        return resolve(null);
      }
      // Some drivers return the parsed object directly when stream: false
      if (typeof streamOrData === 'object' && streamOrData !== null && typeof streamOrData.on !== 'function') {
        clearTimeout(timeout);
        return resolve(streamOrData);
      }
      s = streamOrData;
      const chunks = [];
      streamOrData.on('data', chunk => chunks.push(chunk));
      streamOrData.on('end', () => {
        clearTimeout(timeout);
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString());
          resolve(data);
        } catch (_) {
          resolve(null);
        }
      });
      streamOrData.on('error', () => {
        clearTimeout(timeout);
        resolve(null);
      });
    });
  });
}

/**
 * Parse Docker stats blob into cpu_pct and memory_usage/limit
 */
function parseStatsBlob(stats) {
  if (!stats || !stats.memory_stats) return { cpu_pct: null, memory_usage: null, memory_limit: null };
  const memUsage = stats.memory_stats.usage != null ? Number(stats.memory_stats.usage) : null;
  const memLimit = stats.memory_stats.limit != null ? Number(stats.memory_stats.limit) : null;
  let cpu_pct = null;
  if (stats.cpu_stats && stats.precpu_stats && stats.cpu_stats.cpu_usage && stats.precpu_stats.cpu_usage) {
    const cpuDelta = stats.cpu_stats.cpu_usage.total_usage - stats.precpu_stats.cpu_usage.total_usage;
    const systemDelta = stats.cpu_stats.system_cpu_usage - stats.precpu_stats.system_cpu_usage;
    const numCpus = (stats.cpu_stats.cpu_usage.percpu_usage || []).length || 1;
    if (systemDelta > 0) {
      cpu_pct = (cpuDelta / systemDelta) * numCpus * 100;
    }
  }
  return { cpu_pct, memory_usage: memUsage, memory_limit: memLimit };
}

/**
 * Try to get GPU info by running nvidia-smi in a container that has GPU (e.g. ollama)
 */
async function getGpuMetrics() {
  try {
    const containers = await docker.listContainers({ all: false });
    const ollama = containers.find(c => (c.Names || []).some(n => n.includes('ollama')));
    if (!ollama) return null;
    const container = docker.getContainer(ollama.Id);
    const exec = await container.exec({
      Cmd: ['nvidia-smi', '--query-gpu=utilization.gpu,memory.used,memory.total', '--format=csv,noheader,nounits'],
      AttachStdout: true,
      AttachStderr: true
    });
    const output = await new Promise((resolve, reject) => {
      exec.start({ Detach: false }, (err, stream) => {
        if (err) return reject(err);
        let out = '';
        stream.on('data', chunk => { out += chunk.toString(); });
        stream.on('end', () => resolve(out));
        stream.on('error', reject);
      });
    });
    const line = (output || '').trim().split('\n')[0];
    if (!line) return null;
    const [util, memUsed, memTotal] = line.split(',').map(s => parseFloat(s.trim()));
    if (Number.isNaN(util)) return null;
    return {
      utilization_pct: Number.isNaN(util) ? null : util,
      memory_used_mb: Number.isNaN(memUsed) ? null : memUsed,
      memory_total_mb: Number.isNaN(memTotal) ? null : memTotal
    };
  } catch (_) {
    return null;
  }
}

/**
 * Collect runner metrics: host (from Docker info), GPU (if available), and per-container stats for AionQA-related containers.
 */
async function collectRunnerMetrics() {
  const out = {
    host: null,
    gpu: null,
    containers: [],
    collected_at: new Date().toISOString()
  };
  try {
    const info = await docker.info();
    const dockerNcpu = info.NCPU != null ? info.NCPU : 0;
    const hostLogicalCpus = (typeof cpus === 'function' ? cpus() : []).length;
    out.host = {
      ncpu: Math.max(dockerNcpu, hostLogicalCpus) || null,
      memory_total_bytes: info.MemTotal != null ? info.MemTotal : null,
      memory_used_bytes: info.MemoryLimit && info.MemoryLimit === false ? null : (info.MemTotal != null ? Math.max(0, info.MemTotal - (info.MemAvailable != null ? info.MemAvailable : 0)) : null)
    };
  } catch (_) {
    out.host = { ncpu: null, memory_total_bytes: null, memory_used_bytes: null };
  }

  try {
    out.gpu = await getGpuMetrics();
  } catch (_) {}

  try {
    const containers = await docker.listContainers({ all: true });
    const aionqa = containers.filter(isAionqaRelatedContainer);
    for (const c of aionqa) {
      const name = (c.Names || [])[0] || c.Id;
      const displayName = name.replace(/^\//, '');
      try {
        const container = docker.getContainer(c.Id);
        const [stats, imgFields] = await Promise.all([
          getContainerStatsOnce(container),
          getMetricImageFields(docker, c.Id, c.Image)
        ]);
        const parsed = stats ? parseStatsBlob(stats) : {};
        out.containers.push({
          id: c.Id.substring(0, 12),
          name: displayName,
          state: c.State,
          cpu_pct: parsed.cpu_pct != null ? Math.round(parsed.cpu_pct * 10) / 10 : null,
          memory_usage_bytes: parsed.memory_usage,
          memory_limit_bytes: parsed.memory_limit,
          image: imgFields.image || c.Image || '',
          image_digest: imgFields.image_digest || undefined
        });
      } catch (_) {
        let imgFields = { image: c.Image || '', image_digest: null };
        try {
          imgFields = await getMetricImageFields(docker, c.Id, c.Image);
        } catch (_) {
          /* ignore */
        }
        out.containers.push({
          id: c.Id.substring(0, 12),
          name: displayName,
          state: c.State,
          cpu_pct: null,
          memory_usage_bytes: null,
          memory_limit_bytes: null,
          image: imgFields.image || c.Image || '',
          image_digest: imgFields.image_digest || undefined
        });
      }
    }
  } catch (_) {
    out.containers = [];
  }
  return out;
}

/** Throttle extra reconcile runs from heartbeat when the cache already shows drift (same gates as {@link runAutomaticImageReconcile}). */
let lastHeartbeatAutoReconcileMs = 0;
const HEARTBEAT_AUTO_RECONCILE_MIN_MS = 20000;

function cacheShowsUpdateAvailable() {
  const iv = cachedImageVersionChecks;
  if (!iv || !iv.services || typeof iv.services !== 'object') return false;
  return Object.values(iv.services).some(s => s && s.status === 'update_available');
}

/**
 * True when automatic pull/restart is allowed (shared runners always; self-hosted only when idle auto-update is on and no crawl jobs).
 * Mirrors the gate inside runAutomaticImageReconcile before pull logic runs.
 */
function shouldAttemptAutoImageReconcileNow() {
  const noActiveCrawlJobs = runningContainers.size === 0 && processingJobs.size === 0;
  if (autoUpdateManagedImages) return true;
  return !autoUpdateManagedImages && autoUpdateManagedImagesWhenIdle && noActiveCrawlJobs;
}

async function maybeReconcileImagesFromHeartbeat() {
  if (!cacheShowsUpdateAvailable()) return;
  if (!shouldAttemptAutoImageReconcileNow()) return;
  const now = Date.now();
  if (now - lastHeartbeatAutoReconcileMs < HEARTBEAT_AUTO_RECONCILE_MIN_MS) return;
  lastHeartbeatAutoReconcileMs = now;
  await runAutomaticImageReconcile();
}

/**
 * Send heartbeat to backend (includes runner metrics when available)
 */
async function sendHeartbeat() {
  try {
    let metrics = null;
    try {
      metrics = await collectRunnerMetrics();
    } catch (err) {
      console.warn(`⚠️ Metrics collection failed:`, err.message);
      metrics = {
        collected_at: new Date().toISOString(),
        host: null,
        gpu: null,
        containers: [],
        metrics_collection_error: String(err?.message || err)
      };
    }
    if (metrics && typeof metrics === 'object') {
      const cacheOk =
        cachedImageVersionChecks &&
        typeof cachedImageVersionChecks === 'object' &&
        cachedImageVersionChecks.services &&
        typeof cachedImageVersionChecks.services === 'object';
      if (!cacheOk) {
        await refreshImageVersionCheckCache('heartbeat_backfill').catch((e) =>
          console.warn(`⚠️ [HEARTBEAT] image_version_checks backfill failed: ${e.message}`)
        );
      }
      const iv = cachedImageVersionChecks;
      const ivOk = iv && typeof iv === 'object' && iv.services && typeof iv.services === 'object';
      metrics.image_version_checks = ivOk
        ? iv
        : {
            checked_at: null,
            missing_capability: true,
            interval_ms: IMAGE_CHECK_INTERVAL_MS,
            helper_version: HELPER_VERSION,
            detail:
              'Registry image checks not initialized yet (waiting for first IMAGE-CHECK run after startup), or this build predates image_version_checks.',
            services: {
              'aionqa-helper': { status: 'unknown', detail: 'Waiting for first registry comparison…' },
              'aionqa-ai-service': { status: 'unknown', detail: 'Waiting for first registry comparison…' },
              ollama: { status: 'unknown', detail: 'No registry baseline (third-party image)' },
              'aionqa-cure': { status: 'unknown', detail: 'Waiting for first registry comparison…' }
            }
          };
    }
    const system_info = getSystemInfo();
    try {
      system_info.docker = await getDockerDaemonInfo();
    } catch (_) {}
    const version_sha = await getHelperImageDigest();
    await withBackendRetry(() => axios.post(
      `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/heartbeat`,
      {
        status: 'active',
        running_containers: runningContainers.size,
        metrics,
        version: HELPER_VERSION,
        build_date: HELPER_BUILD_DATE,
        system_info,
        version_sha: version_sha || undefined,
        image_url: HELPER_IMAGE
      },
      { headers: getAuthHeaders(), timeout: 15000 }
    ), 'heartbeat');
    void maybeReconcileImagesFromHeartbeat().catch(() => {});
  } catch (error) {
    // Logged by withBackendRetry; heartbeat is best-effort
  }
}

/**
 * Poll backend for jobs assigned to this engine
 */
async function pollForJobs() {
  try {
    const response = await withBackendRetry(() => {
      console.log(`[POLL] Polling for jobs at ${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/jobs/next`);
      return axios.get(
        `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/jobs/next`,
        {
          headers: getAuthHeaders(),
          timeout: 10000
        }
      );
    }, 'poll');

    console.log(`[POLL] Response status: ${response.status}`);
    console.log(`[POLL] Response data:`, JSON.stringify(response.data).substring(0, 500));

    if (response.data && response.data.restart_requested) {
      console.log(`🔄 [POLL] Restart requested; updating helper image then recreating container.`);
      const result = await updateHelperImage();
      if (result.safeToRestart) {
        console.log(`✅ [POLL] Helper image updated. Recreating via Docker API...`);
        if (result.imageUrl) {
          const ok = await recreateViaDockerApi(result.imageUrl);
          if (!ok) {
            console.log(`ℹ️ [POLL] Could not recreate automatically. Restart the helper container manually to use the new image.`);
          }
        } else {
          console.warn(`⚠️ [POLL] No imageUrl returned; restart helper container manually.`);
        }
      } else {
        console.warn(`⚠️ [POLL] Could not update: ${result.reason}. Restart container manually if needed.`);
      }
    }
    if (response.data && response.data.job) {
      console.log(`📥 [POLL] Received job from backend: ${response.data.job.job_id}`);
      console.log(`   Job type: ${response.data.job.job_type || 'unknown'}`);
      console.log(`   Image: ${response.data.job.image_url || 'unknown'}`);
      return response.data.job;
    }
    if (response.data && response.data.job === null) {
      return null;
    }
    if (response.data) {
      console.log(`⚠️ [POLL] Unexpected response format. Full response:`, JSON.stringify(response.data).substring(0, 300));
    }
  } catch (error) {
    if (error.response?.status === 404) {
      // No jobs available (legacy 404)
      console.log(`[POLL] No jobs available (404)`);
      return null;
    } else if (error.response?.status === 401) {
      console.error(`❌ Authentication failed when polling for jobs!`);
      console.error(`   Status: 401 Unauthorized`);
      console.error(`   Error: ${error.response?.data?.error || error.message}`);
    } else if (error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT') {
      // Connection issues - log occasionally
      console.warn(`⚠️ Connection issue when polling: ${error.message}`);
    } else {
      console.error(`❌ Error polling for jobs:`, error.message);
      if (error.response) {
        console.error(`   Status: ${error.response.status}`);
        console.error(`   Response:`, error.response.data);
      }
    }
  }
  return null;
}

// ECR authentication removed - repositories are public, no auth required

/**
 * Start a Docker container for a crawling job
 */
async function startCrawlerContainer(job) {
  const {
    job_id,
    job_type,
    image_url: jobImageUrl, // Full ECR URL from backend
    image_sha: jobImageSha, // SHA digest from backend (for comparison)
    environment_vars,
    runner_token,
    backend_url
  } = job;
  
  // Backend provides everything: image_url (full ECR URL) and image_sha
  // Helper only needs to compare SHA and pull if needed
  const image_url = jobImageUrl;
  
  if (!image_url) {
    throw new Error(`Job ${job_id} missing image_url`);
  }

  console.log(`🚀 Starting container for job ${job_id}`);
  console.log(`   Image: ${image_url}`);
  if (jobImageSha) {
    console.log(`   Expected SHA: ${jobImageSha.substring(0, 12)}...`);
  }

  try {
    const isECRImage = image_url.includes('.dkr.ecr.') || image_url.includes('amazonaws.com');
    
    if (isECRImage) {
      // Check if we need to pull the image by comparing SHA
      let needsPull = true;
      
      if (jobImageSha) {
        // Check if local image exists and matches SHA
        try {
          const images = await docker.listImages();
          const localImage = images.find(img => {
            if (!img.RepoTags) return false;
            const matchesTag = img.RepoTags.some(tag => tag === image_url || tag.startsWith(image_url.split(':')[0] + ':'));
            if (!matchesTag) return false;
            // Check if SHA matches (Docker image ID is the SHA)
            return img.Id === jobImageSha || img.Id.startsWith(jobImageSha.replace('sha256:', ''));
          });
          
          if (localImage) {
            console.log(`✅ Local image found with matching SHA - no pull needed`);
            needsPull = false;
          } else {
            console.log(`📥 Local image missing or SHA mismatch - will pull from ECR`);
          }
        } catch (checkError) {
          console.warn(`⚠️ Could not check local image: ${checkError.message}, will pull`);
        }
      } else {
        console.log(`📥 No SHA provided - will pull to ensure latest version`);
      }
      
      if (needsPull) {
        // Request ECR token from backend with image URL
        console.log(`🔑 Requesting ECR token from backend for image: ${image_url}`);
        const tokenData = await getECRPullToken(image_url);
        if (!tokenData) {
          throw new Error(`Failed to get ECR token from backend`);
        }
        
        // Get auth config for Dockerode
        const authConfig = getECRAuthConfig(tokenData);
        
        console.log(`📥 Pulling image from ECR: ${image_url}`);
        await new Promise((resolve, reject) => {
          // Dockerode pull with auth - authconfig must be in modem options
          const pullOptions = {
            authconfig: authConfig
          };
          
          docker.pull(image_url, pullOptions, (err, stream) => {
            if (err) {
              console.error(`❌ Pull error:`, err.message);
              return reject(err);
            }
            
            docker.modem.followProgress(stream, (err, output) => {
              if (err) {
                console.error(`❌ Progress error:`, err.message);
                return reject(err);
              }
              console.log(`✅ Image pulled successfully`);
              resolve();
            });
          });
        });
      }
    } else {
      // Not an ECR image - check if it exists locally, if not try to pull from ECR
      console.log(`ℹ️  Checking if local image exists: ${image_url}`);
      try {
        const images = await docker.listImages();
        const localImage = images.find(img => 
          img.RepoTags && img.RepoTags.some(tag => tag === image_url || tag.startsWith(image_url.split(':')[0] + ':'))
        );
        if (localImage) {
          console.log(`✅ Local image found: ${image_url}`);
        } else {
          // Image doesn't exist locally - this shouldn't happen if backend provides ECR URL
          // But if it does, we can't pull without backend providing the URL
          throw new Error(`Image ${image_url} not found locally and backend did not provide ECR URL`);
        }
      } catch (checkError) {
        console.error(`❌ Error checking local images: ${checkError.message}`);
        throw new Error(`Cannot verify or pull image ${image_url}: ${checkError.message}`);
      }
    }

    // Create container: crawler env must be provided by helper, from backend (token + url). Backend defaults url to api.aionqa.com.
    const crawlerBackendUrl = backend_url || BACKEND_URL;
    if (!backend_url) {
      console.warn(`[ENGINE] Job did not include backend_url; using pool BACKEND_URL: ${crawlerBackendUrl}`);
    }
    const containerEnv = [
      `JOB_ID=${job_id}`,
      `JOB_TYPE=${job_type || (job.job_type || (job.type === 'login' ? 'login' : 'crawl'))}`, // CRITICAL: Pass job type explicitly
      `TASK_MODE=single-job`, // Signal to crawler to exit after this job
      `BACKEND_URL=${crawlerBackendUrl}`, // From backend (default api.aionqa.com); crawler uses this to poll and post results
      `RUNNER_TOKEN=${runner_token}`, // From backend; required for crawler to authenticate
      `RUNNER_TYPE=service-managed`, // Mark as service-managed crawler
      `RUNNER_ID=container-${job_id}`,
      // CRITICAL: AI_SERVICE_TOKEN must NOT be passed to crawlers for security
      // Crawlers authenticate to AI service via RUNNER_TOKEN or other method
      `POLL_INTERVAL=3000`,
      `NODE_ENV=${process.env.NODE_ENV || 'development'}`,
      ...Object.entries(environment_vars || {}).map(([key, value]) => `${key}=${value}`)
    ];
    
    // Container name: if RUNNER_NAME is set, use named runner slot (helper can replace existing by name); else per-job name.
    const containerName = RUNNER_NAME
      ? `aionqa-crawler-runner-${sanitizeRunnerName(RUNNER_NAME)}`
      : `aionqa-crawler-job-${job_id}`;

    let container;
    for (let startAttempt = 1; startAttempt <= CRAWLER_CONTAINER_START_ATTEMPTS; startAttempt++) {
      // Replace existing container with same name if present (named runner: replace previous; per-job: clean stale).
      try {
        const existingContainer = docker.getContainer(containerName);
        const existingInfo = await existingContainer.inspect();
        if (existingInfo) {
          console.log(`🧹 Replacing existing container: ${containerName}`);
          try {
            await existingContainer.stop();
          } catch (stopError) {
            // Container might already be stopped, ignore
          }
          await existingContainer.remove({ force: true, v: true });
          console.log(`✅ Removed existing container: ${containerName}`);
        }
      } catch (notFoundError) {
        // Container doesn't exist, that's fine - continue
      }

      container = await docker.createContainer({
        Image: image_url,
        name: containerName, // Use consistent naming pattern
        Env: containerEnv,
        HostConfig: {
          ShmSize: 2147483648, // 2GB for Chromium
          NetworkMode: (helperConfig && helperConfig.crawler_network) || process.env.CRAWLER_NETWORK || 'aionqa-network',
          AutoRemove: false, // Helper removes containers when not running (see monitorContainer + cleanupContainers)
          RestartPolicy: { Name: 'no' }, // Don't restart
          Binds: ['/var/run/docker.sock:/var/run/docker.sock'], // Mount docker socket for video recording
          LogConfig: LOG_CONFIG_JSON_FILE
        },
        Labels: {
          'aionqa.job_id': job_id,
          'aionqa.engine_id': ENGINE_ID
        }
      });

      // Start container
      await container.start();
      console.log(
        `✅ Container started: ${container.id.substring(0, 12)} (start attempt ${startAttempt}/${CRAWLER_CONTAINER_START_ATTEMPTS})`
      );

      // Grace window: Node + Playwright import can exceed 1s on busy hosts; post-deploy cold starts are common.
      const containerInfo = await waitForCrawlerStartupOrExit(container);
      if (containerInfo.State.Status === 'exited' || containerInfo.State.Status === 'dead') {
        const exitCode = containerInfo.State.ExitCode;
        const oom = !!containerInfo.State.OOMKilled;
        const daemonErrRaw = containerInfo.State.Error != null ? String(containerInfo.State.Error).trim() : '';
        const finishedAt = containerInfo.State.FinishedAt || '';

        // Match unified-crawler single-job mode: runner may exit 0 after reporting failure so Docker exit code
        // does not overwrite an already-reported job outcome. Treat as started; completion path notifies backend.
        if (exitCode === 0) {
          console.warn(
            `ℹ️ Container ${container.id.substring(0, 12)} exited with code 0 during startup grace (finishedAt=${finishedAt}); attaching monitor for terminal notify`
          );
          runningContainers.set(container.id, job_id);
          processingJobs.delete(job_id);
          monitorContainer(container, job_id);
          return container.id;
        }

        const logTail = await fetchCrawlerContainerLogTail(container, 100);
        const detailParts = [
          `exitCode=${exitCode}`,
          oom ? 'OOMKilled=true' : null,
          daemonErrRaw ? `daemonError=${daemonErrRaw}` : null,
          finishedAt ? `finishedAt=${finishedAt}` : null,
          logTail ? `logTail=${logTail}` : null
        ].filter(Boolean);
        const detail = detailParts.length ? detailParts.join('; ') : 'Unknown error';
        console.error(`❌ Container exited during startup grace: ${detail}`);
        console.error(`❌ (legacy summary) Container exited immediately with code ${exitCode}: ${daemonErrRaw || 'Unknown error'}`);

        try {
          console.error(`Container logs (decoded tail):`, logTail);
        } catch (logError) {
          const msg = logError.message || '';
          if (msg.includes('does not support reading')) {
            console.error(`Container logs unavailable (daemon log driver does not support reading). Ensure containers use json-file log driver.`);
          } else {
            console.error(`Failed to get container logs:`, msg);
          }
        }

        // Remove container so it does not stay on the host (force + volumes for isolation)
        try {
          await container.remove({ force: true, v: true });
          console.log(`🧹 Removed failed container ${containerName}`);
        } catch (removeError) {
          console.warn(`⚠️ Failed to remove container: ${removeError.message}`);
        }

        const transient = isTransientCrawlerStartupExit({ exitCode, oom, logTail });
        if (transient && startAttempt < CRAWLER_CONTAINER_START_ATTEMPTS) {
          const delayMs = CRAWLER_CONTAINER_START_RETRY_BASE_MS * startAttempt;
          console.warn(
            `♻️ Crawler startup failed (attempt ${startAttempt}/${CRAWLER_CONTAINER_START_ATTEMPTS}); retrying in ${delayMs}ms`
          );
          await new Promise((r) => setTimeout(r, delayMs));
          continue;
        }

        processingJobs.delete(job_id);
        throw new Error(`Container exited immediately with code ${exitCode}: ${detail}`);
      }

      // Still running (or restarting) after grace — proceed to monitor path
      break;
    }

    // Track container
    runningContainers.set(container.id, job_id);
    
    // Remove from processing set once container is successfully started (job is now running)
    processingJobs.delete(job_id);

    // Monitor container
    monitorContainer(container, job_id);

    // Notify backend that container started (retry on backend unavailable)
    try {
      await withBackendRetry(() => axios.post(
        `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/jobs/${job_id}/started`,
        { container_id: container.id },
        { headers: getAuthHeaders() }
      ), `jobs/${job_id}/started`);
    } catch (e) {
      console.error(`⚠️ Failed to notify backend of container start for job ${job_id}:`, e?.message);
    }

    return container.id;
  } catch (error) {
    console.error(`❌ Failed to start container for job ${job_id}:`, error.message);
    
    // Notify backend of failure (retry on backend unavailable)
    try {
      await withBackendRetry(() => axios.post(
        `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/jobs/${job_id}/failed`,
        { error: error.message },
        { headers: getAuthHeaders() }
      ), `jobs/${job_id}/failed`);
    } catch (notifyError) {
      console.error(`❌ Failed to notify backend of container failure after retries:`, notifyError?.message);
    }
    
    throw error;
  }
}

/**
 * Monitor container: wait for it to exit, then notify backend and remove container.
 * (Previously only inspected once, so we never saw "exited" and never called /completed.)
 */
async function monitorContainer(container, jobId) {
  try {
    const waitResult = await container.wait();
    const exitCode = waitResult?.StatusCode ?? waitResult?.ExitCode ?? 0;
    runningContainers.delete(container.id);

    console.log(`📦 Container ${container.id.substring(0, 12)} exited with code ${exitCode} for job ${jobId}`);

    try {
      await withBackendRetry(() => axios.post(
        `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/jobs/${jobId}/completed`,
        { container_id: container.id, exit_code: exitCode },
        { headers: getAuthHeaders() }
      ), `jobs/${jobId}/completed`);
    } catch (error) {
      console.error(`⚠️ Failed to notify backend of container completion after retries:`, error?.message);
    }

    try {
      await container.remove({ force: true, v: true });
      console.log(`🧹 Removed exited container ${container.id.substring(0, 12)} for job ${jobId}`);
    } catch (removeError) {
      console.warn(`⚠️ Failed to remove exited container: ${removeError.message}`);
    }
  } catch (error) {
    console.error(`⚠️ Error monitoring container for job ${jobId}:`, error.message);
  }
}

/**
 * Clean up stopped containers
 */
async function cleanupContainers() {
  try {
    const containers = await docker.listContainers({ all: true });
    const aionqaContainers = containers.filter(c => 
      c.Labels && c.Labels['aionqa.engine_id'] === ENGINE_ID
    );

    for (const containerInfo of aionqaContainers) {
      if (containerInfo.State === 'exited' || containerInfo.State === 'dead') {
        try {
          const container = docker.getContainer(containerInfo.Id);
          const jobId = runningContainers.get(containerInfo.Id);
          await container.remove({ force: true, v: true });
          runningContainers.delete(containerInfo.Id);
          // Also remove from processing set if it was there
          if (jobId) {
            processingJobs.delete(jobId);
          }
          console.log(`🧹 Cleaned up container: ${containerInfo.Id.substring(0, 12)}`);
        } catch (error) {
          // Container might already be removed
        }
      }
    }
  } catch (error) {
    console.error(`⚠️ Error cleaning up containers:`, error.message);
  }
}

// Helper image updates are handled externally (docker-compose, k8s, etc.)
// No need for helper to update itself

// Track jobs currently being processed to prevent duplicate starts
const processingJobs = new Set();

/**
 * Update helper image from ECR if newer version available.
 * Uses short-lived token from backend. Requires image_sha from backend to avoid blind pulls.
 * @returns {{ safeToRestart: true } | { safeToRestart: false, reason: string }}
 */
async function updateHelperImage() {
  try {
    console.log(`🔄 Checking for helper image updates...`);
    const response = await axios.post(
      `${BACKEND_URL}/api/docker-engines/image-url`,
      { image_name: 'aionqa-docker-engine-pool:latest' },
      { headers: getAuthHeaders(), timeout: 10000 }
    );
    if (!response.data || !response.data.image_url) {
      return { safeToRestart: false, reason: 'backend did not return image_url' };
    }
    const imageUrl = response.data.image_url;
    const imageSha = response.data.image_sha;
    console.log(`📦 Helper image URL: ${imageUrl}`);
    if (imageSha) {
      console.log(`📦 Helper image SHA: ${imageSha.substring(0, 12)}...`);
      try {
        const isCurrent = await localImageDigestMatchesSha(imageUrl, imageSha);
        if (isCurrent) {
          console.log(`✅ Helper image is up to date (SHA matches)`);
          return { safeToRestart: true, imageUrl, updated: false };
        }
      } catch (checkError) {
        console.error(`❌ Error comparing SHA: ${checkError.message}; skipping SHA check, will pull`);
      }
    } else {
      // Never self-restart helper without a digest match signal.
      // Missing SHA from backend previously caused perpetual self-update loops.
      console.warn(`⚠️ Backend did not return image_sha; skipping helper self-update to avoid restart loops`);
      return { safeToRestart: true, imageUrl, updated: false };
    }
    const tokenData = await getECRPullToken(imageUrl);
    if (!tokenData) {
      console.warn(`⚠️ Failed to get ECR token for helper image`);
      return { safeToRestart: false, reason: 'failed to get ECR token' };
    }
    const authConfig = getECRAuthConfig(tokenData);
    await new Promise((resolve, reject) => {
      docker.pull(imageUrl, { authconfig: authConfig }, (err, stream) => {
        if (err) return reject(err);
        docker.modem.followProgress(stream, (err, output) => (err ? reject(err) : resolve()));
      });
    });
    console.log(`✅ Helper image updated successfully`);
    return { safeToRestart: true, imageUrl, updated: true };
  } catch (error) {
    console.warn(`⚠️ Failed to update helper image: ${error.message}`);
    return { safeToRestart: false, reason: error.message, updated: false };
  }
}

/**
 * Check and update crawler image from ECR if newer version available
 * Uses backend to get latest image URL and SHA
 */
async function checkCrawlerImage() {
  try {
    console.log(`🔄 Checking for crawler image updates...`);
    
    // Get crawler image URL from backend
    const response = await axios.post(
      `${BACKEND_URL}/api/docker-engines/image-url`,
      { image_name: 'aionqa-unified-crawler:latest' },
      { headers: getAuthHeaders(), timeout: 10000 }
    );
    
    if (!response.data || !response.data.image_url) {
      console.warn(`⚠️ Failed to get crawler image URL from backend`);
      return;
    }
    
    const imageUrl = response.data.image_url;
    const imageSha = response.data.image_sha;
    console.log(`📦 Crawler image URL: ${imageUrl}`);
    if (imageSha) {
      console.log(`📦 Crawler image SHA: ${imageSha.substring(0, 12)}...`);
    }
    
    // Check if we need to update by comparing SHA
    if (imageSha) {
      try {
        const isCurrent = await localImageDigestMatchesSha(imageUrl, imageSha);
        if (isCurrent) {
          console.log(`✅ Crawler image is up to date (SHA matches)`);
          return false; // Already up to date
        }
      } catch (checkError) {
        console.warn(`⚠️ Could not check local crawler image: ${checkError.message}, will pull`);
      }
    }
    
    // Image needs updating - pull from ECR
    console.log(`📥 Pulling latest crawler image from ECR: ${imageUrl}`);
    const tokenData = await getECRPullToken(imageUrl);
    if (!tokenData) {
      console.warn(`⚠️ Failed to get ECR token for crawler image`);
      return false;
    }
    
    const authConfig = getECRAuthConfig(tokenData);
    
    await new Promise((resolve, reject) => {
      docker.pull(imageUrl, { authconfig: authConfig }, (err, stream) => {
        if (err) {
          console.warn(`⚠️ Failed to pull crawler image: ${err.message}`);
          return resolve(); // Don't fail on update
        }
        
        docker.modem.followProgress(stream, (err, output) => {
          if (err) {
            console.warn(`⚠️ Crawler image pull progress error: ${err.message}`);
            return resolve(); // Don't fail on update
          }
          console.log(`✅ Crawler image updated successfully`);
          resolve();
        });
      });
    });
    return true;
  } catch (error) {
    console.warn(`⚠️ Failed to check crawler image: ${error.message}`);
    // Don't fail if crawler image check fails
    return false;
  }
}

async function checkAIServiceImageAndRestartIfUpdated() {
  try {
    if (!isAIServiceEnabled()) return false;
    const containerName = getAIServiceContainerName();
    const response = await axios.post(
      `${BACKEND_URL}/api/docker-engines/image-url`,
      { image_name: 'aionqa-ai-service:latest' },
      { headers: getAuthHeaders(), timeout: 10000 }
    );
    const imageUrl = response.data?.image_url;
    const imageSha = response.data?.image_sha || null;
    if (!imageUrl || !imageSha) {
      // Without SHA we cannot safely detect updates without forced pull/restart loops.
      return false;
    }

    const containers = await docker.listContainers({ all: true });
    const existing = containers.find(c => c.Names && c.Names.some(n => n.replace(/^\//, '') === containerName));
    if (existing && await localImageDigestMatchesSha(imageUrl, imageSha)) {
      return false;
    }

    console.log(`🔄 AI service image update detected (${containerName}); pulling and restarting...`);
    await pullAIServiceImage(true);
    await stopAndRemoveAIServiceContainer();
    await startAIServiceContainer();
    return true;
  } catch (error) {
    console.warn(`⚠️ Failed AI service auto-update check: ${error.message}`);
    return false;
  }
}

function getCureServiceContainerName() {
  const envName = process.env.CURE_CONTAINER_NAME;
  if (envName && typeof envName === 'string' && envName.trim()) return envName.trim();
  const cfgName = helperConfig?.cure_container_name;
  if (cfgName && typeof cfgName === 'string' && cfgName.trim()) return cfgName.trim();
  return 'aionqa-cure';
}

function isCureServiceEnabled() {
  if (process.env.CURE_SERVICE_ENABLED === 'true' || process.env.CURE_SERVICE_ENABLED === '1') return true;
  if (process.env.CURE_SERVICE_ENABLED === 'false' || process.env.CURE_SERVICE_ENABLED === '0') return false;
  if (typeof helperConfig?.cure_service_enabled === 'boolean') return helperConfig.cure_service_enabled;
  // If not explicitly configured, treat existing container as enabled.
  return true;
}

async function getCureImageUrl() {
  if (!isCureServiceEnabled()) return null;
  const imageUrlFromEnv = process.env.CURE_IMAGE_URL;
  if (imageUrlFromEnv) return imageUrlFromEnv;
  try {
    const response = await axios.post(
      `${BACKEND_URL}/api/docker-engines/image-url`,
      { image_name: 'aionqa-cure:latest' },
      { headers: getAuthHeaders(), timeout: 10000 }
    );
    if (response.data?.image_url) return response.data.image_url;
  } catch (error) {
    console.warn(`⚠️ Failed to get cure image URL from backend: ${error.message}`);
  }
  const region = process.env.AWS_REGION || 'ap-southeast-2';
  const accountId = process.env.AWS_ACCOUNT_ID || '778954507986';
  return `${accountId}.dkr.ecr.${region}.amazonaws.com/aionqa-cure:latest`;
}

async function pullManagedImage(imageUrl, forcePull = false, label = 'service') {
  if (!imageUrl) return false;
  try {
    if (!forcePull) {
      const images = await docker.listImages();
      const localImage = images.find(img => {
        if (!img.RepoTags) return false;
        return img.RepoTags.some(tag => tag === imageUrl || tag.startsWith(imageUrl.split(':')[0] + ':'));
      });
      if (localImage) return true;
    }
    const tokenData = await getECRPullToken(imageUrl);
    if (!tokenData) {
      console.warn(`⚠️ Failed to get ECR token for ${label} image`);
      return false;
    }
    const authConfig = getECRAuthConfig(tokenData);
    await new Promise((resolve, reject) => {
      docker.pull(imageUrl, { authconfig: authConfig }, (err, stream) => {
        if (err) return reject(err);
        docker.modem.followProgress(stream, (progressErr) => (progressErr ? reject(progressErr) : resolve()));
      });
    });
    return true;
  } catch (error) {
    console.warn(`⚠️ Failed to pull ${label} image: ${error.message}`);
    return false;
  }
}

/** Base URL for admin-api (cure queue). Helper uses SERVICE_TOKEN to call bulk replan after cure image redeploy. */
function resolveAdminApiBaseUrlForCureReplan() {
  const direct = String(
    process.env.HELPER_ADMIN_API_URL
      || process.env.ADMIN_API_URL
      || process.env.CURE_BACKEND_URL
      || ''
  ).trim();
  if (direct) return direct.replace(/\/$/, '');
  const b = String(process.env.BACKEND_URL || '').trim();
  if (b.includes('://api.')) return b.replace('://api.', '://admin-api.').replace(/\/$/, '');
  return '';
}

/**
 * After a successful cure container redeploy (new image pulled / recreated), re-queue all terminal fix requests
 * so runners perform a fresh plan on the updated image.
 */
async function notifyBulkCureReplanAfterCureImageRefresh(contextLabel = 'cure-redeploy') {
  const base = resolveAdminApiBaseUrlForCureReplan();
  const serviceToken = String(process.env.SERVICE_TOKEN || '').trim();
  if (!base || !serviceToken) {
    console.log(
      `⏭️ Skipping bulk cure replan (${contextLabel}): ${!base ? 'set HELPER_ADMIN_API_URL, ADMIN_API_URL, or CURE_BACKEND_URL (or api.* BACKEND_URL)' : 'set SERVICE_TOKEN to match admin-api SERVICE_TOKEN'}`
    );
    return;
  }
  const url = `${base}/api/admin/cure/requests/replan-all-terminal`;
  try {
    const response = await axios.post(
      url,
      {
        note: `Cure runner refreshed (${contextLabel}, engine ${ENGINE_ID}); queued fresh plans for completed/failed requests.`,
      },
      {
        headers: {
          Authorization: `Bearer ${serviceToken}`,
          'Content-Type': 'application/json',
        },
        timeout: 60000,
      },
    );
    const mod = response.data?.modified_count;
    const matched = response.data?.matched_count;
    if (response.data?.ok) {
      console.log(`✅ Bulk cure replan triggered: matched=${matched ?? '?'}, modified=${mod ?? '?'}`);
    } else {
      console.warn(`⚠️ Bulk cure replan unexpected response:`, response.data);
    }
  } catch (err) {
    console.warn(`⚠️ Bulk cure replan request failed (${contextLabel}): ${err.message}`);
  }
}

async function recreateContainerFromExisting(existingId, newImageUrl, containerName, label = 'service') {
  if (!existingId || !newImageUrl || !containerName) return false;
  try {
    const oldContainer = docker.getContainer(existingId);
    const inspectData = await oldContainer.inspect();
    const createOpts = buildCreateOptsFromInspect(inspectData, newImageUrl, containerName);
    // Cure must not inherit stale Env or binds from an old container (e.g. missing CURE_ENGINE_ID, wrong /app workspace).
    if (label === 'cure') {
      await fetchHelperConfig();
      createOpts.Env = getCureServiceEnv();
      const wsBind = await resolveCureWorkspaceBindString();
      if (wsBind) {
        createOpts.HostConfig = createOpts.HostConfig || {};
        createOpts.HostConfig.Binds = [wsBind];
      }
    }
    if (inspectData?.State?.Running) {
      await oldContainer.stop({ t: 10 }).catch(() => {});
    }
    await oldContainer.remove({ force: true }).catch(() => {});
    const newContainer = await docker.createContainer(createOpts);
    await newContainer.start();
    console.log(`✅ ${label} container redeployed: ${containerName}`);
    if (label === 'cure') {
      await notifyBulkCureReplanAfterCureImageRefresh('cure-container-redeploy');
    }
    return true;
  } catch (error) {
    console.warn(`⚠️ Failed to redeploy ${label} container ${containerName}: ${error.message}`);
    return false;
  }
}

/**
 * host:path[:mode] bind for cure workspace, or null (no bind).
 * Priority: CURE_WORKSPACE_BIND → AIONQA_WORKSPACE_HOST_PATH (or CURE_WORKSPACE_HOST_PATH) → same host path as this helper's /workspace mount.
 */
async function resolveCureWorkspaceBindString() {
  const explicit = String(process.env.CURE_WORKSPACE_BIND || '').trim();
  if (explicit && explicit.includes(':')) return explicit;

  const hostOnly = String(
    process.env.AIONQA_WORKSPACE_HOST_PATH
      || process.env.CURE_WORKSPACE_HOST_PATH
      || ''
  ).trim();
  if (hostOnly) {
    const dest = String(process.env.CURE_WORKSPACE_DEST_IN_CONTAINER || DEFAULT_CURE_WORKSPACE_CONTAINER).trim()
      || DEFAULT_CURE_WORKSPACE_CONTAINER;
    return `${hostOnly}:${dest}`;
  }

  const myId = getOwnContainerId();
  if (!myId) return null;
  try {
    const data = await docker.getContainer(myId).inspect();
    const binds = Array.isArray(data?.HostConfig?.Binds) ? data.HostConfig.Binds : [];
    for (const b of binds) {
      if (typeof b !== 'string') continue;
      const marker = ':/workspace';
      const idx = b.indexOf(marker);
      if (idx < 0) continue;
      const host = b.slice(0, idx);
      const tail = b.slice(idx + marker.length);
      if (!host) continue;
      if (tail === '' || tail === ':rw' || tail === ':ro') {
        return tail ? `${host}${marker}${tail}` : `${host}${marker}`;
      }
    }
    const mounts = Array.isArray(data?.Mounts) ? data.Mounts : [];
    for (const m of mounts) {
      if (m?.Destination === '/workspace' && m.Source) {
        return m.RW === false ? `${m.Source}:/workspace:ro` : `${m.Source}:/workspace`;
      }
    }
  } catch (_) {}
  return null;
}

async function checkCureServiceImageAndRestartIfUpdated() {
  try {
    if (!isCureServiceEnabled()) return false;
    const containerName = getCureServiceContainerName();
    const containers = await docker.listContainers({ all: true });
    const existing = findContainerByName(containers, containerName);
    if (!existing) return false;

    const response = await axios.post(
      `${BACKEND_URL}/api/docker-engines/image-url`,
      { image_name: 'aionqa-cure:latest' },
      { headers: getAuthHeaders(), timeout: 10000 }
    );
    const imageUrl = response.data?.image_url;
    const imageSha = response.data?.image_sha || null;
    if (!imageUrl) return false;
    if (!imageSha) {
      // Avoid restart churn when backend does not provide digest for cure image.
      return false;
    }
    if (imageSha && await localImageDigestMatchesSha(imageUrl, imageSha)) return false;

    console.log(`🔄 Cure image update detected (${containerName}); pulling and redeploying...`);
    const pulled = await pullManagedImage(imageUrl, true, 'cure');
    if (!pulled) return false;
    const targetName = normalizeContainerName(existing.Names?.[0]) || containerName;
    return await recreateContainerFromExisting(existing.Id, imageUrl, targetName, 'cure');
  } catch (error) {
    console.warn(`⚠️ Failed cure service auto-update check: ${error.message}`);
    return false;
  }
}

function getCureServiceEnv() {
  const configuredCureBackendUrl = process.env.CURE_BACKEND_URL;
  const defaultBackendUrl = process.env.BACKEND_URL || '';
  const backendUrl = configuredCureBackendUrl
    || (defaultBackendUrl.includes('://api.') ? defaultBackendUrl.replace('://api.', '://admin-api.') : defaultBackendUrl)
    || 'https://admin-api.aionqa.com';
  const backendKind = process.env.CURE_BACKEND_KIND || 'admin';
  const adminToken = process.env.ADMIN_TOKEN || helperConfig?.cure_admin_token || '';
  const authMode = process.env.CURE_AUTH_MODE || (adminToken ? 'admin' : 'runner');
  const runnerToken = process.env.CURE_TOKEN || process.env.ENGINE_TOKEN || '';
  return [
    `BACKEND_URL=${backendUrl}`,
    `CURE_BACKEND_KIND=${backendKind}`,
    `CURE_AUTH_MODE=${authMode}`,
    `ADMIN_TOKEN=${adminToken}`,
    `CURE_TOKEN=${runnerToken}`,
    `SERVICE_TOKEN=${process.env.SERVICE_TOKEN || ''}`,
    `CURE_AGENT_ID=${process.env.CURE_AGENT_ID || `${ENGINE_ID}-cure`}`,
    // Admin cure runtime-config: ENGINE_ID + runner_id (from token) so ADMIN_TOKEN auth can still merge the right docker_engines row.
    `CURE_ENGINE_ID=${ENGINE_ID}`,
    `ENGINE_ID=${ENGINE_ID}`,
    `AIONQA_DOCKER_RUNNER_ID=${RUNNER_ID}`,
    `RUNNER_ID=${RUNNER_ID}`,
    `CURSOR_WORKSPACE_ROOT=${process.env.CURE_WORKSPACE_ROOT || process.env.CURSOR_WORKSPACE_ROOT || DEFAULT_CURE_WORKSPACE_CONTAINER}`,
    `CURSOR_AGENT_COMMAND=${process.env.CURE_AGENT_COMMAND || ''}`,
    `POLL_INTERVAL_MS=${process.env.CURE_POLL_INTERVAL_MS || '10000'}`,
    `CURE_EXEC_TIMEOUT_MS=${process.env.CURE_EXEC_TIMEOUT_MS || '7200000'}`,
    `CURE_RUNTIME_CONFIG_REFRESH_MS=${process.env.CURE_RUNTIME_CONFIG_REFRESH_MS || '300000'}`,
    `CURE_HTTP_RETRY_MAX_ATTEMPTS=${process.env.CURE_HTTP_RETRY_MAX_ATTEMPTS || '4'}`,
    `CURE_HTTP_RETRY_BASE_MS=${process.env.CURE_HTTP_RETRY_BASE_MS || '1000'}`,
    // GitHub HTTPS clones (prefetch + repo sync): PAT from host or rely on admin-api CURE_GITHUB_TOKEN → runtime.secrets.
    `GITHUB_TOKEN=${process.env.GITHUB_TOKEN || ''}`,
    `GH_TOKEN=${process.env.GH_TOKEN || ''}`,
  ];
}

async function stopAndRemoveCureServiceContainer() {
  const containerName = getCureServiceContainerName();
  try {
    const containers = await docker.listContainers({ all: true });
    const existing = findContainerByName(containers, containerName);
    if (!existing) return;
    const container = docker.getContainer(existing.Id);
    if (existing.State === 'running') {
      await container.stop({ t: 10 }).catch(() => {});
    }
    await container.remove({ force: true }).catch(() => {});
    console.log(`✅ Cure service container stopped/removed: ${containerName}`);
  } catch (error) {
    console.warn(`⚠️ Failed to stop/remove cure service container: ${error.message}`);
  }
}

async function startCureServiceContainer(useCachedConfig = false) {
  if (!useCachedConfig) await fetchHelperConfig();
  if (!isCureServiceEnabled()) return false;

  const containerName = getCureServiceContainerName();
  const imageUrl = await getCureImageUrl();
  if (!imageUrl) return false;

  try {
    const containers = await docker.listContainers({ all: true });
    const existing = findContainerByName(containers, containerName);
    if (existing && existing.State === 'running') return true;
    if (existing) {
      const oldContainer = docker.getContainer(existing.Id);
      await oldContainer.stop({ t: 10 }).catch(() => {});
      await oldContainer.remove({ force: true }).catch(() => {});
    }

    const pulled = await pullManagedImage(imageUrl, true, 'cure');
    if (!pulled) return false;

    const hostConfig = {
      NetworkMode: getConfigNetwork(),
      RestartPolicy: { Name: 'unless-stopped' },
      LogConfig: LOG_CONFIG_JSON_FILE,
    };
    const workspaceBind = await resolveCureWorkspaceBindString();
    if (workspaceBind) {
      hostConfig.Binds = [workspaceBind];
      console.log(`📁 Cure workspace bind: ${workspaceBind} → CURSOR_WORKSPACE_ROOT=${process.env.CURE_WORKSPACE_ROOT || process.env.CURSOR_WORKSPACE_ROOT || DEFAULT_CURE_WORKSPACE_CONTAINER}`);
    } else {
      console.log('📁 Cure workspace: no host bind (clone or image layout: repos should live under CURSOR_WORKSPACE_ROOT, default /workspace)');
    }

    const container = await docker.createContainer({
      Image: imageUrl,
      name: containerName,
      Env: getCureServiceEnv(),
      HostConfig: hostConfig,
    });
    await container.start();
    console.log(`✅ Cure service container started: ${containerName} using ${imageUrl}`);
    return true;
  } catch (error) {
    console.warn(`⚠️ Failed to start cure service container: ${error.message}`);
    return false;
  }
}

/**
 * Compare managed images to registry digests (no pull). Populates UI-facing checks and drift list for self-hosted.
 * @returns {{ checked_at: string, services: Record<string, { status: string, registry_digest?: string, detail?: string }>, outdated: string[] }}
 */
async function evaluateManagedImages() {
  const checked_at = new Date().toISOString();
  const services = {
    'aionqa-helper': { status: 'unknown', detail: 'Not evaluated' },
    'aionqa-ai-service': { status: 'unknown', detail: 'Not evaluated' },
    ollama: { status: 'unknown', detail: 'No registry baseline (third-party image)' },
    'aionqa-cure': { status: 'unknown', detail: 'Not evaluated' }
  };
  const outdated = [];

  try {
    const helperResp = await axios.post(
      `${BACKEND_URL}/api/docker-engines/image-url`,
      { image_name: 'aionqa-docker-engine-pool:latest' },
      { headers: getAuthHeaders(), timeout: 10000 }
    );
    const hUrl = helperResp.data?.image_url;
    const hSha = helperResp.data?.image_sha;
    if (hUrl && hSha) {
      try {
        const match = await localImageDigestMatchesSha(hUrl, hSha);
        services['aionqa-helper'] = { status: match ? 'up_to_date' : 'update_available', registry_digest: hSha };
        if (!match) outdated.push('helper');
      } catch (e) {
        services['aionqa-helper'] = { status: 'unknown', detail: String(e?.message || e) };
      }
    } else if (hUrl) {
      const apiErr = helperResp.data?.image_sha_error;
      services['aionqa-helper'] = {
        status: 'unknown',
        detail: apiErr
          ? `Registry digest unavailable (${apiErr})`
          : 'Registry digest unavailable (API returned no image_sha; ensure api role can call ecr-public:DescribeImages)'
      };
    }
  } catch (e) {
    services['aionqa-helper'] = { status: 'unknown', detail: String(e?.message || e) };
  }

  try {
    const crawlResp = await axios.post(
      `${BACKEND_URL}/api/docker-engines/image-url`,
      { image_name: 'aionqa-unified-crawler:latest' },
      { headers: getAuthHeaders(), timeout: 10000 }
    );
    const cUrl = crawlResp.data?.image_url;
    const cSha = crawlResp.data?.image_sha;
    if (cUrl && cSha) {
      try {
        if (!(await localImageDigestMatchesSha(cUrl, cSha))) outdated.push('crawler');
      } catch (_) {}
    }
  } catch (_) {}

  if (isAIServiceEnabled()) {
    try {
      const cn = getAIServiceContainerName();
      const containers = await docker.listContainers({ all: true });
      const existing = containers.find(c => c.Names && c.Names.some(n => n.replace(/^\//, '') === cn));
      const aiResp = await axios.post(
        `${BACKEND_URL}/api/docker-engines/image-url`,
        { image_name: 'aionqa-ai-service:latest' },
        { headers: getAuthHeaders(), timeout: 10000 }
      );
      const aiUrl = aiResp.data?.image_url;
      const aiSha = aiResp.data?.image_sha;
      if (aiUrl && aiSha) {
        try {
          const match = await localImageDigestMatchesSha(aiUrl, aiSha);
          services['aionqa-ai-service'] = { status: match ? 'up_to_date' : 'update_available', registry_digest: aiSha };
          if (!match && existing) outdated.push('ai_service');
        } catch (e) {
          services['aionqa-ai-service'] = { status: 'unknown', detail: String(e?.message || e) };
        }
      } else if (aiUrl) {
        services['aionqa-ai-service'] = { status: 'unknown', detail: 'Registry digest unavailable' };
      }
    } catch (e) {
      services['aionqa-ai-service'] = { status: 'unknown', detail: String(e?.message || e) };
    }
  } else {
    services['aionqa-ai-service'] = { status: 'skipped', detail: 'Managed AI container not used (cloud/balance or disabled)' };
  }

  if (isCureServiceEnabled()) {
    try {
      const cureName = getCureServiceContainerName();
      const containers = await docker.listContainers({ all: true });
      const existing = findContainerByName(containers, cureName);
      const r = await axios.post(
        `${BACKEND_URL}/api/docker-engines/image-url`,
        { image_name: 'aionqa-cure:latest' },
        { headers: getAuthHeaders(), timeout: 10000 }
      );
      const cuUrl = r.data?.image_url;
      const cuSha = r.data?.image_sha;
      if (cuUrl && cuSha) {
        try {
          const match = await localImageDigestMatchesSha(cuUrl, cuSha);
          services['aionqa-cure'] = { status: match ? 'up_to_date' : 'update_available', registry_digest: cuSha };
          if (!match && existing) outdated.push('cure');
        } catch (e) {
          services['aionqa-cure'] = { status: 'unknown', detail: String(e?.message || e) };
        }
      } else if (cuUrl) {
        services['aionqa-cure'] = { status: 'unknown', detail: 'Registry digest unavailable' };
      }
    } catch (e) {
      services['aionqa-cure'] = { status: 'unknown', detail: String(e?.message || e) };
    }
  } else {
    services['aionqa-cure'] = { status: 'skipped', detail: 'Cure service disabled' };
  }

  return { checked_at, services, outdated };
}

/**
 * Run registry digest comparison and store for heartbeats. Safe to call often; heavy work is bounded by callers.
 * @param {string} reason - e.g. startup | auto_reconcile
 */
async function refreshImageVersionCheckCache(reason = 'interval') {
  try {
    const evaluation = await evaluateManagedImages();
    cachedImageVersionChecks = {
      checked_at: evaluation.checked_at,
      services: evaluation.services,
      check_reason: reason,
      interval_ms: IMAGE_CHECK_INTERVAL_MS,
      helper_version: HELPER_VERSION
    };
    console.log(`✅ [IMAGE-CHECK] Registry comparison done (${reason}), checked_at=${evaluation.checked_at}`);
    return evaluation;
  } catch (err) {
    const checked_at = new Date().toISOString();
    console.warn(`⚠️ [IMAGE-CHECK] Registry comparison failed (${reason}): ${err.message}`);
    cachedImageVersionChecks = {
      checked_at,
      services: {
        'aionqa-helper': { status: 'unknown', detail: String(err?.message || err) },
        'aionqa-ai-service': { status: 'unknown', detail: 'Check failed' },
        ollama: { status: 'unknown', detail: 'No registry baseline (third-party image)' },
        'aionqa-cure': { status: 'unknown', detail: 'Check failed' }
      },
      check_reason: reason,
      error: String(err?.message || err),
      interval_ms: IMAGE_CHECK_INTERVAL_MS,
      helper_version: HELPER_VERSION
    };
    return null;
  }
}

async function collectOutdatedManagedImages() {
  try {
    const { outdated } = await evaluateManagedImages();
    return outdated;
  } catch (_) {
    return [];
  }
}

async function reportManagedImageDriftToBackend(precomputedOutdated = null) {
  if (autoUpdateManagedImages) return;
  try {
    const outdated = Array.isArray(precomputedOutdated) ? precomputedOutdated : await collectOutdatedManagedImages();
    await axios.post(
      `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/managed-image-drift`,
      { outdated },
      { headers: getAuthHeaders(), timeout: 15000 }
    );
    if (outdated.length > 0) {
      console.log(`ℹ️ [IMAGE-DRIFT] Reported outdated managed images (no auto-pull on self-hosted): ${outdated.join(', ')}`);
    }
  } catch (err) {
    const st = err.response?.status;
    if (st === 404 || st === 403) return;
    console.warn(`⚠️ [IMAGE-DRIFT] Failed to report drift: ${err.message}`);
  }
}

async function runAutomaticImageReconcile() {
  try {
    const evaluation = await refreshImageVersionCheckCache('auto_reconcile');
    const noActiveCrawlJobs = runningContainers.size === 0 && processingJobs.size === 0;
    const hasOutdated =
      evaluation && Array.isArray(evaluation.outdated) && evaluation.outdated.length > 0;
    const runIdleAutoUpdate =
      !autoUpdateManagedImages && autoUpdateManagedImagesWhenIdle && noActiveCrawlJobs && hasOutdated;

    if (!autoUpdateManagedImages && !runIdleAutoUpdate) {
      await reportManagedImageDriftToBackend(evaluation ? evaluation.outdated : null);
      return;
    }

    if (runIdleAutoUpdate) {
      console.log(
        `🔄 [AUTO-IMAGE] Self-hosted idle update: applying outdated components (${evaluation.outdated.join(', ')}) — no active crawl jobs.`
      );
    }

    const crawlerUpdated = await checkCrawlerImage();
    if (crawlerUpdated) {
      console.log(`✅ [AUTO-IMAGE] Crawler image updated (new jobs will use latest image).`);
    }

    const aiUpdated = await checkAIServiceImageAndRestartIfUpdated();
    if (aiUpdated) {
      console.log(`✅ [AUTO-IMAGE] AI service image updated and container restarted.`);
    }

    const cureUpdated = await checkCureServiceImageAndRestartIfUpdated();
    if (cureUpdated) {
      console.log(`✅ [AUTO-IMAGE] Cure image updated and container redeployed.`);
    }

    const helperResult = await updateHelperImage();
    if (helperResult?.updated) {
      const hasActiveWork = runningContainers.size > 0 || processingJobs.size > 0;
      if (hasActiveWork) {
        helperRestartPendingFromImageUpdate = true;
        helperPendingImageUrl = helperResult.imageUrl || null;
        console.log(`⏸️ [AUTO-IMAGE] Helper image updated but restart deferred (active jobs=${runningContainers.size}, processing=${processingJobs.size}).`);
      } else if (helperResult.imageUrl) {
        console.log(`🔄 [AUTO-IMAGE] Helper image updated; restarting helper container now.`);
        await recreateViaDockerApi(helperResult.imageUrl);
      }
    } else if (helperRestartPendingFromImageUpdate) {
      const hasActiveWork = runningContainers.size > 0 || processingJobs.size > 0;
      if (!hasActiveWork && helperPendingImageUrl) {
        console.log(`🔄 [AUTO-IMAGE] Executing deferred helper restart after image update.`);
        helperRestartPendingFromImageUpdate = false;
        const imageUrl = helperPendingImageUrl;
        helperPendingImageUrl = null;
        await recreateViaDockerApi(imageUrl);
      }
    }
  } catch (error) {
    console.warn(`⚠️ [AUTO-IMAGE] Automatic image reconcile failed: ${error.message}`);
  }
}

/**
 * Check and update all images (helper, AI service, crawler). Only used when explicitly updating (e.g. not on a timer).
 */
async function checkAllImages(skipHelper = false) {
  console.log(`🔄 Checking all images for updates...`);
  const tasks = [
    ...(skipHelper ? [] : [updateHelperImage()]),
    pullAIServiceImage(),
    checkCrawlerImage(),
    checkCureServiceImageAndRestartIfUpdated()
  ];
  await Promise.allSettled(tasks);
  console.log(`✅ Image check complete`);
}

/** Default container name for the AI service when ai_service_source is self_deployed (helper starts it with short-lived ECR token). */
function getAIServiceContainerName() {
  if (!helperConfig) return 'aionqa-ai-service';
  if (helperConfig.ai_service_source === 'aionqa' || helperConfig.ai_service_source === 'balance') return 'aionqa-ai-service';
  const name = helperConfig.ai_service_container_name;
  return (name && typeof name === 'string' && name.trim()) ? name.trim() : 'aionqa-ai-service';
}
/** Ollama container (default LLM when USE_LM_STUDIO_HOST is not set) */
const OLLAMA_CONTAINER_NAME = 'ollama';

/** Helper config from API (fetched on startup; env overrides) */
let helperConfig = null;
/** When true (shared AionQA helpers), pull/restart managed images on a schedule. When false (self-hosted), only report drift for in-app user notices. */
let autoUpdateManagedImages = true;
/** When autoUpdateManagedImages is false: if true, still pull/restart when backend/registry says images are outdated and there are no active crawl jobs. */
let autoUpdateManagedImagesWhenIdle = true;

function logStartupConfigSnapshot(source = 'api') {
  try {
    const c = helperConfig || {};
    const snapshot = {
      source,
      engine_id: ENGINE_ID,
      runner_id: RUNNER_ID,
      max_concurrent_jobs: c.max_concurrent_jobs ?? null,
      poll_interval: c.poll_interval ?? POLL_INTERVAL,
      ai_service_enabled: c.ai_service_enabled ?? true,
      ollama_enabled: c.ollama_enabled ?? true,
      ai_service_provider: c.ai_service_provider || 'ollama',
      ai_service_url: c.ai_service_url || null,
      ai_services_count: Array.isArray(c.ai_services) ? c.ai_services.length : 0
    };
    console.log(`⚙️ [STARTUP CONFIG] ${JSON.stringify(snapshot)}`);
  } catch (error) {
    console.warn(`⚠️ [STARTUP CONFIG] Failed to print config snapshot: ${error.message}`);
  }
}

/**
 * Fetch helper config from backend. Called on startup so params can be changed in UI and applied after restart.
 * Throws on failure (missing config, backend down, invalid token). Caller must handle or process will exit.
 */
async function fetchHelperConfig() {
  let response;
  try {
    response = await axios.get(
      `${BACKEND_URL}/api/docker-engines/config`,
      { params: { engine_id: ENGINE_ID }, headers: getAuthHeaders(), timeout: 10000 }
    );
  } catch (error) {
    const status = error.response?.status;
    const msg = error.response?.data?.error || error.message;
    if (status === 401 || status === 403) {
      throw new Error(`Invalid or unauthorized ENGINE_TOKEN (${status}). Check Settings > Docker Engines > token.`);
    }
    if (status === 404) {
      throw new Error(`Engine not found or not owned by this token (404). Check ENGINE_ID and backend.`);
    }
    throw new Error(`Helper config unavailable: ${msg || error.message}. Is the backend at ${BACKEND_URL} responding?`);
  }
  if (!response.data || !response.data.config) {
    throw new Error('Backend returned no config (missing response.data.config).');
  }
  helperConfig = response.data.config;
  const au = response.data.auto_update_managed_images;
  const forceAutoUpdate =
    process.env.FORCE_AUTO_UPDATE_MANAGED_IMAGES === 'true' ||
    process.env.FORCE_AUTO_UPDATE_MANAGED_IMAGES === '1';
  autoUpdateManagedImages = forceAutoUpdate ? true : au !== false;
  const disableIdleEnv =
    process.env.DISABLE_IDLE_AUTO_IMAGE_UPDATE === 'true' ||
    process.env.DISABLE_IDLE_AUTO_IMAGE_UPDATE === '1';
  const idleCfg = helperConfig.auto_update_managed_images_when_idle;
  autoUpdateManagedImagesWhenIdle = !disableIdleEnv && idleCfg !== false;
  const provider = helperConfig.ai_service_provider || 'ollama';
  if (forceAutoUpdate) {
    console.log('ℹ️ FORCE_AUTO_UPDATE_MANAGED_IMAGES is set; managed images will auto-pull/restart regardless of API flag.');
  }
  if (disableIdleEnv) {
    console.log('ℹ️ DISABLE_IDLE_AUTO_IMAGE_UPDATE is set; skipping idle-only auto image updates for self-hosted helpers.');
  }
  console.log(
    `✅ Helper config loaded from API (ai_service_provider: ${provider}, ollama_model: ${helperConfig.ollama_model}, auto_update_managed_images: ${autoUpdateManagedImages}, auto_update_managed_images_when_idle: ${autoUpdateManagedImagesWhenIdle})`
  );
  logStartupConfigSnapshot('api');
}

/**
 * Use Ollama as LLM backend (default). Set use_lm_studio_host (API) or USE_LM_STUDIO_HOST (env) to use host LM Studio.
 * When config has not been loaded yet, return false so we do not start Ollama.
 * API config (ai_service_provider) wins over env so UI choice is respected.
 */
function useOllamaAsLLM() {
  if (!helperConfig) return false; // No config loaded yet - do not start Ollama
  const provider = helperConfig.ai_service_provider || 'ollama';
  if (provider === 'lm_studio' || provider === 'openai') return false; // UI chose LM Studio or OpenAI
  if (helperConfig.use_lm_studio_host === true) return false;
  if (process.env.USE_LM_STUDIO_HOST !== undefined) {
    return process.env.USE_LM_STUDIO_HOST !== 'true' && process.env.USE_LM_STUDIO_HOST !== '1';
  }
  return true;
}

/** Whether to run the Ollama container (config + useOllamaAsLLM). When false, helper will stop and remove it. */
function isOllamaEnabled() {
  if (!useOllamaAsLLM()) return false;
  if (process.env.OLLAMA_ENABLED !== undefined) {
    return process.env.OLLAMA_ENABLED === 'true' || process.env.OLLAMA_ENABLED === '1';
  }
  return helperConfig?.ollama_enabled !== false;
}

/** Whether to run the AI service container (config or env AI_SERVICE). When false or ai_service_source is aionqa/balance, helper will not run it. */
function isAIServiceEnabled() {
  if (helperConfig?.ai_service_source === 'aionqa' || helperConfig?.ai_service_source === 'balance') return false;
  if (process.env.AI_SERVICE === 'true' || process.env.AI_SERVICE === '1') return true;
  if (process.env.AI_SERVICE === 'false' || process.env.AI_SERVICE === '0') return false;
  return helperConfig?.ai_service_enabled !== false;
}

/**
 * Model to pull and run in Ollama. From API config or env.
 */
function getOllamaModel() {
  if (process.env.OLLAMA_MODEL) return process.env.OLLAMA_MODEL;
  if (helperConfig && helperConfig.ollama_model) return helperConfig.ollama_model;
  return 'qwen2.5vl:3b';
}

function getConfigNetwork() {
  if (helperConfig && helperConfig.ai_service_network) return helperConfig.ai_service_network;
  return process.env.AI_SERVICE_NETWORK || 'aionqa-network';
}

/**
 * Ensure aionqa-network exists (for AI service and Ollama). Create if missing.
 */
async function ensureAionqaNetwork() {
  const networkName = getConfigNetwork();
  try {
    const networks = await docker.listNetworks();
    const exists = networks.some(n => n.Name === networkName);
    if (exists) return;
    await docker.createNetwork({ Name: networkName });
    console.log(`✅ Created network: ${networkName}`);
  } catch (error) {
    console.warn(`⚠️ Failed to ensure network ${networkName}: ${error.message}`);
  }
}

/**
 * Pull Ollama image if not present.
 */
async function pullOllamaImage() {
  const image = 'ollama/ollama:latest';
  try {
    const images = await docker.listImages();
    const hasImage = images.some(img =>
      img.RepoTags && img.RepoTags.some(tag => tag === image || tag.startsWith('ollama/ollama:'))
    );
    if (hasImage) {
      console.log(`✅ Ollama image found locally`);
      return;
    }
    console.log(`📥 Pulling ${image}...`);
    await new Promise((resolve, reject) => {
      docker.pull(image, (err, stream) => {
        if (err) return reject(err);
        docker.modem.followProgress(stream, (err) => (err ? reject(err) : resolve()));
      });
    });
    console.log(`✅ Ollama image pulled`);
  } catch (error) {
    console.warn(`⚠️ Failed to pull Ollama image: ${error.message}`);
  }
}

/**
 * LM Studio–matching params for Ollama. From API config or env.
 * OLLAMA_MAX_VRAM caps GPU memory so Ollama uses system RAM for the rest (e.g. 7G for 8 GB GPU).
 */
function getOllamaEnv() {
  const c = helperConfig || {};
  const env = [
    `OLLAMA_NUM_PARALLEL=${process.env.OLLAMA_NUM_PARALLEL ?? c.ollama_num_parallel ?? '4'}`,
    `OLLAMA_MAX_LOADED_MODELS=${process.env.OLLAMA_MAX_LOADED_MODELS ?? c.ollama_max_loaded_models ?? '1'}`,
    `OLLAMA_KEEP_ALIVE=${process.env.OLLAMA_KEEP_ALIVE ?? c.ollama_keep_alive ?? '24h'}`,
    `OLLAMA_MAX_QUEUE=${process.env.OLLAMA_MAX_QUEUE ?? c.ollama_max_queue ?? '512'}`,
    `OLLAMA_NUM_BATCH=${process.env.OLLAMA_NUM_BATCH ?? (c.ollama_num_batch != null && c.ollama_num_batch !== '' ? c.ollama_num_batch : '512')}`,
    `OLLAMA_CONTEXT_LENGTH=${process.env.OLLAMA_CONTEXT_LENGTH ?? c.ollama_context_length ?? '4096'}`,
    `OLLAMA_NUM_THREADS=${process.env.OLLAMA_NUM_THREADS ?? c.ollama_num_threads ?? '9'}`
  ];
  const maxVram = process.env.OLLAMA_MAX_VRAM || c.ollama_max_vram;
  if (maxVram) env.push(`OLLAMA_MAX_VRAM=${String(maxVram).trim()}`);
  return env;
}

/**
 * Start Ollama container with GPU and pull the configured model.
 * If container already exists and is running, skip (no recreate) to avoid repeated pulls every reconcile.
 */
async function startOllamaContainer() {
  if (!isOllamaEnabled()) return;
  const networkName = getConfigNetwork();
  const model = getOllamaModel();
  try {
    const containers = await docker.listContainers({ all: true });
    const existing = containers.find(c =>
      c.Names && c.Names.some(n => n.replace(/^\//, '') === OLLAMA_CONTAINER_NAME)
    );
    if (existing && existing.State === 'running') {
      return; // Already running; do not recreate every 60s (avoids repeated image/model pulls)
    }
    if (existing) {
      const oldContainer = docker.getContainer(existing.Id);
      await oldContainer.remove({ force: true }).catch(() => {});
    }
    await pullOllamaImage();
    const hostConfig = {
      NetworkMode: networkName,
      RestartPolicy: { Name: 'unless-stopped' },
      PortBindings: { '11434/tcp': [{ HostPort: '11434' }] },
      Binds: ['ollama-models:/root/.ollama'],
      LogConfig: LOG_CONFIG_JSON_FILE
    };
    const memGb = process.env.OLLAMA_MEMORY_GB || process.env.OLLAMA_CONTAINER_MEMORY_GB || (helperConfig && helperConfig.ollama_memory_gb != null && helperConfig.ollama_memory_gb !== '' ? helperConfig.ollama_memory_gb : null);
    if (memGb != null && memGb !== '' && /^\d+$/.test(String(memGb).trim())) {
      const bytes = Math.floor(Number(memGb) * 1024 * 1024 * 1024);
      hostConfig.Memory = bytes;
      console.log(`📦 Ollama container memory limit: ${memGb} GB`);
    }
    if (process.env.OLLAMA_GPU !== '0' && process.env.OLLAMA_GPU !== 'false') {
      hostConfig.DeviceRequests = [
        { Driver: 'nvidia', Count: -1, Capabilities: [['gpu']] }
      ];
    }

    const container = await docker.createContainer({
      Image: 'ollama/ollama:latest',
      name: OLLAMA_CONTAINER_NAME,
      Env: getOllamaEnv(),
      ExposedPorts: { '11434/tcp': {} },
      HostConfig: hostConfig
    });
    await container.start();
    console.log(`✅ Ollama container started on network ${networkName}`);

    // Wait for Ollama server to be ready then pull the model
    await new Promise(resolve => setTimeout(resolve, 3000));
    try {
      const exec = await container.exec({
        Cmd: ['ollama', 'pull', model],
        AttachStdout: true,
        AttachStderr: true
      });
      await new Promise((resolve, reject) => {
        exec.start({ Detach: false }, (err, stream) => {
          if (err) return reject(err);
          container.modem.demuxStream(stream, process.stdout, process.stderr);
          stream.on('finish', resolve);
          stream.on('error', reject);
        });
      });
      let exitCode = null;
      for (let i = 0; i < 120; i++) {
        await new Promise(r => setTimeout(r, 500));
        const info = await exec.inspect();
        if (info.Running === false) {
          exitCode = info.ExitCode;
          break;
        }
      }
      if (exitCode === 0) {
        console.log(`✅ Ollama model pulled: ${model}`);
      } else if (exitCode != null) {
        console.warn(`⚠️ ollama pull ${model} exited with code ${exitCode}`);
      } else {
        console.warn(`⚠️ ollama pull ${model} still running (timeout)`);
      }
    } catch (pullErr) {
      console.warn(`⚠️ Failed to pull Ollama model ${model}: ${pullErr.message}`);
    }
  } catch (error) {
    console.warn(`⚠️ Failed to start Ollama container: ${error.message}`);
  }
}

/**
 * Stop and remove the Ollama container. Used when user turns "Ollama service" off in UI.
 */
async function stopAndRemoveOllamaContainer() {
  try {
    const containers = await docker.listContainers({ all: true });
    const existing = containers.find(c =>
      c.Names && c.Names.some(n => n.replace(/^\//, '') === OLLAMA_CONTAINER_NAME)
    );
    if (!existing) return;
    const container = docker.getContainer(existing.Id);
    if (existing.State === 'running') {
      await container.stop({ t: 10 }).catch(() => {});
    }
    await container.remove({ force: true }).catch(() => {});
    console.log(`🛑 Ollama container stopped and removed`);
  } catch (error) {
    console.warn(`⚠️ Failed to stop/remove Ollama container: ${error.message}`);
  }
}

/**
 * Stop and remove the AI service container(s). Removes both the current config name and legacy aionqa-ai-service.
 */
async function stopAndRemoveAIServiceContainer() {
  const namesToRemove = [getAIServiceContainerName()];
  if (!namesToRemove.includes('aionqa-ai-service')) namesToRemove.push('aionqa-ai-service');
  try {
    const containers = await docker.listContainers({ all: true });
    for (const containerName of namesToRemove) {
      const existing = containers.find(c => c.Names && c.Names.some(n => n.replace(/^\//, '') === containerName));
      if (!existing) continue;
      const container = docker.getContainer(existing.Id);
      if (existing.State === 'running') {
        await container.stop({ t: 10 }).catch(() => {});
      }
      await container.remove({ force: true }).catch(() => {});
      console.log(`🛑 AI service container stopped and removed: ${containerName}`);
    }
  } catch (error) {
    console.warn(`⚠️ Failed to stop/remove AI service container: ${error.message}`);
  }
}

/**
 * Reconcile service containers with config: start or stop Ollama and AI service to match UI.
 * Called once on startup and when UI sends pending actions (start/stop/update). No timer - Docker handles restarts.
 */
async function reconcileServices() {
  try {
    await fetchHelperConfig();
    const provider = helperConfig?.ai_service_provider || 'ollama';
    const useOllama = useOllamaAsLLM();
    const ollamaEnabled = isOllamaEnabled();
    console.log(`🔄 [RECONCILE] ai_service_provider=${provider} useOllamaAsLLM=${useOllama} isOllamaEnabled=${ollamaEnabled}`);
    if (ollamaEnabled) {
      await startOllamaContainer();
    } else {
      await stopAndRemoveOllamaContainer();
    }
    if (isAIServiceEnabled()) {
      await startAIServiceContainer(true); // config already fetched above
    } else {
      await stopAndRemoveAIServiceContainer();
    }
    if (isCureServiceEnabled()) {
      await startCureServiceContainer(true); // config already fetched above
    } else {
      await stopAndRemoveCureServiceContainer();
    }
  } catch (error) {
    console.warn(`⚠️ Reconcile services failed: ${error.message}`);
  }
}

/**
 * Resolve AI service image URL: env AI_SERVICE_IMAGE_URL, or from backend.
 * Used for both pull and start. Returns null if not AI_SERVICE mode or URL unavailable.
 */
async function getAIServiceImageUrl() {
  if (!isAIServiceEnabled()) return null;

  let imageUrl = process.env.AI_SERVICE_IMAGE_URL;
  if (imageUrl) return imageUrl;

  try {
    const response = await axios.post(
      `${BACKEND_URL}/api/docker-engines/image-url`,
      { image_name: 'aionqa-ai-service:latest' },
      { headers: getAuthHeaders(), timeout: 10000 }
    );
    if (response.data && response.data.image_url) {
      return response.data.image_url;
    }
  } catch (error) {
    console.warn(`⚠️ Failed to get AI service image URL from backend: ${error.message}`);
  }
  // Fallback: common ECR URL
  const region = process.env.AWS_REGION || 'ap-southeast-2';
  const accountId = process.env.AWS_ACCOUNT_ID || '778954507986';
  return `${accountId}.dkr.ecr.${region}.amazonaws.com/aionqa-ai-service:latest`;
}

/**
 * Pull AI service image from ECR using short-lived token from backend.
 * AI service is the common image; only the helper pulls it (with token), not the user.
 * @param {boolean} forcePull - If true, always pull (get token and pull); used on Update so latest image is used.
 */
async function pullAIServiceImage(forcePull = false) {
  const imageUrl = await getAIServiceImageUrl();
  if (!imageUrl) {
    console.log(`ℹ️  AI_SERVICE not enabled, skipping AI service image pull`);
    return;
  }

  try {
    if (!forcePull) {
      console.log(`📥 Checking AI service image: ${imageUrl}`);
      const images = await docker.listImages();
      const localImage = images.find(img => {
        if (!img.RepoTags) return false;
        return img.RepoTags.some(tag => tag === imageUrl || tag.startsWith(imageUrl.split(':')[0] + ':'));
      });
      if (localImage) {
        console.log(`✅ AI service image found locally: ${imageUrl}`);
        return;
      }
    }

    console.log(`📥 Pulling AI service image from ECR (token from backend)...`);
    const tokenData = await getECRPullToken(imageUrl);
    if (!tokenData) {
      throw new Error(`Failed to get ECR token from backend`);
    }
    const authConfig = getECRAuthConfig(tokenData);

    await new Promise((resolve, reject) => {
      docker.pull(imageUrl, { authconfig: authConfig }, (err, stream) => {
        if (err) return reject(err);
        docker.modem.followProgress(stream, (err, output) => {
          if (err) return reject(err);
          console.log(`✅ AI service image pulled successfully`);
          resolve();
        });
      });
    });
  } catch (error) {
    console.warn(`⚠️ Failed to pull AI service image: ${error.message}`);
  }
}

/**
 * Resolve LLM URL: when using Ollama (default) use http://ollama:11434; else API config or env (host LM Studio).
 */
function getLMStudioUrl() {
  if (useOllamaAsLLM()) {
    return 'http://ollama:11434';
  }
  if (process.env.AI_SERVICE_LM_STUDIO_URL || process.env.LM_STUDIO_URL) {
    return process.env.AI_SERVICE_LM_STUDIO_URL || process.env.LM_STUDIO_URL;
  }
  if (helperConfig && helperConfig.ai_service_lm_studio_url) {
    return helperConfig.ai_service_lm_studio_url;
  }
  return 'http://host.docker.internal:1234';
}

/**
 * Build env vars for the AI service container from helper config (ai_service_provider: openai | lm_studio | ollama).
 */
function getAIServiceEnv() {
  const c = helperConfig || {};
  const provider = c.ai_service_provider || 'ollama';
  const base = [
    `BACKEND_URL=${process.env.BACKEND_URL || 'https://api.aionqa.com'}`,
    'NODE_ENV=production'
  ];
  if (provider === 'openai') {
    base.push('AI_MODE=openai');
    if (c.openai_api_key) base.push(`OPENAI_API_KEY=${c.openai_api_key}`);
    if (c.openai_base_url) base.push(`LLM_BASE_URL=${c.openai_base_url}`);
  } else if (provider === 'lm_studio') {
    const url = c.ai_service_lm_studio_url || process.env.AI_SERVICE_LM_STUDIO_URL || 'http://host.docker.internal:1234';
    const model = c.ai_service_lm_studio_model || process.env.LM_STUDIO_MODEL || 'qwen2.5-vl-3b-instruct-img2code';
    base.push('AI_MODE=lm-studio', `LM_STUDIO_URL=${url}`, `LM_STUDIO_MODEL=${model}`);
    if (c.ai_service_lm_studio_api_key) base.push(`LM_STUDIO_API_KEY=${c.ai_service_lm_studio_api_key}`);
  } else {
    // ollama: AI service uses native Ollama API (POST /api/chat)
    const url = c.ai_service_ollama_url || 'http://ollama:11434';
    const model = c.ollama_model || process.env.OLLAMA_MODEL || 'llama3.2';
    base.push('AI_MODE=ollama', `OLLAMA_URL=${url}`, `OLLAMA_MODEL=${model}`);
  }
  return base;
}

/**
 * Start the AI service container (common image pulled by helper with short-lived token).
 * Connects container to AI_SERVICE_NETWORK so helper can reach it at AI_SERVICE_URL.
 * LLM backend comes from helper config: ai_service_provider (openai | lm_studio | ollama).
 * @param {boolean} useCachedConfig - If true, skip fetching config (caller already did, e.g. reconcileServices).
 */
async function startAIServiceContainer(useCachedConfig = false) {
  if (!useCachedConfig) await fetchHelperConfig();
  if (!isAIServiceEnabled()) return;

  const containerName = getAIServiceContainerName();
  const imageUrl = await getAIServiceImageUrl();
  if (!imageUrl) return;

  const networkName = getConfigNetwork();
  try {
    const containers = await docker.listContainers({ all: true });
    const existing = containers.find(c => c.Names && c.Names.some(n => n.replace(/^\//, '') === containerName));
    if (existing && existing.State === 'running') {
      return; // Already running; do not recreate every 60s (avoids repeated image pulls)
    }
    if (existing) {
      const oldContainer = docker.getContainer(existing.Id);
      await oldContainer.stop({ t: 10 }).catch(() => {});
      await oldContainer.remove({ force: true }).catch(() => {});
    }
    await pullAIServiceImage();
    const env = getAIServiceEnv();

    const container = await docker.createContainer({
      Image: imageUrl,
      name: containerName,
      Env: env,
      ExposedPorts: { '5000/tcp': {} },
      HostConfig: {
        PortBindings: { '5000/tcp': [{ HostPort: '5000' }] },
        NetworkMode: networkName,
        RestartPolicy: { Name: 'unless-stopped' },
        LogConfig: LOG_CONFIG_JSON_FILE
      }
    });
    await container.start();
    console.log(`✅ AI service container started: ${containerName} on network ${networkName}`);
  } catch (error) {
    console.warn(`⚠️ Failed to start AI service container: ${error.message}`);
  }
}

/**
 * Get this process's container ID (when running inside Docker).
 * Reads /proc/self/cgroup and parses Docker container id.
 * @returns {string|null} Full or short container ID, or null
 */
function getOwnContainerId() {
  try {
    const cgroup = readFileSync('/proc/self/cgroup', 'utf8');
    const lines = cgroup.split('\n');
    for (const line of lines) {
      const m = line.match(/[0-9]+:[^:]*:(?:\/docker\/)?([a-f0-9]{64}|[a-f0-9]{12})(?:\.scope)?$/);
      if (m) return m[1];
      const m2 = line.match(/[0-9]+:[^:]*:.*\/docker-([a-f0-9]{64}|[a-f0-9]{12})\.scope$/);
      if (m2) return m2[1];
      const m3 = line.match(/[0-9]+:[^:]*:.*\/([a-f0-9]{64}|[a-f0-9]{12})$/);
      if (m3) return m3[1];
    }
    if (hostname() && /^[a-f0-9]{12}$/.test(hostname())) return hostname();
  } catch (_) {}
  return null;
}

/** Compare Docker container IDs from inspect / cgroups / list (12- or 64-char). */
function sameDockerContainerId(a, b) {
  const key = (id) => String(id || '').replace(/^sha256:/i, '').toLowerCase().replace(/^docker-/, '');
  const ka = key(a);
  const kb = key(b);
  if (!ka || !kb) return false;
  const pa = ka.length >= 12 ? ka.slice(0, 12) : ka;
  const pb = kb.length >= 12 ? kb.slice(0, 12) : kb;
  return pa === pb;
}

/**
 * If this process is the new container after a self-update, take over the original name:
 * stop and remove the old container, then rename this container to the original name.
 * Call at startup before registration. No-op if not a replacement.
 */
async function runSelfUpdateTakeoverIfNeeded() {
  const oldId = process.env.OLD_HELPER_CONTAINER_ID;
  const originalName = process.env.ORIGINAL_HELPER_NAME;
  if (!oldId || !originalName) return;
  const myId = getOwnContainerId();
  if (!myId || sameDockerContainerId(myId, oldId)) return;
  try {
    console.log(`🔄 [SELF-UPDATE] Taking over: stopping and removing old container ${oldId}, then renaming to ${originalName}.`);
    const oldContainer = docker.getContainer(oldId);
    try {
      await oldContainer.stop({ t: 10 });
    } catch (_) {}
    await oldContainer.remove({ force: true });
    const self = docker.getContainer(myId);
    await self.rename({ name: originalName });
    console.log(`✅ [SELF-UPDATE] Replaced old container; this container is now ${originalName}.`);
  } catch (err) {
    console.warn(`⚠️ [SELF-UPDATE] Takeover failed: ${err.message}. Continuing with current name.`);
  }
}

/**
 * Remove leftover containers from a failed or partial helper self-update (name pattern: {canonical}-new-{suffix}).
 * Safe to run at startup after takeover; keeps the current container only.
 */
async function removeOrphanedHelperSelfUpdateContainers() {
  const cid = getOwnContainerId();
  if (!cid) return;
  try {
    const data = await docker.getContainer(cid).inspect();
    const myName = normalizeContainerName(data.Name);
    let canonical = process.env.ORIGINAL_HELPER_NAME
      ? normalizeContainerName(process.env.ORIGINAL_HELPER_NAME)
      : null;
    if (!canonical) {
      const m = myName.match(/^(.*)-new-[a-z0-9]+$/i);
      canonical = m ? m[1] : myName;
    }
    if (!canonical) return;
    const escaped = canonical.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp(`^${escaped}-new-[a-z0-9]+$`, 'i');
    const list = await docker.listContainers({ all: true });
    for (const c of list) {
      if (sameDockerContainerId(c.Id, cid)) continue;
      const names = (c.Names || []).map(normalizeContainerName);
      if (!names.some((n) => re.test(n))) continue;
      console.log(`🧹 [SELF-UPDATE] Removing stale helper swap container ${String(c.Id).slice(0, 12)} (${names.join(', ')})`);
      const cont = docker.getContainer(c.Id);
      await cont.stop({ t: 8 }).catch(() => {});
      await cont.remove({ force: true }).catch((e) => {
        console.warn(`⚠️ [SELF-UPDATE] Could not remove stale ${String(c.Id).slice(0, 12)}: ${e.message}`);
      });
    }
  } catch (e) {
    console.warn(`⚠️ [SELF-UPDATE] Stale swap container cleanup: ${e.message}`);
  }
}

/**
 * Build createContainer options from current container inspect, with new image.
 * Clones Config (Cmd, Env, WorkingDir, Labels) and HostConfig so the new container has the same params as compose.
 */
function buildCreateOptsFromInspect(data, newImageUrl, newName, extraEnv) {
  const config = data.Config || {};
  const hostConfig = data.HostConfig || {};
  const env = Array.isArray(config.Env) ? [...config.Env] : [];
  if (extraEnv) {
    for (const [k, v] of Object.entries(extraEnv)) env.push(`${k}=${v}`);
  }
  const createOpts = {
    Image: newImageUrl,
    Cmd: config.Cmd,
    Env: env,
    WorkingDir: config.WorkingDir || '',
    name: newName
  };
  if (config.Labels && Object.keys(config.Labels).length > 0) {
    createOpts.Labels = config.Labels;
  }
  const hc = {
    NetworkMode: hostConfig.NetworkMode || 'default',
    Binds: hostConfig.Binds || [],
    PortBindings: hostConfig.PortBindings || {},
    RestartPolicy: hostConfig.RestartPolicy || { Name: 'unless-stopped' },
    AutoRemove: false,
    LogConfig: hostConfig.LogConfig && hostConfig.LogConfig.Type === 'json-file' ? hostConfig.LogConfig : LOG_CONFIG_JSON_FILE
  };
  if (hostConfig.Mounts && hostConfig.Mounts.length > 0) hc.Mounts = hostConfig.Mounts;
  if (hostConfig.ExtraHosts) hc.ExtraHosts = hostConfig.ExtraHosts;
  if (hostConfig.CapAdd && hostConfig.CapAdd.length > 0) hc.CapAdd = hostConfig.CapAdd;
  if (hostConfig.CapDrop && hostConfig.CapDrop.length > 0) hc.CapDrop = hostConfig.CapDrop;
  if (hostConfig.Sysctls && Object.keys(hostConfig.Sysctls).length > 0) hc.Sysctls = hostConfig.Sysctls;
  createOpts.HostConfig = hc;
  if (data.NetworkSettings?.Networks && Object.keys(data.NetworkSettings.Networks).length > 0) {
    createOpts.NetworkingConfig = {
      EndpointsConfig: Object.fromEntries(
        Object.entries(data.NetworkSettings.Networks).map(([net, cfg]) => [
          net,
          { Aliases: cfg.Aliases || [], Links: cfg.Links || [] }
        ])
      )
    };
  }
  return createOpts;
}

/**
 * Recreate this container using Docker API only: same params as current (as if composed), new image.
 * Creates a new container with same config, starts it with env so it takes over this container's name, then exits.
 */
async function recreateViaDockerApi(newImageUrl) {
  const cid = getOwnContainerId();
  if (!cid || !newImageUrl) return false;
  try {
    const container = docker.getContainer(cid);
    const data = await container.inspect();
    const name = (data.Name || '').replace(/^\//, '') || 'aionqa-helper';
    const newName = `${name}-new-${Date.now().toString(36)}`;
    const createOpts = buildCreateOptsFromInspect(data, newImageUrl, newName, {
      OLD_HELPER_CONTAINER_ID: cid,
      ORIGINAL_HELPER_NAME: name
    });
    console.log(`🔄 [SELF-UPDATE] Creating new container from ${newImageUrl} (same params as current), temp name ${newName}...`);
    const newContainer = await docker.createContainer(createOpts);
    await newContainer.start();
    // Critical: compose/Kubernetes often use restart=unless-stopped. Exiting this process would otherwise
    // stop the old container and Docker would immediately respawn it → two helpers (old + new temp name).
    try {
      await container.update({ RestartPolicy: { Name: 'no', MaximumRetryCount: 0 } });
      console.log(`🔄 [SELF-UPDATE] Disabled auto-restart on previous container so the daemon will not respawn it after exit.`);
    } catch (polErr) {
      console.warn(
        `⚠️ [SELF-UPDATE] Could not set restart policy to "no" on previous container: ${polErr.message}. ` +
          `If you see two helpers, stop the duplicate manually or upgrade Docker Engine.`
      );
    }
    console.log(`✅ [SELF-UPDATE] New container started; it will take over name "${name}". Exiting.`);
    process.exit(0);
  } catch (err) {
    console.warn(`⚠️ [SELF-UPDATE] Docker API recreate failed: ${err.message}.`);
    return false;
  }
}

/**
 * Update helper image (pull if needed) and recreate container via Docker API with same params (no compose).
 */
async function runHelperSelfUpdate() {
  console.log(`🔄 [SELF-UPDATE] Updating helper image (pull then recreate container with same params).`);
  const result = await updateHelperImage();
  if (result.safeToRestart && result.imageUrl) {
    console.log(`✅ [SELF-UPDATE] New image downloaded. Recreating container (Docker API, same config)...`);
    const done = await recreateViaDockerApi(result.imageUrl);
    if (!done) {
      console.log(`ℹ️ [SELF-UPDATE] Recreate failed. Restart the container manually to use the new image.`);
    }
  } else {
    console.warn(`⚠️ [SELF-UPDATE] Could not update: ${result.reason || 'no image URL'}. Restart container manually if needed.`);
  }
}

/**
 * Run pending container actions from the backend (stop/start/remove/update). Helper polls, executes, then acks.
 */
async function runPendingContainerActions() {
  try {
    const response = await axios.get(
      `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/pending-actions`,
      { headers: getAuthHeaders(), timeout: 5000 }
    );
    const pending = response.data?.pending || [];
    if (pending.length === 0) return;
    const allIds = pending.map(p => p.id).filter(Boolean);
    // Ack immediately so backend clears the list (stops re-sending same commands); then execute
    if (allIds.length > 0) {
      try {
        await axios.post(
          `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/pending-actions/ack`,
          { ids: allIds },
          { headers: getAuthHeaders(), timeout: 10000 }
        );
        console.log(`[CONTAINER-ACTIONS] Acked ${allIds.length} action(s); list cleared on backend.`);
      } catch (ackErr) {
        console.warn(`[CONTAINER-ACTIONS] Ack failed (list may not be cleared): ${ackErr.message}`);
      }
    }
    // Deduplicate: execute each (action, container) once
    const key = (a, name) => `${a}:${normalizeContainerName(name).toLowerCase()}`;
    const byKey = new Map();
    for (const p of pending) {
      const k = key(p.action, p.container_name);
      if (!byKey.has(k)) byKey.set(k, { action: p.action, container_name: p.container_name });
      byKey.get(k).ids = byKey.get(k).ids || [];
      byKey.get(k).ids.push(p.id);
    }
    const toRun = [...byKey.values()];
    console.log(`[CONTAINER-ACTIONS] Executing ${toRun.length} unique action(s):`, toRun.map(p => ({ action: p.action, container: p.container_name })));
    let selfUpdateRequested = false;
    for (const { action, container_name: name } of toRun) {
      const normalizedName = normalizeContainerName(name).toLowerCase();
      try {
        const containers = await docker.listContainers({ all: true });
        const found = findContainerByName(containers, normalizedName);
        if (action === 'stop') {
          if (found) {
            const container = docker.getContainer(found.Id);
            await container.stop({ t: 10 }).catch(() => {});
            console.log(`🛑 Stopped container: ${name}`);
          }
        } else if (action === 'remove') {
          if (found) {
            const container = docker.getContainer(found.Id);
            if (found.State === 'running') await container.stop({ t: 10 }).catch(() => {});
            await container.remove({ force: true }).catch(() => {});
            console.log(`🗑️ Removed container: ${name}`);
          }
        } else if (action === 'start') {
          if (normalizedName.includes('ollama')) {
            await fetchHelperConfig();
            if (isOllamaEnabled()) {
              await startOllamaContainer();
              console.log(`▶️ Started Ollama`);
            } else {
              console.log(`⏭️ Skipping start Ollama: config says use LM Studio / OpenAI`);
            }
          } else if (normalizedName.includes('ai-service') || normalizedName.includes('aionqa-ai-service')) {
            await fetchHelperConfig();
            if (normalizedName === getAIServiceContainerName().toLowerCase() || normalizedName.includes('aionqa-ai-service')) {
              await startAIServiceContainer();
              console.log(`▶️ Started AI service`);
            }
          } else if (normalizedName.includes('aionqa-cure') || normalizedName === getCureServiceContainerName().toLowerCase()) {
            await fetchHelperConfig();
            const started = await startCureServiceContainer(true);
            if (started) {
              console.log(`▶️ Started cure service`);
            } else {
              console.log(`⏭️ Cure start skipped or failed`);
            }
          } else if (found) {
            const container = docker.getContainer(found.Id);
            await container.start();
            console.log(`▶️ Started container: ${name}`);
          }
        } else if (action === 'update') {
          if (normalizedName === HELPER_CONTAINER_NAME || normalizedName.includes('aionqa-helper') || normalizedName.includes('docker-engine')) {
            selfUpdateRequested = true;
            console.log(`🔄 [SELF-UPDATE] Update requested for helper; will update image.`);
          } else if (normalizedName.includes('ollama')) {
            await stopAndRemoveOllamaContainer();
            await startOllamaContainer();
            console.log(`🔄 Updated Ollama`);
          } else if (normalizedName.includes('ai-service') || normalizedName.includes('aionqa-ai-service')) {
            await fetchHelperConfig();
            if (normalizedName === getAIServiceContainerName().toLowerCase() || normalizedName.includes('aionqa-ai-service')) {
              await stopAndRemoveAIServiceContainer();
              await pullAIServiceImage(true);
              await startAIServiceContainer(true);
              console.log(`🔄 Updated AI service`);
            }
          } else if (normalizedName.includes('aionqa-cure') || normalizedName === getCureServiceContainerName().toLowerCase()) {
            if (!found) {
              console.log(`⏭️ Skipping cure update: container not found (${name})`);
            } else {
              const imageUrl = await getCureImageUrl();
              if (!imageUrl) {
                console.warn(`⚠️ Cure update skipped: no image URL`);
              } else {
                const pulled = await pullManagedImage(imageUrl, true, 'cure');
                if (pulled) {
                  const targetName = normalizeContainerName(found.Names?.[0]) || getCureServiceContainerName();
                  const updated = await recreateContainerFromExisting(found.Id, imageUrl, targetName, 'cure');
                  if (updated) console.log(`🔄 Updated cure service`);
                }
              }
            }
          } else if (found) {
            const container = docker.getContainer(found.Id);
            if (found.State === 'running') await container.stop({ t: 10 }).catch(() => {});
            await container.remove({ force: true }).catch(() => {});
            console.log(`🔄 Removed container for update: ${name} (recreate not implemented for generic containers)`);
          }
        }
      } catch (err) {
        console.warn(`⚠️ Failed to run action ${action} on ${name}:`, err.message);
      }
    }
    if (selfUpdateRequested) {
      await runHelperSelfUpdate();
    }
  } catch (err) {
    if (err.response?.status === 404) {
      console.warn(`[CONTAINER-ACTIONS] Pending actions 404 - engine_id=${ENGINE_ID} may not match token's runner`);
    } else {
      console.warn(`[CONTAINER-ACTIONS] Pending container actions: ${err.message}`);
    }
  }
}

/**
 * Run pending AI-only requests from the backend (no crawler container).
 * Helper polls, executes LLM call via configured AI service, posts result, then acks.
 */
function resolveAiServiceUrl(choice = 'default', preferredUrl = '') {
  const c = String(choice || 'default').toLowerCase();
  if (c === 'shared') return 'https://ai.aionqa.com';
  if (c === 'specific' && preferredUrl) return String(preferredUrl);
  // default: use helper config's ai_services first (self-hosted / local), else fall back to local container name.
  const fromConfig = helperConfig?.ai_services && Array.isArray(helperConfig.ai_services) ? helperConfig.ai_services : [];
  const first = fromConfig.find(s => s && typeof s.url === 'string' && s.url.trim());
  return first?.url || 'http://aionqa-ai-service:5000';
}

async function runPendingAIRequests() {
  try {
    const response = await axios.get(
      `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/pending-ai-requests`,
      { headers: getAuthHeaders(), timeout: 5000 }
    );
    const pending = response.data?.pending || [];
    if (!Array.isArray(pending) || pending.length === 0) return;

    // Execute sequentially to avoid overloading the AI service.
    for (const reqItem of pending) {
      const requestId = reqItem?.id;
      const projectId = reqItem?.project_id;
      const jobId = reqItem?.job_id;
      const model = reqItem?.model || 'gpt-5-mini';
      const aiChoice = reqItem?.ai_service_choice || 'default';
      const preferredUrl = reqItem?.preferred_ai_service_url || '';
      const messages = Array.isArray(reqItem?.messages) ? reqItem.messages : [];

      if (!requestId || !projectId || !jobId || messages.length === 0) {
        console.warn(`[HELPER-AI] Skipping invalid AI request payload:`, {
          requestId, hasProjectId: !!projectId, hasJobId: !!jobId, messagesCount: messages.length
        });
        continue;
      }

      const aiServiceUrl = resolveAiServiceUrl(aiChoice, preferredUrl);
      console.log(`[HELPER-AI] Executing AI request ${requestId} via ${aiServiceUrl} (choice=${aiChoice}, model=${model})`);

      let success = false;
      let responseText = '';
      let errorText = '';

      try {
        // 1) Mint AI service token for this job (required by aionqa-ai-service)
        const tokenResp = await axios.post(
          `${BACKEND_URL}/api/ai/issue-token`,
          { project_id: projectId, job_id: jobId },
          { headers: getAuthHeaders(), timeout: 10000 }
        );
        const aiToken = tokenResp.data?.token;
        if (!aiToken) throw new Error('No ai service token returned');

        // 2) Call AI service
        const aiResp = await axios.post(
          `${aiServiceUrl.replace(/\/$/, '')}/api/ai/chat`,
          { project_id: projectId, job_id: jobId, messages, model },
          {
            headers: { Authorization: `Bearer ${aiToken}` },
            timeout: 120000
          }
        );
        responseText = String(aiResp.data?.output_text || aiResp.data?.output?.[0]?.text || '').trim();
        if (!responseText) {
          // fallback: keep small JSON
          responseText = JSON.stringify(aiResp.data || {}).slice(0, 4000);
        }
        success = true;
      } catch (err) {
        errorText = err.response?.data?.error?.message || err.response?.data?.error || err.message || 'AI request failed';
        console.warn(`[HELPER-AI] Request ${requestId} failed: ${errorText}`);
      }

      // 3) Post result to backend
      try {
        await axios.post(
          `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/ai-requests/${requestId}/result`,
          { job_id: jobId, success, response: responseText, error: success ? null : errorText },
          { headers: getAuthHeaders(), timeout: 10000 }
        );
      } catch (postErr) {
        console.warn(`[HELPER-AI] Failed to post result for ${requestId}: ${postErr.message}`);
      }

      // 4) Ack so it is removed from queue
      try {
        await axios.post(
          `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/pending-ai-requests/ack`,
          { ids: [requestId] },
          { headers: getAuthHeaders(), timeout: 10000 }
        );
      } catch (ackErr) {
        console.warn(`[HELPER-AI] Ack failed for ${requestId}: ${ackErr.message}`);
      }
    }
  } catch (err) {
    if (err.response?.status === 404) return; // backend not yet updated
    console.warn(`[HELPER-AI] Pending AI requests: ${err.message}`);
  }
}

/**
 * Main loop
 */
async function main() {
  console.log(
    `🚀 AionQA helper main() starting | HELPER_GIT_COMMIT=${HELPER_GIT_COMMIT} | version=${HELPER_VERSION} | ENGINE_ID=${ENGINE_ID}`,
  );
  // If we are the new container after a self-update, take over the original name (stop old, rename self)
  await runSelfUpdateTakeoverIfNeeded();
  await removeOrphanedHelperSelfUpdateContainers();

  // Register engine first so it exists in the backend before we fetch config (config endpoint returns 404 otherwise)
  let registered = false;
  let registrationRetries = 10;
  while (!registered && registrationRetries > 0) {
    try {
      await registerEngine();
      registered = true;
    } catch (error) {
      registrationRetries--;
      if (registrationRetries === 0) {
        console.error(`❌ Failed to register engine after multiple attempts.`);
        console.warn(`⚠️  Continuing anyway - engine may already be registered in database.`);
        console.warn(`⚠️  Polling will continue, but heartbeats may fail.`);
        registered = false;
        break;
      }
      console.log(`   Waiting 5 seconds before retry...`);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }

  if (registered) {
    console.log(`✅ Engine registration successful`);
  } else {
    console.warn(`⚠️  Engine registration failed - continuing with polling (engine may be pre-registered)`);
  }

  // Require config after registration: missing config, backend down, or wrong token → throw and exit
  await fetchHelperConfig();

  // Compare local images to registry digests early so admin UI gets `metrics.image_version_checks` on first heartbeat
  await refreshImageVersionCheckCache('startup').catch((err) =>
    console.warn(`⚠️ [IMAGE-CHECK] Startup comparison skipped: ${err.message}`)
  );

  await ensureAionqaNetwork().catch(err => console.warn('⚠️ Ensure network failed:', err.message));
  // Load config and start Ollama / AI service once (reconcileServices fetches config; no need to fetch twice)
  await reconcileServices();

  // Retry reconcile once after delay so Ollama/AI service start even if first run had transient failures (config not ready, image pull slow, etc.)
  const STARTUP_RECONCILE_DELAY_MS = 25 * 1000;
  setTimeout(() => {
    console.log(`🔄 [STARTUP] Retrying reconcile (Ollama / AI service) after ${STARTUP_RECONCILE_DELAY_MS / 1000}s...`);
    reconcileServices().catch(err => console.warn('⚠️ [STARTUP] Delayed reconcile failed:', err.message));
  }, STARTUP_RECONCILE_DELAY_MS);

  // Send heartbeat every 30 seconds; one immediate beat so DB has metrics (including image_version_checks) without waiting
  sendHeartbeat().catch(() => {});
  setInterval(sendHeartbeat, 30000);
  // No periodic reconcile: Docker handles restarts. Start/stop/update only when UI sends pending actions.

  setInterval(cleanupContainers, 30 * 1000);
  await cleanupContainers();

  // Automatic image reconciliation: helper controls image lifecycle and restarts affected services.
  setInterval(() => {
    runAutomaticImageReconcile().catch(() => {});
  }, IMAGE_CHECK_INTERVAL_MS);
  setTimeout(() => {
    runAutomaticImageReconcile().catch(() => {});
  }, 15000);

  // Main polling loop
  let pollCount = 0;
  while (true) {
    try {
      runPendingContainerActions().catch(() => {});
      runPendingAIRequests().catch(() => {});
      pollCount++;
      const job = await pollForJobs();
      
      if (job) {
        // Atomic check-and-set: if job is already being processed, skip it
        if (processingJobs.has(job.job_id)) {
          console.log(`⏭️  [MAIN LOOP] Job ${job.job_id} is already being processed, skipping...`);
          await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL));
          continue;
        }
        
        // Mark job as being processed IMMEDIATELY (before any async operations)
        // This prevents race conditions where the same job is polled twice
        processingJobs.add(job.job_id);
        console.log(`🔒 [MAIN LOOP] Locked job ${job.job_id} for processing`);
        
        console.log(`📋 [MAIN LOOP] Received job: ${job.job_id}`);
        console.log(`   Job type: ${job.job_type || 'unknown'}`);
        console.log(`   Image: ${job.image_url || 'unknown'}`);
        console.log(`   Full job object keys:`, Object.keys(job).join(', '));
        
        // Start container (pulls job image if needed); no periodic image check - update only when UI explicitly requests
        startCrawlerContainer(job)
          .then(() => {
            console.log(`✅ [MAIN LOOP] Successfully started container for job ${job.job_id}`);
            // Job is removed from processingJobs in startCrawlerContainer when container starts
          })
          .catch((containerError) => {
            console.error(`❌ [MAIN LOOP] Failed to start container for job ${job.job_id}:`, containerError.message);
            console.error(`   Stack:`, containerError.stack);
            // Remove from processing set on error so it can be retried
            processingJobs.delete(job.job_id);
            console.log(`🔓 [MAIN LOOP] Unlocked job ${job.job_id} due to error`);
          });
        
        // Continue polling immediately (don't wait for container to start)
        // This prevents blocking and allows processing multiple jobs
        // Small delay to prevent tight loop if backend keeps returning same job
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else {
        // Log occasionally when no jobs (every 12 polls = 1 minute)
        if (pollCount % 12 === 0) {
          console.log(`⏳ [MAIN LOOP] No jobs available (poll #${pollCount}, polling every ${POLL_INTERVAL}ms)`);
        }
      }
    } catch (error) {
      console.error(`❌ [MAIN LOOP] Error in main loop:`, error.message);
      console.error(`   Stack:`, error.stack);
    }

    // Wait before next poll
    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL));
  }
}

// Handle graceful shutdown
// Shared unregister function with timeout and error handling
async function unregisterEngine() {
  try {
    await axios.post(
      `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/unregister`,
      {},
      { 
        headers: getAuthHeaders(),
        timeout: 5000 // 5 second timeout
      }
    );
    console.log(`✅ Engine unregistered successfully`);
  } catch (error) {
    // Don't log socket hang up as error - it's common during shutdown
    if (error.code === 'ECONNRESET' || error.code === 'ETIMEDOUT' || error.message?.includes('socket hang up')) {
      console.log(`ℹ️  Backend connection closed during unregister (expected during shutdown)`);
    } else {
      console.error(`⚠️ Failed to unregister engine:`, error.message);
    }
  }
}

process.on('SIGTERM', async () => {
  console.log('🛑 Shutting down engine...');
  await unregisterEngine();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('🛑 Shutting down engine...');
  await unregisterEngine();
  process.exit(0);
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
  // Don't exit - log and continue
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error);
  // Don't exit immediately - log and try to continue
});

// Start main loop
main().catch(error => {
  console.error(`❌ Fatal error in main:`, error.message);
  console.error(`   Stack:`, error.stack);
  process.exit(1);
});

