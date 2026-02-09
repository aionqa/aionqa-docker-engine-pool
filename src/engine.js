/**
 * Docker Engine Pool Manager
 * Connects to backend and manages Docker containers for crawling jobs
 */

import Docker from 'dockerode';
import axios from 'axios';
import { hostname, cpus } from 'os';
import { randomUUID } from 'crypto';
import { execSync } from 'child_process';

// Default to 'back' service name for docker-compose networks
// In Docker Compose, services are resolved by service name, not container name
const BACKEND_URL = process.env.BACKEND_URL || 'http://back:4000';
const ENGINE_TOKEN = process.env.ENGINE_TOKEN; // Token for authenticating with backend
const SERVICE_KEY = process.env.SERVICE_KEY; // Optional service key for image URL retrieval
const ENGINE_ID = process.env.ENGINE_ID || `docker-engine-${hostname()}-${randomUUID().substring(0, 8)}`;
const POLL_INTERVAL = parseInt(process.env.POLL_INTERVAL || '5000'); // 5 seconds
const DOCKER_SOCKET = process.env.DOCKER_SOCKET || '/var/run/docker.sock';
// ECR repositories are public - no AWS credentials needed
// Helper uses ENGINE_TOKEN (project credentials) for backend authentication only

// Validate engine token (required for job polling)
if (!ENGINE_TOKEN) {
  console.error('❌ ENGINE_TOKEN environment variable is required');
  console.error('    Generate a token in the UI: Settings > Docker Engines > Generate Token');
  process.exit(1);
}

// SERVICE_KEY is optional - if provided, will request image URL from backend
// If not provided, image URL comes from job response

console.log(`🐳 Docker Engine ID: ${ENGINE_ID}`);
console.log(`🔗 Backend URL: ${BACKEND_URL}`);
console.log(`⏱️  Poll interval: ${POLL_INTERVAL}ms`);
console.log(`🔌 Docker socket: ${DOCKER_SOCKET}`);
// Version info for image update verification
const HELPER_VERSION = process.env.HELPER_VERSION || process.env.IMAGE_TAG || 'unknown';
const HELPER_BUILD_DATE = process.env.BUILD_DATE || new Date().toISOString();
const HELPER_CONTAINER_NAME = (process.env.HELPER_CONTAINER_NAME || process.env.CONTAINER_NAME || 'aionqa-helper').toLowerCase().replace(/^\//, '');
const HELPER_IMAGE = process.env.HELPER_IMAGE || 'public.ecr.aws/k0f6y3u1/aionqa-docker-engine-pool:latest';
console.log(`🏷️  Helper Version: ${HELPER_VERSION}`);
console.log(`📅 Helper Build Date: ${HELPER_BUILD_DATE}`);

// Initialize Docker client
// Note: We'll set authconfig per-pull, not globally
const docker = new Docker({ socketPath: DOCKER_SOCKET });

// Track running containers (containerId -> jobId)
const runningContainers = new Map();

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
 * Register this engine with the backend
 */
async function registerEngine() {
  let retries = 5;
  let delay = 2000; // Start with 2 seconds
  
  while (retries > 0) {
    try {
      const response = await axios.post(
        `${BACKEND_URL}/api/docker-engines/register`,
        {
          engine_id: ENGINE_ID,
          hostname: hostname(),
          docker_socket: DOCKER_SOCKET,
          status: 'active'
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
  if (/aionqa|ollama|unified-crawler|aionqa-ai-service|aionqa-docker-engine/.test(names)) return true;
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
        const stats = await getContainerStatsOnce(container);
        const parsed = stats ? parseStatsBlob(stats) : {};
        out.containers.push({
          id: c.Id.substring(0, 12),
          name: displayName,
          state: c.State,
          cpu_pct: parsed.cpu_pct != null ? Math.round(parsed.cpu_pct * 10) / 10 : null,
          memory_usage_bytes: parsed.memory_usage,
          memory_limit_bytes: parsed.memory_limit,
          image: c.Image || ''
        });
      } catch (_) {
        out.containers.push({
          id: c.Id.substring(0, 12),
          name: displayName,
          state: c.State,
          cpu_pct: null,
          memory_usage_bytes: null,
          memory_limit_bytes: null,
          image: c.Image || ''
        });
      }
    }
  } catch (_) {
    out.containers = [];
  }
  return out;
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
    }
    await axios.post(
      `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/heartbeat`,
      {
        status: 'active',
        running_containers: runningContainers.size,
        metrics,
        version: HELPER_VERSION,
        build_date: HELPER_BUILD_DATE
      },
      { headers: getAuthHeaders(), timeout: 15000 }
    );
  } catch (error) {
    console.error(`⚠️ Heartbeat failed:`, error.message);
  }
}

/**
 * Poll backend for jobs assigned to this engine
 */
async function pollForJobs() {
  try {
    console.log(`[POLL] Polling for jobs at ${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/jobs/next`);
    const response = await axios.get(
      `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/jobs/next`,
      {
        headers: getAuthHeaders(),
        timeout: 10000
      }
    );

    console.log(`[POLL] Response status: ${response.status}`);
    console.log(`[POLL] Response data:`, JSON.stringify(response.data).substring(0, 500));

    if (response.data && response.data.restart_requested) {
      console.log(`🔄 [POLL] Restart requested; updating helper image then recreating container.`);
      const result = await updateHelperImage();
      if (result.safeToRestart) {
        console.log(`✅ [POLL] Helper image updated.`);
        const recreated = runComposeRecreate();
        if (!recreated) {
          console.log(`ℹ️ [POLL] Set COMPOSE_PROJECT_DIR to recreate automatically. Otherwise run: docker compose up -d --force-recreate`);
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

    // Create container
    // Build environment variables - match localDockerTaskService.js configuration
    // AI_SERVICE_URL is configurable via helper environment or defaults to docker-compose service name
    const aiServiceUrl = process.env.AI_SERVICE_URL || 'http://aionqa-ai-service:5000';
    const containerEnv = [
      `JOB_ID=${job_id}`,
      `JOB_TYPE=${job_type || (job.job_type || (job.type === 'login' ? 'login' : 'crawl'))}`, // CRITICAL: Pass job type explicitly
      `TASK_MODE=single-job`, // Signal to crawler to exit after this job
      `BACKEND_URL=${backend_url || BACKEND_URL}`,
      `RUNNER_TOKEN=${runner_token}`, // Service-managed token from backend
      `RUNNER_TYPE=service-managed`, // Mark as service-managed crawler
      `RUNNER_ID=container-${job_id}`,
      `AI_SERVICE_URL=${aiServiceUrl}`, // AI service URL (no OpenAI key needed in crawler)
      // CRITICAL: AI_SERVICE_TOKEN must NOT be passed to crawlers for security
      // Crawlers authenticate to AI service via RUNNER_TOKEN or other method
      `POLL_INTERVAL=3000`,
      `NODE_ENV=${process.env.NODE_ENV || 'development'}`,
      ...Object.entries(environment_vars || {}).map(([key, value]) => `${key}=${value}`)
    ];
    
    // Generate container name: aionqa-crawler-job-{jobId}
    const containerName = `aionqa-crawler-job-${job_id}`;
    
    // Clean up any existing container with the same name (from previous failed attempts)
    try {
      const existingContainer = docker.getContainer(containerName);
      const containerInfo = await existingContainer.inspect();
      if (containerInfo) {
        console.log(`🧹 Cleaning up existing container: ${containerName}`);
        try {
          await existingContainer.stop();
        } catch (stopError) {
          // Container might already be stopped, ignore
        }
        await existingContainer.remove();
        console.log(`✅ Removed existing container: ${containerName}`);
      }
    } catch (notFoundError) {
      // Container doesn't exist, that's fine - continue
    }
    
    const container = await docker.createContainer({
      Image: image_url,
      name: containerName, // Use consistent naming pattern
      Env: containerEnv,
      HostConfig: {
        ShmSize: 2147483648, // 2GB for Chromium
        NetworkMode: (helperConfig && helperConfig.crawler_network) || process.env.CRAWLER_NETWORK || 'aionqa-network',
        AutoRemove: false, // Helper removes containers when not running (see monitorContainer + cleanupContainers)
        RestartPolicy: { Name: 'no' }, // Don't restart
        Binds: ['/var/run/docker.sock:/var/run/docker.sock'] // Mount docker socket for video recording
      },
      Labels: {
        'aionqa.job_id': job_id,
        'aionqa.engine_id': ENGINE_ID
      }
    });

    // Start container
    await container.start();
    console.log(`✅ Container started: ${container.id.substring(0, 12)}`);

    // Wait a moment and check if container is still running
    await new Promise(resolve => setTimeout(resolve, 1000));
    const containerInfo = await container.inspect();
    if (containerInfo.State.Status === 'exited') {
      const exitCode = containerInfo.State.ExitCode;
      const error = containerInfo.State.Error || 'Unknown error';
      console.error(`❌ Container exited immediately with code ${exitCode}: ${error}`);
      
      // Remove from processing set on error
      processingJobs.delete(job_id);
      
      // Try to get container logs
      try {
        const logs = await container.logs({ stdout: true, stderr: true, tail: 50 });
        console.error(`Container logs:`, logs.toString());
      } catch (logError) {
        console.error(`Failed to get container logs:`, logError.message);
      }

      // Remove container so it does not stay on the host
      try {
        await container.remove();
        console.log(`🧹 Removed failed container ${containerName}`);
      } catch (removeError) {
        console.warn(`⚠️ Failed to remove container: ${removeError.message}`);
      }

      throw new Error(`Container exited immediately with code ${exitCode}: ${error}`);
    }

    // Track container
    runningContainers.set(container.id, job_id);
    
    // Remove from processing set once container is successfully started (job is now running)
    processingJobs.delete(job_id);

    // Monitor container
    monitorContainer(container, job_id);

    // Notify backend that container started
    await axios.post(
      `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/jobs/${job_id}/started`,
      {
        container_id: container.id
      },
      { headers: getAuthHeaders() }
    );

    return container.id;
  } catch (error) {
    console.error(`❌ Failed to start container for job ${job_id}:`, error.message);
    
    // Notify backend of failure
    try {
      await axios.post(
        `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/jobs/${job_id}/failed`,
        {
          error: error.message
        },
        { headers: getAuthHeaders() }
      );
    } catch (notifyError) {
      console.error(`❌ Failed to notify backend of container failure:`, notifyError.message);
    }
    
    throw error;
  }
}

/**
 * Monitor container status
 */
async function monitorContainer(container, jobId) {
  try {
    const containerInfo = await container.inspect();
    
    if (containerInfo.State.Status === 'exited') {
      runningContainers.delete(container.id);
      
      const exitCode = containerInfo.State.ExitCode;
      console.log(`📦 Container ${container.id.substring(0, 12)} exited with code ${exitCode} for job ${jobId}`);
      
      // Notify backend
      try {
        await axios.post(
          `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/jobs/${jobId}/completed`,
          {
            container_id: container.id,
            exit_code: exitCode
          },
          { headers: getAuthHeaders() }
        );
      } catch (error) {
        console.error(`⚠️ Failed to notify backend of container completion:`, error.message);
      }

      // Remove exited container so it does not stay on the host
      try {
        await container.remove();
        console.log(`🧹 Removed exited container ${container.id.substring(0, 12)} for job ${jobId}`);
      } catch (removeError) {
        console.warn(`⚠️ Failed to remove exited container: ${removeError.message}`);
      }
    }
  } catch (error) {
    console.error(`⚠️ Error monitoring container:`, error.message);
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
          await container.remove();
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
        const images = await docker.listImages();
        const localImage = images.find(img => {
          if (!img.RepoTags) return false;
          const matchesTag = img.RepoTags.some(tag => tag === imageUrl || tag.startsWith(imageUrl.split(':')[0] + ':'));
          if (!matchesTag) return false;
          const idNorm = (img.Id || '').replace(/^sha256:/, '');
          const shaNorm = (imageSha || '').replace(/^sha256:/, '');
          return idNorm === shaNorm || (idNorm && shaNorm && (idNorm.startsWith(shaNorm) || shaNorm.startsWith(idNorm)));
        });
        if (localImage) {
          console.log(`✅ Helper image is up to date (SHA matches)`);
          return { safeToRestart: true };
        }
      } catch (checkError) {
        console.error(`❌ Error comparing SHA: ${checkError.message}; skipping SHA check, will pull`);
      }
    } else {
      console.warn(`⚠️ Backend did not return image_sha; pulling image to update (no skip-if-current check)`);
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
    return { safeToRestart: true };
  } catch (error) {
    console.warn(`⚠️ Failed to update helper image: ${error.message}`);
    return { safeToRestart: false, reason: error.message };
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
        const images = await docker.listImages();
        const localImage = images.find(img => {
          if (!img.RepoTags) return false;
          const matchesTag = img.RepoTags.some(tag => tag === imageUrl || tag.startsWith(imageUrl.split(':')[0] + ':'));
          if (!matchesTag) return false;
          // Check if SHA matches
          return img.Id === imageSha || img.Id.startsWith(imageSha.replace('sha256:', ''));
        });
        
        if (localImage) {
          console.log(`✅ Crawler image is up to date (SHA matches)`);
          return; // Already up to date
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
      return;
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
  } catch (error) {
    console.warn(`⚠️ Failed to check crawler image: ${error.message}`);
    // Don't fail if crawler image check fails
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
    checkCrawlerImage()
  ];
  await Promise.allSettled(tasks);
  console.log(`✅ Image check complete`);
}

/** Default container name for the AI service (helper starts it with short-lived ECR token) */
const AI_SERVICE_CONTAINER_NAME = 'aionqa-ai-service';
/** Ollama container (default LLM when USE_LM_STUDIO_HOST is not set) */
const OLLAMA_CONTAINER_NAME = 'ollama';

/** Helper config from API (fetched on startup; env overrides) */
let helperConfig = null;

/**
 * Fetch helper config from backend. Called on startup so params can be changed in UI and applied after restart.
 */
async function fetchHelperConfig() {
  try {
    const response = await axios.get(
      `${BACKEND_URL}/api/docker-engines/config`,
      { params: { engine_id: ENGINE_ID }, headers: getAuthHeaders(), timeout: 10000 }
    );
    if (response.data && response.data.config) {
      helperConfig = response.data.config;
      const provider = helperConfig.ai_service_provider || 'ollama';
      console.log(`✅ Helper config loaded from API (ai_service_provider: ${provider}, ollama_model: ${helperConfig.ollama_model})`);
    }
  } catch (error) {
    console.warn(`⚠️ Could not fetch helper config (using env/defaults): ${error.message}`);
  }
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

/** Whether to run the AI service container (config or env AI_SERVICE). When false, helper will stop and remove it. */
function isAIServiceEnabled() {
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
      Binds: ['ollama-models:/root/.ollama']
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
 * Stop and remove the AI service container. Used when user turns "AI service" off in UI.
 */
async function stopAndRemoveAIServiceContainer() {
  try {
    const containers = await docker.listContainers({ all: true });
    const existing = containers.find(c => c.Names && c.Names.some(n => n.replace(/^\//, '') === AI_SERVICE_CONTAINER_NAME));
    if (!existing) return;
    const container = docker.getContainer(existing.Id);
    if (existing.State === 'running') {
      await container.stop({ t: 10 }).catch(() => {});
    }
    await container.remove({ force: true }).catch(() => {});
    console.log(`🛑 AI service container stopped and removed`);
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
      await startAIServiceContainer();
    } else {
      await stopAndRemoveAIServiceContainer();
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
 */
async function startAIServiceContainer() {
  if (!isAIServiceEnabled()) return;

  await fetchHelperConfig();
  const imageUrl = await getAIServiceImageUrl();
  if (!imageUrl) return;

  const networkName = getConfigNetwork();
  try {
    const containers = await docker.listContainers({ all: true });
    const existing = containers.find(c => c.Names && c.Names.some(n => n.replace(/^\//, '') === AI_SERVICE_CONTAINER_NAME));
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
      name: AI_SERVICE_CONTAINER_NAME,
      Env: env,
      ExposedPorts: { '5000/tcp': {} },
      HostConfig: {
        PortBindings: { '5000/tcp': [{ HostPort: '5000' }] },
        NetworkMode: networkName,
        RestartPolicy: { Name: 'unless-stopped' }
      }
    });
    await container.start();
    console.log(`✅ AI service container started: ${AI_SERVICE_CONTAINER_NAME} on network ${networkName}`);
  } catch (error) {
    console.warn(`⚠️ Failed to start AI service container: ${error.message}`);
  }
}

/**
 * Run docker compose up -d --force-recreate when COMPOSE_PROJECT_DIR is set.
 * Returns true if recreate was run (and may have succeeded; process may exit when container is recreated).
 */
function runComposeRecreate() {
  const composeDir = process.env.COMPOSE_PROJECT_DIR || process.env.COMPOSE_PROJECT_DIRECTORY;
  if (!composeDir) return false;
  const composeFile = process.env.COMPOSE_FILE;
  try {
    const cmd = composeFile
      ? `docker compose -f "${composeFile}" up -d --force-recreate`
      : 'docker compose up -d --force-recreate';
    console.log(`🔄 [SELF-UPDATE] Running recreate: ${cmd} (cwd: ${composeDir})`);
    execSync(cmd, { cwd: composeDir, stdio: 'inherit', timeout: 120000 });
    console.log(`✅ [SELF-UPDATE] Recreate completed. Container should be running new image.`);
    return true;
  } catch (err) {
    console.warn(`⚠️ [SELF-UPDATE] Recreate failed: ${err.message}. Recreate the container manually (e.g. docker compose up -d --force-recreate).`);
    return false;
  }
}

/**
 * Update helper image (backend URL + image_sha, pull only if needed).
 * If download succeeds and COMPOSE_PROJECT_DIR is set, runs docker compose up -d --force-recreate immediately.
 */
async function runHelperSelfUpdate() {
  console.log(`🔄 [SELF-UPDATE] Updating helper image (pull only; recreate container to run new image).`);
  const result = await updateHelperImage();
  if (result.safeToRestart) {
    console.log(`✅ [SELF-UPDATE] New image downloaded.`);
    const recreated = runComposeRecreate();
    if (!recreated) {
      console.log(`ℹ️ [SELF-UPDATE] Set COMPOSE_PROJECT_DIR to recreate automatically. Otherwise run: docker compose up -d --force-recreate`);
    }
  } else {
    console.warn(`⚠️ [SELF-UPDATE] Could not update: ${result.reason}. Restart container manually if needed.`);
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
    const key = (a, name) => `${a}:${(name || '').replace(/^\//, '').toLowerCase()}`;
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
      const normalizedName = (name || '').replace(/^\//, '').toLowerCase();
      try {
        const containers = await docker.listContainers({ all: true });
        const found = containers.find(c =>
          (c.Names || []).some(n => n.replace(/^\//, '').toLowerCase() === normalizedName || n.replace(/^\//, '').toLowerCase().includes(normalizedName))
        );
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
          } else if (normalizedName.includes('aionqa-ai-service')) {
            await startAIServiceContainer();
            console.log(`▶️ Started AI service`);
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
          } else if (normalizedName.includes('aionqa-ai-service')) {
            await fetchHelperConfig();
            await stopAndRemoveAIServiceContainer();
            await pullAIServiceImage(true);
            await startAIServiceContainer();
            console.log(`🔄 Updated AI service`);
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
 * Main loop
 */
async function main() {
  // Register engine with retries
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
        // Don't exit - continue polling in case engine is pre-registered
        registered = false; // Mark as not registered but continue
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

  await fetchHelperConfig().catch(err => console.warn('⚠️ Initial config fetch failed (will retry in reconcile):', err.message));
  await ensureAionqaNetwork().catch(err => console.warn('⚠️ Ensure network failed:', err.message));
  // Start Ollama and AI service once from config; no periodic reconcile - Docker handles restarts, UI actions handle updates
  await reconcileServices();

  // Retry reconcile once after delay so Ollama/AI service start even if first run had transient failures (config not ready, image pull slow, etc.)
  const STARTUP_RECONCILE_DELAY_MS = 25 * 1000;
  setTimeout(() => {
    console.log(`🔄 [STARTUP] Retrying reconcile (Ollama / AI service) after ${STARTUP_RECONCILE_DELAY_MS / 1000}s...`);
    reconcileServices().catch(err => console.warn('⚠️ [STARTUP] Delayed reconcile failed:', err.message));
  }, STARTUP_RECONCILE_DELAY_MS);

  // Send heartbeat every 30 seconds
  setInterval(sendHeartbeat, 30000);
  // No periodic reconcile: Docker handles restarts. Start/stop/update only when UI sends pending actions.

  setInterval(cleanupContainers, 30 * 1000);
  await cleanupContainers();

  // No periodic image check: update images only when explicitly told (restart_requested, pending "update" actions) or when starting a container that needs one.

  // Main polling loop
  let pollCount = 0;
  while (true) {
    try {
      runPendingContainerActions().catch(() => {});
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
  console.error(`❌ Fatal error in main:`, error);
  console.error(`   Stack:`, error.stack);
  // Don't exit - try to continue
  console.log('   Attempting to restart main loop in 5 seconds...');
  setTimeout(() => {
    main().catch(err => {
      console.error(`❌ Failed to restart main loop:`, err);
    });
  }, 5000);
});

