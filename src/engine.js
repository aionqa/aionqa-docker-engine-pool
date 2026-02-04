/**
 * Docker Engine Pool Manager
 * Connects to backend and manages Docker containers for crawling jobs
 */

import Docker from 'dockerode';
import axios from 'axios';
import { hostname } from 'os';
import { randomUUID } from 'crypto';
// execAsync no longer needed - ECR authentication removed

// Default to 'back' service name for docker-compose networks
// In Docker Compose, services are resolved by service name, not container name
const BACKEND_URL = process.env.BACKEND_URL || 'http://back:4000';
const ENGINE_TOKEN = process.env.ENGINE_TOKEN; // Token for authenticating with backend
const SERVICE_KEY = process.env.SERVICE_KEY; // Optional service key for image URL retrieval
const ENGINE_ID = process.env.ENGINE_ID || `docker-engine-${hostname()}-${randomUUID().substring(0, 8)}`;
const POLL_INTERVAL = parseInt(process.env.POLL_INTERVAL || '5000'); // 5 seconds
const DOCKER_SOCKET = process.env.DOCKER_SOCKET || '/var/run/docker.sock';
const IMAGE_UPDATE_INTERVAL = 12 * 60 * 60 * 1000; // 12 hours in milliseconds

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
 * Send heartbeat to backend
 */
async function sendHeartbeat() {
  try {
    await axios.post(
      `${BACKEND_URL}/api/docker-engines/${ENGINE_ID}/heartbeat`,
      {
        status: 'active',
        running_containers: runningContainers.size
      },
      { headers: getAuthHeaders() }
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

    if (response.data && response.data.job) {
      console.log(`📥 [POLL] Received job from backend: ${response.data.job.job_id}`);
      console.log(`   Job type: ${response.data.job.job_type || 'unknown'}`);
      console.log(`   Image: ${response.data.job.image_url || 'unknown'}`);
      return response.data.job;
    } else {
      console.log(`⚠️ [POLL] Unexpected response format. Full response:`, JSON.stringify(response.data));
    }
  } catch (error) {
    if (error.response?.status === 404) {
      // No jobs available - log occasionally for debugging
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
        NetworkMode: 'aionqa-network', // Use the same network as helper (from docker-compose.helper.yml)
        AutoRemove: false, // Keep containers for debugging (can be set to true in production)
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
 * Pull AI service image from ECR using short-lived token
 * Same mechanism as crawler images
 * Only pulls if AI_SERVICE env var is set to 'true'
 */
async function pullAIServiceImage() {
  // Only pull if AI_SERVICE is enabled
  const aiServiceEnabled = process.env.AI_SERVICE === 'true' || process.env.AI_SERVICE === '1';
  if (!aiServiceEnabled) {
    console.log(`ℹ️  AI_SERVICE not enabled, skipping AI service image pull`);
    return;
  }
  // Priority: 1. AI_SERVICE_IMAGE_URL env var, 2. Extract from AI_SERVICE_URL if ECR, 3. Get from backend
  let imageUrl = process.env.AI_SERVICE_IMAGE_URL;
  
  if (!imageUrl) {
    const aiServiceUrl = process.env.AI_SERVICE_URL || 'http://aionqa-ai-service:5000';
    
    // Check if AI_SERVICE_URL is an ECR URL
    if (aiServiceUrl.includes('.dkr.ecr.') || aiServiceUrl.includes('amazonaws.com')) {
      // Extract image URL from AI_SERVICE_URL
      // Format: http://778954507986.dkr.ecr.ap-southeast-2.amazonaws.com/aionqa-ai-service:latest
      imageUrl = aiServiceUrl.replace(/^https?:\/\//, ''); // Remove http:// or https://
      if (!imageUrl.includes(':')) {
        imageUrl = `${imageUrl}:latest`; // Add :latest if no tag
      }
    } else {
      // Not an ECR URL, try to get image URL from backend
      try {
        console.log(`📥 Requesting AI service image URL from backend...`);
        const response = await axios.post(
          `${BACKEND_URL}/api/docker-engines/image-url`,
          { image_name: 'aionqa-ai-service:latest' },
          { headers: getAuthHeaders(), timeout: 10000 }
        );
        
        if (response.data && response.data.image_url) {
          imageUrl = response.data.image_url;
          console.log(`✅ Got AI service image URL from backend: ${imageUrl}`);
        } else {
          console.log(`ℹ️  AI Service URL is not ECR (${aiServiceUrl}), and backend didn't provide image URL - skipping image pull`);
          return;
        }
      } catch (error) {
        console.warn(`⚠️ Failed to get AI service image URL from backend: ${error.message}`);
        console.log(`ℹ️  Skipping AI service image pull (using local service or image already available)`);
        return;
      }
    }
  }
  
  try {
    console.log(`📥 Checking AI service image: ${imageUrl}`);
    
    // Check if image exists locally
    const images = await docker.listImages();
    const localImage = images.find(img => {
      if (!img.RepoTags) return false;
      return img.RepoTags.some(tag => tag === imageUrl || tag.startsWith(imageUrl.split(':')[0] + ':'));
    });
    
    if (localImage) {
      console.log(`✅ AI service image found locally: ${imageUrl}`);
      return;
    }
    
    // Image not found locally, pull from ECR
    console.log(`📥 AI service image not found locally, pulling from ECR: ${imageUrl}`);
    
    // Request ECR token from backend
    const tokenData = await getECRPullToken(imageUrl);
    if (!tokenData) {
      throw new Error(`Failed to get ECR token from backend`);
    }
    
    // Get auth config for Dockerode
    const authConfig = getECRAuthConfig(tokenData);
    
    // Pull image
    console.log(`📥 Pulling AI service image from ECR: ${imageUrl}`);
    await new Promise((resolve, reject) => {
      const pullOptions = {
        authconfig: authConfig
      };
      
      docker.pull(imageUrl, pullOptions, (err, stream) => {
        if (err) {
          console.error(`❌ Pull error:`, err.message);
          return reject(err);
        }
        
        docker.modem.followProgress(stream, (err, output) => {
          if (err) {
            console.error(`❌ Progress error:`, err.message);
            return reject(err);
          }
          console.log(`✅ AI service image pulled successfully`);
          resolve();
        });
      });
    });
  } catch (error) {
    console.warn(`⚠️ Failed to pull AI service image: ${error.message}`);
    // Don't fail startup if AI service image pull fails
    // The image might be built locally or available via other means
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

  // Pull AI service image from ECR if needed (using short-lived token)
  await pullAIServiceImage();

  // Send heartbeat every 30 seconds
  setInterval(sendHeartbeat, 30000);

  // Cleanup containers every 5 minutes
  setInterval(cleanupContainers, 5 * 60 * 1000);
  
  // Helper image updates are handled externally (docker-compose, k8s, etc.)
  // No need to check for updates here

  // Main polling loop
  let pollCount = 0;
  while (true) {
    try {
      pollCount++;
      const job = await pollForJobs();
      
      if (job) {
        // Check if this job is already being processed
        if (processingJobs.has(job.job_id)) {
          console.log(`⏭️  [MAIN LOOP] Job ${job.job_id} is already being processed, skipping...`);
          await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL));
          continue;
        }
        
        // Mark job as being processed
        processingJobs.add(job.job_id);
        
        console.log(`📋 [MAIN LOOP] Received job: ${job.job_id}`);
        console.log(`   Job type: ${job.job_type || 'unknown'}`);
        console.log(`   Image: ${job.image_url || 'unknown'}`);
        console.log(`   Full job object keys:`, Object.keys(job).join(', '));
        try {
          console.log(`🚀 [MAIN LOOP] Calling startCrawlerContainer for job ${job.job_id}...`);
          await startCrawlerContainer(job);
          console.log(`✅ [MAIN LOOP] Successfully started container for job ${job.job_id}`);
        } catch (containerError) {
          console.error(`❌ [MAIN LOOP] Failed to start container for job ${job.job_id}:`, containerError.message);
          console.error(`   Stack:`, containerError.stack);
          // Remove from processing set on error so it can be retried
          processingJobs.delete(job.job_id);
          // Continue polling - don't crash the helper
        }
        // Note: We don't remove from processingJobs on success because the job is now running
        // The cleanup will happen when the container stops (handled elsewhere)
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

