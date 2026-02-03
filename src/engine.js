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
const HELPER_IMAGE_NAME = process.env.HELPER_IMAGE_NAME || 'aionqa-docker-engine-pool';
const AWS_ACCOUNT_ID = process.env.AWS_ACCOUNT_ID || '778954507986';
const AWS_REGION = process.env.AWS_REGION || 'ap-southeast-2';
const HELPER_ECR_IMAGE = `${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${HELPER_IMAGE_NAME}:latest`;

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

/**
 * Get full ECR image URL from backend
 * Converts local image names (like "aionqa-unified-crawler:latest") to full ECR URLs
 * Uses ENGINE_TOKEN for authentication (no SERVICE_KEY needed)
 */
async function getImageUrlFromBackend(imageName) {
  try {
    // If already full ECR URL, return as-is
    if (imageName.includes('.dkr.ecr.') || imageName.includes('amazonaws.com')) {
      return { image_url: imageName, requires_auth: true };
    }
    
    console.log(`📥 Requesting full ECR image URL from backend for: ${imageName}`);
    const response = await axios.post(
      `${BACKEND_URL}/api/docker-engines/image-url`,
      { image_name: imageName },
      {
        headers: getAuthHeaders(),
        timeout: 10000
      }
    );
    
    if (response.data && response.data.image_url) {
      console.log(`✅ Got full ECR image URL from backend: ${response.data.image_url} (auth: ${response.data.requires_auth ? 'required' : 'public'})`);
      return response.data;
    }
    
    return null;
  } catch (error) {
    console.warn(`⚠️ Failed to get image URL from backend:`, error.message);
    return null;
  }
}

/**
 * Get ECR pull token from backend
 * Returns token for authenticating Docker with ECR
 */
async function getECRPullToken() {
  try {
    console.log(`🔑 Requesting temporary ECR pull token from backend...`);
    const response = await axios.post(
      `${BACKEND_URL}/api/docker-engines/ecr-token`,
      {},
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
 * Authenticate Docker with ECR using temporary token
 */
async function authenticateECR(tokenData) {
  if (!tokenData || !tokenData.token || !tokenData.registry) {
    return false;
  }
  
  try {
    console.log(`🔐 Authenticating Docker with ECR registry: ${tokenData.registry}`);
    
    // Use dockerode to authenticate
    // Note: dockerode doesn't have direct login, so we'll use exec
    const { exec } = await import('child_process');
    const { promisify } = await import('util');
    const execAsync = promisify(exec);
    
    // Build docker login command
    const loginCmd = `echo "${tokenData.token}" | docker login --username AWS --password-stdin ${tokenData.registry}`;
    
    await execAsync(loginCmd);
    console.log(`✅ Docker authenticated with ECR`);
    return true;
  } catch (error) {
    console.warn(`⚠️ Failed to authenticate Docker with ECR:`, error.message);
    return false;
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
    job_type, // Get job_type from backend response
    image_url: jobImageUrl, // Image URL from job (may be overridden)
    environment_vars,
    runner_token,
    backend_url
  } = job;
  
  // ECR repositories are public - no authentication required
  // Helper uses ENGINE_TOKEN (project credentials) for backend authentication only
  // No AWS credentials needed for image pulls
  
  // Get full ECR image URL from backend if needed
  let image_url = jobImageUrl;
  let requiresAuth = true;
  
  // If image_url is a local name (not full ECR URL), resolve it via backend
  if (!image_url.includes('.dkr.ecr.') && !image_url.includes('amazonaws.com')) {
    console.log(`📥 Resolving image URL from backend: ${image_url}`);
    const imageUrlData = await getImageUrlFromBackend(image_url);
    if (imageUrlData && imageUrlData.image_url) {
      image_url = imageUrlData.image_url;
      requiresAuth = imageUrlData.requires_auth !== false;
      console.log(`✅ Resolved to: ${image_url} (auth: ${requiresAuth ? 'required' : 'public'})`);
    } else {
      // Backend failed - construct ECR URL manually as fallback
      console.warn(`⚠️ Failed to resolve image URL from backend, constructing ECR URL manually`);
      const AWS_ACCOUNT_ID = process.env.AWS_ACCOUNT_ID || '778954507986';
      const AWS_REGION = process.env.AWS_REGION || 'ap-southeast-2';
      
      // Map common image names to ECR repositories
      let repository = image_url;
      if (image_url.startsWith('aionqa-unified-crawler')) {
        repository = `aionqa-unified-crawler${image_url.replace('aionqa-unified-crawler', '')}`;
        requiresAuth = true; // PRIVATE - requires auth
      } else if (image_url.startsWith('aionqa-docker-engine-pool')) {
        repository = `aionqa-docker-engine-pool${image_url.replace('aionqa-docker-engine-pool', '')}`;
        requiresAuth = false; // PUBLIC - no auth
      } else {
        // Default: assume it's a crawler image
        repository = image_url;
        requiresAuth = true;
      }
      
      image_url = `${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${repository}`;
      console.log(`📦 Constructed ECR URL: ${image_url} (auth: ${requiresAuth ? 'required' : 'public'})`);
    }
  } else {
    // Already full ECR URL
    requiresAuth = !image_url.includes('aionqa-docker-engine-pool'); // Helper images are public
  }

  console.log(`🚀 Starting container for job ${job_id}`);
  console.log(`   Image: ${image_url}`);

  try {
    // ALWAYS pull the latest image from ECR before starting container
    // This ensures we're always using the most up-to-date code
    const isECRImage = image_url.includes('.dkr.ecr.') || image_url.includes('amazonaws.com');
    const isLocalImage = image_url.includes('localhost') || (!image_url.includes('.') && !image_url.includes('/'));
    
    if (isECRImage) {
      // Get ECR token if authentication is required
      if (requiresAuth) {
        const tokenData = await getECRPullToken();
        if (tokenData) {
          await authenticateECR(tokenData);
        } else {
          console.warn(`⚠️ Could not get ECR token, will try to pull anyway (may fail for private repos)`);
        }
      } else {
        console.log(`ℹ️  Image is public, no authentication needed`);
      }
      
      console.log(`📥 Pulling latest image from ECR: ${image_url}`);
      
      // Pull the latest image (Docker will use cache if up-to-date)
      try {
        await new Promise((resolve, reject) => {
          docker.pull(image_url, (err, stream) => {
            if (err) return reject(err);
            
            docker.modem.followProgress(stream, (err, output) => {
              if (err) return reject(err);
              console.log(`✅ Image pulled successfully`);
              resolve();
            });
          });
        });
      } catch (pullError) {
        console.error(`❌ Failed to pull image: ${pullError.message}`);
        // Check if image exists locally as fallback
        try {
          const images = await docker.listImages();
          const localImage = images.find(img => 
            img.RepoTags && img.RepoTags.some(tag => tag === image_url || tag.startsWith(image_url.split(':')[0] + ':'))
          );
          if (localImage) {
            console.warn(`⚠️ Using cached image (pull failed but image exists locally)`);
          } else {
            throw pullError; // Re-throw if no local image
          }
        } catch {
          throw pullError; // Re-throw if can't check or no local image
        }
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
          // Image doesn't exist locally - try to construct ECR URL and pull
          console.warn(`⚠️ Local image not found: ${image_url}`);
          console.log(`📥 Attempting to pull from ECR...`);
          
          const AWS_ACCOUNT_ID = process.env.AWS_ACCOUNT_ID || '778954507986';
          const AWS_REGION = process.env.AWS_REGION || 'ap-southeast-2';
          let ecrUrl = image_url;
          
          // Construct ECR URL if it's a local name
          if (!image_url.includes('.dkr.ecr.')) {
            let repository = image_url;
            if (image_url.startsWith('aionqa-unified-crawler')) {
              repository = `aionqa-unified-crawler${image_url.replace('aionqa-unified-crawler', '')}`;
              requiresAuth = true;
            } else if (image_url.startsWith('aionqa-docker-engine-pool')) {
              repository = `aionqa-docker-engine-pool${image_url.replace('aionqa-docker-engine-pool', '')}`;
              requiresAuth = false;
            } else {
              repository = image_url;
              requiresAuth = true;
            }
            ecrUrl = `${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${repository}`;
            console.log(`📦 Constructed ECR URL: ${ecrUrl}`);
          }
          
          // Get ECR token if needed
          if (requiresAuth) {
            const tokenData = await getECRPullToken();
            if (tokenData) {
              await authenticateECR(tokenData);
            }
          }
          
          // Try to pull from ECR
          try {
            await new Promise((resolve, reject) => {
              docker.pull(ecrUrl, (err, stream) => {
                if (err) return reject(err);
                docker.modem.followProgress(stream, (err, output) => {
                  if (err) return reject(err);
                  console.log(`✅ Image pulled successfully from ECR: ${ecrUrl}`);
                  image_url = ecrUrl; // Update to use ECR URL
                  resolve();
                });
              });
            });
          } catch (pullError) {
            console.error(`❌ Failed to pull image from ECR: ${pullError.message}`);
            throw new Error(`Image ${image_url} not found locally and failed to pull from ECR: ${pullError.message}`);
          }
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
    
    const container = await docker.createContainer({
      Image: image_url,
      name: containerName, // Use consistent naming pattern
      Env: containerEnv,
      HostConfig: {
        ShmSize: 2147483648, // 2GB for Chromium
        NetworkMode: 'aionqa_default', // Use docker-compose network for local development
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
          await container.remove();
          runningContainers.delete(containerInfo.Id);
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

  // Send heartbeat every 30 seconds
  setInterval(sendHeartbeat, 30000);

  // Cleanup containers every 5 minutes
  setInterval(cleanupContainers, 5 * 60 * 1000);

  // Main polling loop
  let pollCount = 0;
  while (true) {
    try {
      pollCount++;
      const job = await pollForJobs();
      
      if (job) {
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
          // Continue polling - don't crash the helper
        }
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

