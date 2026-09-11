import os from 'os';
import { spawn } from 'child_process';
import { createReadStream, createWriteStream } from 'fs';
import { mkdir, unlink, writeFile } from 'fs/promises';
import { join } from 'path';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

// ============================================
// CONFIGURATION & CONSTANTS
// ============================================
const MACHINE_ID = os.hostname();
const WORKER_SESSION_ID = process.env.WORKER_SESSION_ID;
const WORKER_API_SECRET = process.env.WORKER_API_SECRET;

if (!WORKER_API_SECRET) {
  console.error('[worker] FATAL: WORKER_API_SECRET environment variable is missing.');
  process.exit(1);
}

const API_BASE_URL = process.env.API_BASE_URL || 'https://api.runltx.com';
const JOB_TYPE = process.env.JOB_TYPE || 'interpolate';
const MODEL = process.env.MODEL || 'interpolate-video';

const CONCURRENCY = parseInt(process.env.CONCURRENCY, 10) || 8;
const POLL_INTERVAL_MS = (parseInt(process.env.POLL_INTERVAL_SECONDS, 10) || 5) * 1000;
const MAX_EMPTY_POLLS = parseInt(process.env.MAX_EMPTY_POLLS, 10) || 3;
const MAX_JOB_SECONDS = parseInt(process.env.MAX_JOB_SECONDS, 10) || 1800;
const FFMPEG_BIN = process.env.FFMPEG_BIN || 'ffmpeg';

const WORK_DIR = process.env.WORK_DIR || '/tmp/interpolator';
const STATS_FILE = '/tmp/worker_stats.json';

// Cloudflare R2 Credentials
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME;
const R2_CDN_URL = process.env.R2_CDN_URL;

const s3_client = new S3Client({
  region: 'auto',
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID || '',
    secretAccessKey: R2_SECRET_ACCESS_KEY || '',
  },
});

let total_jobs_processed = 0;
let total_generation_time_sec = 0;
let consecutive_failures = 0;
let is_shutting_down = false;

// Track active background job promises
const active_job_promises = new Set();

// ============================================
// HELPERS & STATS
// ============================================
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const get_api_headers = () => ({
  'worker-auth': WORKER_API_SECRET,
  'x-machine-id': MACHINE_ID,
  'content-type': 'application/json'
});

const sync_stats_file = async () => {
  try {
    const stats = {
      jobs_processed: total_jobs_processed,
      total_generation_time_sec: Math.round(total_generation_time_sec * 100) / 100,
    };
    await writeFile(STATS_FILE, JSON.stringify(stats));
  } catch (_) {}
};

// ============================================
// API CONTRACTS
// ============================================
const poll_for_job = async () => {
  try {
    const url = `${API_BASE_URL}/v1/worker/get`;
    const response = await fetch(url, {
      method: 'POST',
      headers: get_api_headers(),
      body: JSON.stringify({
        session_id: WORKER_SESSION_ID,
        job_type: JOB_TYPE,
        models: MODEL
      })
    });

    if (response.status === 404) return null;
    if (!response.ok) {
      const err_text = await response.text();
      throw new Error(`HTTP ${response.status}: ${err_text}`);
    }

    return await response.json();
  } catch (err) {
    console.error('[api] Poll error:', err.message);
    return null;
  }
};

const complete_job = async (job_id, output_url, generation_time_sec) => {
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const url = `${API_BASE_URL}/v1/worker/complete`;
      const response = await fetch(url, {
        method: 'POST',
        headers: get_api_headers(),
        body: JSON.stringify({
          session_id: WORKER_SESSION_ID,
          job_id,
          output_url,
          generation_time_sec
        })
      });

      if (!response.ok) {
        const err_text = await response.text();
        throw new Error(`HTTP ${response.status}: ${err_text}`);
      }

      return await response.json();
    } catch (err) {
      console.error(`[api] Complete attempt ${attempt} failed: ${err.message}`);
      if (attempt < 3) await sleep(2000);
    }
  }
};

const fail_job = async (job_id, error_message) => {
  try {
    const url = `${API_BASE_URL}/v1/worker/fail`;
    await fetch(url, {
      method: 'POST',
      headers: get_api_headers(),
      body: JSON.stringify({
        session_id: WORKER_SESSION_ID,
        job_id,
        error_message: typeof error_message === 'string' ? error_message : (error_message?.message || 'Worker failure')
      })
    });
  } catch (err) {
    console.error('[api] Fail report error:', err.message);
  }
};

// ============================================
// I/O & INTERPOLATION PIPELINE
// ============================================
const download_video = async (url, target_path) => {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Download failed: ${res.statusText}`);
  const file_stream = createWriteStream(target_path);
  const reader = res.body.getReader();

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    file_stream.write(Buffer.from(value));
  }
  await new Promise((resolve) => file_stream.end(resolve));
};

const upload_to_r2 = async (file_path, job_id) => {
  const key = `interpolations/${job_id}.mp4`;
  const file_stream = createReadStream(file_path);

  await s3_client.send(new PutObjectCommand({
    Bucket: R2_BUCKET_NAME,
    Key: key,
    Body: file_stream,
    ContentType: 'video/mp4',
  }));

  return `${R2_CDN_URL}/${key}`;
};

const run_ffmpeg_interpolation = (input_path, output_path) => {
  return new Promise((resolve, reject) => {
    const args = [
      '-y',
      '-hide_banner',
      '-loglevel', 'error',
      '-i', input_path,
      '-filter:v', "setpts=4*PTS,minterpolate='fps=30:mi_mode=mci:mc_mode=aobmc:me_mode=bidir:vsbmc=1'",
      '-an',
      '-c:v', 'libx264',
      '-crf', '18',
      '-preset', 'slow',
      '-pix_fmt', 'yuv420p',
      output_path
    ];

    const child = spawn(FFMPEG_BIN, args);
    let stderr_data = '';

    const watchdog = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error(`FFmpeg timed out after ${MAX_JOB_SECONDS}s`));
    }, MAX_JOB_SECONDS * 1000);

    child.stderr.on('data', (chunk) => {
      stderr_data += chunk.toString();
    });

    child.on('close', (code) => {
      clearTimeout(watchdog);
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`FFmpeg exited with code ${code}: ${stderr_data.trim()}`));
      }
    });

    child.on('error', (err) => {
      clearTimeout(watchdog);
      reject(err);
    });
  });
};

// ============================================
// JOB PROCESSOR
// ============================================
const process_job = async (job_data) => {
  const job_id = job_data.job_id;
  const video_url = job_data.input?.video_url || job_data.video_url;

  if (!video_url) {
    console.error(`[worker] Job [${job_id}] missing video_url`);
    await fail_job(job_id, 'Missing video_url');
    return;
  }

  const input_path = join(WORK_DIR, `${job_id}.webm`);
  const output_path = join(WORK_DIR, `${job_id}_slow4x.mp4`);
  const start_time = Date.now();

  console.log(`[worker] Started Job [${job_id}] (Active slots: ${active_job_promises.size})`);

  try {
    await download_video(video_url, input_path);
    await run_ffmpeg_interpolation(input_path, output_path);
    const r2_url = await upload_to_r2(output_path, job_id);

    const generation_time = (Date.now() - start_time) / 1000;
    await complete_job(job_id, r2_url, generation_time);

    total_jobs_processed++;
    total_generation_time_sec += generation_time;
    consecutive_failures = 0;
    await sync_stats_file();

    console.log(`\x1b[32m✔ [worker] Job [${job_id}] completed in ${generation_time.toFixed(1)}s\x1b[0m -> ${r2_url}`);
  } catch (err) {
    console.error(`[worker] Job [${job_id}] failed:`, err.message);
    consecutive_failures++;
    await fail_job(job_id, err.message);

    if (consecutive_failures >= 50) {
      console.error('[worker] FATAL: 50 consecutive failures. Exiting.');
      process.exit(1);
    }
  } finally {
    try { await unlink(input_path); } catch (_) {}
    try { await unlink(output_path); } catch (_) {}
  }
};

// ============================================
// MAIN EVENT LOOP
// ============================================
const main = async () => {
  console.log(`[worker] Initializing on host: ${MACHINE_ID} (Max Concurrency: ${CONCURRENCY})`);

  await mkdir(WORK_DIR, { recursive: true });
  await sync_stats_file();

  let empty_poll_count = 0;

  while (!is_shutting_down) {
    // If worker capacity is full, wait for any active job to finish
    if (active_job_promises.size >= CONCURRENCY) {
      await Promise.race(active_job_promises);
      continue;
    }

    const job_res = await poll_for_job();

    if (!job_res || !job_res.success || !job_res.data) {
      // Only count empty polls if there are NO jobs currently running
      if (active_job_promises.size === 0) {
        empty_poll_count++;
        console.log(`[worker] Queue dry (${empty_poll_count}/${MAX_EMPTY_POLLS})`);

        if (empty_poll_count >= MAX_EMPTY_POLLS) {
          console.log('[worker] Inactivity limit reached. Shutting down...');
          break;
        }
      }

      await sleep(POLL_INTERVAL_MS);
      continue;
    }

    // Reset empty poll count on job arrival
    empty_poll_count = 0;

    // Track active job execution in the concurrency pool
    const job_promise = (async () => {
      try {
        await process_job(job_res.data);
      } finally {
        active_job_promises.delete(job_promise);
      }
    })();

    active_job_promises.add(job_promise);

    // Yield loop briefly before claiming the next slot
    await sleep(200);
  }

  // Drain all running jobs before container termination
  if (active_job_promises.size > 0) {
    console.log(`[worker] Waiting for ${active_job_promises.size} running jobs to finish...`);
    await Promise.all(active_job_promises);
  }

  process.exit(0);
};

const handle_exit = async () => {
  console.log('[worker] Shutdown signal received. Draining workers...');
  is_shutting_down = true;
  await Promise.all(active_job_promises);
  process.exit(0);
};

process.on('SIGINT', handle_exit);
process.on('SIGTERM', handle_exit);

main().catch((err) => {
  console.error('[worker] Unhandled fatal exception:', err);
  process.exit(1);
});
