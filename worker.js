import os from 'os';
import { stat } from 'fs/promises';
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

// Scaler & Worker Constraints
const HARD_CAP_CONCURRENCY = 10;
const INITIAL_CONCURRENCY = 4;
const CPU_SCALE_THRESHOLD = 60.0;
const CPU_SCALE_STEP = 2;

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

// Dynamic Concurrency & Slot Management
let current_target_concurrency = INITIAL_CONCURRENCY;
const active_job_promises = new Set();
const free_slots = Array.from({ length: HARD_CAP_CONCURRENCY }, (_, i) => i + 1);

// Maps job_id -> { slot: number, frame: number, fps: number, total_frames: number }
const active_progress_map = new Map();

// Coordinator Queues
const download_queue = []; // Items: { job_data, local_input_path }
const upload_queue = [];   // Items: { job_id, local_output_path, generation_time_sec }
const MAX_PREFETCH = 3;

// ============================================
// CPU SAMPLING & AUTOSCALER
// ============================================
const get_cpu_times = () => {
  const cpus = os.cpus();
  let idle = 0;
  let total = 0;
  for (const cpu of cpus) {
    for (const type in cpu.times) {
      total += cpu.times[type];
    }
    idle += cpu.times.idle;
  }
  return { idle, total };
};

const measure_cpu_percent = async (sample_duration_ms = 2000) => {
  const start = get_cpu_times();
  await new Promise((r) => setTimeout(r, sample_duration_ms));
  const end = get_cpu_times();

  const idle_delta = end.idle - start.idle;
  const total_delta = end.total - start.total;
  if (total_delta === 0) return 0;

  return Math.max(0, Math.min(100, (1 - idle_delta / total_delta) * 100));
};

// Ramp Controller: Runs every 60 seconds
setInterval(async () => {
  if (is_shutting_down) return;

  if (active_job_promises.size >= current_target_concurrency && current_target_concurrency < HARD_CAP_CONCURRENCY) {
    const cpu_pct = await measure_cpu_percent(2000);

    if (cpu_pct < CPU_SCALE_THRESHOLD) {
      const added = Math.min(CPU_SCALE_STEP, HARD_CAP_CONCURRENCY - current_target_concurrency);
      const old_target = current_target_concurrency;
      current_target_concurrency += added;

      console.log(
        `\n\x1b[36m[Autoscaler] CPU at ${cpu_pct.toFixed(1)}% (< ${CPU_SCALE_THRESHOLD}%). ` +
        `Ramping concurrency up: ${old_target} -> ${current_target_concurrency} (Cap: ${HARD_CAP_CONCURRENCY})\x1b[0m\n`
      );
    } else {
      console.log(
        `\n\x1b[33m[Autoscaler] CPU load at ${cpu_pct.toFixed(1)}% (>= ${CPU_SCALE_THRESHOLD}%). ` +
        `Holding concurrency at ${current_target_concurrency}.\x1b[0m\n`
      );
    }
  }
}, 60 * 1000);

// ============================================
// PERIODIC SUMMARY PRINTER (Every 10 Seconds)
// ============================================
setInterval(() => {
  if (active_progress_map.size === 0) return;

  const entries = Array.from(active_progress_map.entries())
    .sort((a, b) => a[1].slot - b[1].slot);

  const formatted_cells = entries.map(([_, stats]) => {
    const slot_str = String(stats.slot).padStart(2, '0');
    const fr_str = String(stats.frame).padStart(4, ' ');
    const fps_str = stats.fps > 0 ? stats.fps.toFixed(2).padStart(4, ' ') : '0.00';

    const target = stats.total_frames || 288;
    const pct = Math.min(100, Math.round((stats.frame / target) * 100));
    const pct_str = String(pct).padStart(3, ' ');

    return `[\x1b[90m ${slot_str} \x1b[0m] Fr: ${fr_str} | FPS: ${fps_str} | \x1b[36m${pct_str}%\x1b[0m`;
  });

  const COLS = 4;
  const rows = [];
  for (let i = 0; i < formatted_cells.length; i += COLS) {
    rows.push(formatted_cells.slice(i, i + COLS).join('   '));
  }

  const TABLE_WIDTH = 113;
  const timestamp = new Date().toISOString().split('T')[1].slice(0, 8);
  const title_left = `--- [${timestamp}] Active Workers (${entries.length}/${current_target_concurrency}) [Cap: ${HARD_CAP_CONCURRENCY}] [Q_in: ${download_queue.length} | Q_out: ${upload_queue.length}] `;
  const top_divider = title_left + '-'.repeat(Math.max(0, TABLE_WIDTH - title_left.length));
  const bottom_divider = '-'.repeat(TABLE_WIDTH);

  console.log(`\n${top_divider}`);
  console.log(rows.join('\n'));
  console.log(`${bottom_divider}\n`);
}, 10000);

// ============================================
// HELPERS & STATS
// ============================================
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const get_api_headers = () => ({
  'worker-auth': WORKER_API_SECRET,
  'x-machine-id': MACHINE_ID,
  'content-type': 'application/json',
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
        models: MODEL,
      }),
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
          generation_time_sec,
        }),
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
        error_message: typeof error_message === 'string' ? error_message : error_message?.message || 'Worker failure',
      }),
    });
  } catch (err) {
    console.error('[api] Fail report error:', err.message);
  }
};

// ============================================
// STORAGE & INTERPOLATION
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
  const file_stats = await stat(file_path);
  const file_stream = createReadStream(file_path);

  await s3_client.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET_NAME,
      Key: key,
      Body: file_stream,
      ContentLength: file_stats.size,
      ContentType: 'video/mp4',
    })
  );

  return `${R2_CDN_URL}/${key}`;
};

const run_ffmpeg_interpolation = (input_path, output_path, job_id, slot_num) => {
  return new Promise((resolve, reject) => {
    active_progress_map.set(job_id, {
      slot: slot_num,
      frame: 0,
      fps: 0,
      total_frames: 288,
    });

    const args = [
      '-y',
      '-hide_banner',
      '-loglevel', 'error',
      '-progress', 'pipe:1',
      '-nostats',
      '-i', input_path,
      '-filter:v', "setpts=4*PTS,minterpolate='fps=30:mi_mode=mci:mc_mode=aobmc:me_mode=bidir:vsbmc=1'",
      '-an',
      '-c:v', 'libx264',
      '-crf', '18',
      '-preset', 'slow',
      '-pix_fmt', 'yuv420p',
      output_path,
    ];

    const child = spawn(FFMPEG_BIN, args);
    let stderr_data = '';

    const watchdog = setTimeout(() => {
      child.kill('SIGKILL');
      active_progress_map.delete(job_id);
      reject(new Error(`FFmpeg timed out after ${MAX_JOB_SECONDS}s`));
    }, MAX_JOB_SECONDS * 1000);

    child.stdout.on('data', (chunk) => {
      const lines = chunk.toString().split('\n');
      const stats = active_progress_map.get(job_id);
      if (!stats) return;

      for (const line of lines) {
        const [key, value] = line.split('=').map((s) => s?.trim());
        if (!key || !value) continue;

        if (key === 'frame') stats.frame = parseInt(value, 10) || 0;
        if (key === 'fps') stats.fps = parseFloat(value) || 0;
      }
    });

    child.stderr.on('data', (chunk) => {
      stderr_data += chunk.toString();
    });

    child.on('close', (code) => {
      clearTimeout(watchdog);
      active_progress_map.delete(job_id);

      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`FFmpeg error (${code}): ${stderr_data.trim()}`));
      }
    });

    child.on('error', (err) => {
      clearTimeout(watchdog);
      active_progress_map.delete(job_id);
      reject(err);
    });
  });
};

// ============================================
// COORDINATOR QUEUES (IN & OUT)
// ============================================
// 1. In-Queue: Download & Prefetch Loop
(async function coordinator_download_loop() {
  while (!is_shutting_down) {
    if (download_queue.length >= MAX_PREFETCH) {
      await sleep(1000);
      continue;
    }

    const job_res = await poll_for_job();
    if (!job_res || !job_res.success || !job_res.data) {
      await sleep(POLL_INTERVAL_MS);
      continue;
    }

    const job_data = job_res.data;
    const job_id = job_data.job_id;
    const video_url = job_data.input?.video_url || job_data.video_url;

    if (!video_url) {
      console.error(`[prefetch] Job [${job_id}] missing video_url`);
      await fail_job(job_id, 'Missing video_url');
      continue;
    }

    const local_input_path = join(WORK_DIR, `${job_id}.webm`);

    try {
      await download_video(video_url, local_input_path);
      download_queue.push({ job_data, local_input_path });
    } catch (err) {
      console.error(`[prefetch] Failed download for [${job_id}]:`, err.message);
      await fail_job(job_id, err.message);
      try { await unlink(local_input_path); } catch (_) {}
    }
  }
})();

// 2. Out-Queue: Upload to R2 & Completion Reporter Loop
(async function coordinator_upload_loop() {
  while (!is_shutting_down || upload_queue.length > 0) {
    if (upload_queue.length === 0) {
      await sleep(500);
      continue;
    }

    const item = upload_queue.shift();
    const { job_id, slot_num, local_output_path, generation_time_sec } = item;

    try {
      const r2_url = await upload_to_r2(local_output_path, job_id);
      await complete_job(job_id, r2_url, generation_time_sec);

      total_jobs_processed++;
      total_generation_time_sec += generation_time_sec;
      consecutive_failures = 0;
      await sync_stats_file();

      console.log(
        `\x1b[32m✔ [worker] Job [${job_id}] Slot [${String(slot_num).padStart(2, '0')}] ` +
        `finished in ${generation_time_sec.toFixed(1)}s\x1b[0m -> ${r2_url}`
      );
    } catch (err) {
      console.error(`[uploader] Failed push for [${job_id}]:`, err.message);
      consecutive_failures++;
      await fail_job(job_id, err.message);

      if (consecutive_failures >= 50) {
        console.error('[worker] FATAL: 50 consecutive failures. Exiting.');
        process.exit(1);
      }
    } finally {
      try { await unlink(local_output_path); } catch (_) {}
    }
  }
})();

// ============================================
// COMPUTE EXECUTION HANDLER
// ============================================
const process_job_compute = async (job_data, input_path, slot_num) => {
  const job_id = job_data.job_id;
  const output_path = join(WORK_DIR, `${job_id}_slow4x.mp4`);
  const start_time = Date.now();

  console.log(`[worker] Started Job [${job_id}] in Slot [${String(slot_num).padStart(2, '0')}]`);

  try {
    await run_ffmpeg_interpolation(input_path, output_path, job_id, slot_num);

    const generation_time_sec = (Date.now() - start_time) / 1000;

    // Shift to asynchronous out-queue immediately to release compute slot
    upload_queue.push({
      job_id,
      slot_num,
      local_output_path: output_path,
      generation_time_sec,
    });
  } catch (err) {
    console.error(`[worker] Job [${job_id}] failed compute:`, err.message);
    consecutive_failures++;
    await fail_job(job_id, err.message);
    try { await unlink(output_path); } catch (_) {}
  } finally {
    try { await unlink(input_path); } catch (_) {}
  }
};

// ============================================
// MAIN DISPATCH LOOP
// ============================================
const main = async () => {
  console.log(
    `[worker] Initializing on host: ${MACHINE_ID} ` +
    `(Start: ${INITIAL_CONCURRENCY}, Cap: ${HARD_CAP_CONCURRENCY}, Threshold: ${CPU_SCALE_THRESHOLD}%)`
  );

  await mkdir(WORK_DIR, { recursive: true });
  await sync_stats_file();

  let empty_poll_count = 0;

  while (!is_shutting_down) {
    // Hold if active compute slots match target or no jobs downloaded yet
    if (active_job_promises.size >= current_target_concurrency || free_slots.length === 0 || download_queue.length === 0) {
      if (active_job_promises.size === 0 && download_queue.length === 0) {
        empty_poll_count++;
        if (empty_poll_count >= MAX_EMPTY_POLLS * 6) {
          console.log('[worker] Queue dry. Inactivity limit reached. Shutting down...');
          break;
        }
      } else {
        empty_poll_count = 0;
      }

      await Promise.race([
        ...active_job_promises,
        sleep(500),
      ]);
      continue;
    }

    empty_poll_count = 0;
    const { job_data, local_input_path } = download_queue.shift();
    const slot_num = free_slots.shift();

    const job_promise = (async () => {
      try {
        await process_job_compute(job_data, local_input_path, slot_num);
      } finally {
        free_slots.push(slot_num);
        free_slots.sort((a, b) => a - b);
        active_job_promises.delete(job_promise);
      }
    })();

    active_job_promises.add(job_promise);
    await sleep(200);
  }

  if (active_job_promises.size > 0) {
    console.log(`[worker] Waiting for ${active_job_promises.size} running encodes to complete...`);
    await Promise.all(active_job_promises);
  }

  // Drain upload queue
  while (upload_queue.length > 0) {
    console.log(`[worker] Draining upload queue (${upload_queue.length} files remaining)...`);
    await sleep(1000);
  }

  process.exit(0);
};

const handle_exit = async () => {
  console.log('[worker] Shutdown signal received. Draining workers and upload queue...');
  is_shutting_down = true;
  await Promise.all(active_job_promises);

  while (upload_queue.length > 0) {
    await sleep(1000);
  }
  process.exit(0);
};

process.on('SIGINT', handle_exit);
process.on('SIGTERM', handle_exit);

main().catch((err) => {
  console.error('[worker] Unhandled fatal exception:', err);
  process.exit(1);
});