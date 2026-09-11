import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { execFile } from 'child_process';
import { promisify } from 'util';
import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;

const required_env = [
  'DB_HOST',
  'DB_USER',
  'DB_PASSWORD',
  'DB_NAME',
  'RUNLTX_API_BASE_URL',
  'RUNLTX_API_KEY',
  'RUNLTX_JOB_TYPE',
  'RUNLTX_MODEL',
  'RUNLTX_MULTIPLIER',
  'VIDEO_BASE',
  'REMOTE_DATA_BASE',
  'FFPROBE_PATH'
];

for (const env_var of required_env) {
  if (!process.env[env_var] || process.env[env_var].trim() === '') {
    console.error(`FATAL: Missing required environment variable: ${env_var}`);
    process.exit(1);
  }
}

const execFileAsync = promisify(execFile);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5432', 10),
  database: process.env.DB_NAME || 'airtix',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || '',
  options: `-c search_path=${process.env.DB_SCHEMA || 'travel'},public`,
});

const RUNLTX_API_BASE_URL = process.env.RUNLTX_API_BASE_URL;
const RUNLTX_API_KEY = process.env.RUNLTX_API_KEY;
const RUNLTX_JOB_TYPE = process.env.RUNLTX_JOB_TYPE;
const RUNLTX_MODEL = process.env.RUNLTX_MODEL;
const RUNLTX_MULTIPLIER = parseInt(process.env.RUNLTX_MULTIPLIER, 10);

if (isNaN(RUNLTX_MULTIPLIER)) {
  console.error('FATAL: RUNLTX_MULTIPLIER must be a valid integer.');
  process.exit(1);
}

const VIDEO_BASE = process.env.VIDEO_BASE;
const REMOTE_DATA_BASE = process.env.REMOTE_DATA_BASE;
const FFPROBE_BIN = process.env.FFPROBE_PATH;

if (!fs.existsSync(FFPROBE_BIN)) {
  console.error(`FATAL: FFPROBE binary not found on disk at: ${FFPROBE_BIN}`);
  process.exit(1);
}

// Probes both dimensions and duration to confirm slow-motion / length expansion
async function get_video_metadata(file_path) {
  try {
    const { stdout, stderr } = await execFileAsync(
      FFPROBE_BIN,
      [
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height,duration:format=duration',
        '-of', 'json',
        file_path
      ]
    );

    if (stderr && stderr.trim().length > 0) {
      console.warn(`[FFPROBE STDERR] ${file_path}: ${stderr.trim()}`);
    }

    const info = JSON.parse(stdout);
    const stream = info.streams?.[0];
    const duration = parseFloat(stream?.duration || info.format?.duration || '0');

    if (stream && stream.width > 0 && stream.height > 0) {
      return {
        width: stream.width,
        height: stream.height,
        duration: duration
      };
    } else {
      console.error(`[FFPROBE ERROR] No valid stream found for ${file_path}. Raw stdout:`, stdout);
    }
  } catch (err) {
    console.error(`[FFPROBE EXEC ERROR] Failed to probe ${file_path}:`);
    console.error(`  Message: ${err.message}`);
    if (err.code) console.error(`  Exit Code: ${err.code}`);
    if (err.stderr) console.error(`  Stderr: ${err.stderr}`);
  }
  return null;
}

async function find_valid_interpolated_file_on_disk(interpolated_dir, seq_str) {
  if (!fs.existsSync(interpolated_dir)) return null;
  const files = await fs.promises.readdir(interpolated_dir);
  for (const file of files) {
    if (file.startsWith(seq_str + '.')) {
      const full_path = path.join(interpolated_dir, file);
      const meta = await get_video_metadata(full_path);
      // Ensure file exists, has valid dimensions, and expanded beyond original short clip length (> 5s)
      if (meta && meta.duration > 5) {
        const stat = await fs.promises.stat(full_path);
        return {
          filename: file,
          filepath: full_path,
          size: stat.size,
          width: meta.width,
          height: meta.height,
          duration: meta.duration
        };
      }
    }
  }
  return null;
}

async function find_source_upscaled_filename(upscaled_dir, seq_str) {
  if (!fs.existsSync(upscaled_dir)) return null;
  const files = await fs.promises.readdir(upscaled_dir);
  for (const file of files) {
    if (file.startsWith(seq_str + '.')) {
      return file;
    }
  }
  return null;
}

async function download_file(url, destination_path) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP download error! status: ${res.status}`);
  const array_buffer = await res.arrayBuffer();
  const buffer = Buffer.from(array_buffer);
  await fs.promises.writeFile(destination_path, buffer);
}

async function submit_runltx_interpolation_job(video_url) {
  const res = await fetch(`${RUNLTX_API_BASE_URL}/v1/process`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': RUNLTX_API_KEY
    },
    body: JSON.stringify({
      job_type: RUNLTX_JOB_TYPE,
      model: RUNLTX_MODEL,
      video_url: video_url,
      multiplier: RUNLTX_MULTIPLIER
    })
  });

  const json = await res.json();
  if (!res.ok || !json.success || !json.data?.job_id) {
    throw new Error(json.message || `Failed submission HTTP ${res.status}`);
  }
  return json.data.job_id;
}

async function check_runltx_job_status(job_id) {
  const res = await fetch(`${RUNLTX_API_BASE_URL}/v1/job/${job_id}`, {
    method: 'GET',
    headers: { 'x-api-key': RUNLTX_API_KEY }
  });
  const json = await res.json();
  if (!res.ok || !json.success || !json.data) {
    throw new Error(json.message || `Job query failed HTTP ${res.status}`);
  }
  return {
    status: json.data.status,
    output_url: json.data.output_url,
    error: json.data.error_message
  };
}

async function get_global_pipeline_state(client) {
  const video_counts = await client.query(`
    SELECT 
      COUNT(*)::int AS total_videos,
      COUNT(CASE WHEN interpolated_at IS NOT NULL THEN 1 END)::int AS videos_completed,
      COUNT(CASE WHEN upscaled_at IS NOT NULL AND interpolated_at IS NULL THEN 1 END)::int AS videos_pending_interpolation
    FROM t_videos
  `);

  const image_counts = await client.query(`
    SELECT 
      COUNT(*)::int AS total_images,
      COUNT(CASE WHEN interpolation_finished_at IS NOT NULL THEN 1 END)::int AS clips_interpolated,
      COUNT(CASE WHEN interpolation_job_id IS NOT NULL AND interpolation_finished_at IS NULL THEN 1 END)::int AS jobs_in_flight
    FROM t_hotel_images
  `);

  const v = video_counts.rows[0];
  const img = image_counts.rows[0];

  return {
    'Total Videos': v.total_videos,
    'Videos Interpolated': v.videos_completed,
    'Videos Pending Interpolation': v.videos_pending_interpolation,
    'Total Hotel Images': img.total_images,
    'Clips Finished Interpolation': img.clips_interpolated,
    'Active Queued Interpolation Jobs': img.jobs_in_flight
  };
}

async function run_interpolation_pipeline() {
  const start_time = Date.now();
  const client = await pool.connect();

  const stats = {
    videos_scanned: 0,
    videos_completed: 0,
    videos_pending: 0,
    videos_skipped_parse_error: 0,
    images_evaluated: 0,
    clips_found_on_disk: 0,
    clips_downloaded: 0,
    clips_queued_new: 0,
    clips_still_in_flight: 0,
    verification_warnings: 0,
    jobs_failed_resubmitted: 0,
    api_errors: 0
  };

  try {
    console.log('Connected to PostgreSQL Database.');
    console.log(`Using ffprobe executable at: "${FFPROBE_BIN}"`);

    const video_result = await client.query(`
      SELECT id, hotel_id, image_data
      FROM t_videos
      WHERE upscaled_at IS NOT NULL
        AND interpolated_at IS NULL
        AND image_data IS NOT NULL
      ORDER BY id ASC
    `);

    stats.videos_scanned = video_result.rows.length;

    if (stats.videos_scanned === 0) {
      console.log('\nNo pending videos found to interpolate clips for.');
      return;
    }

    console.log(`\nLoaded ${stats.videos_scanned} pending video(s) for interpolation pipeline.`);

    for (const video_row of video_result.rows) {
      const video_id = video_row.id;
      const hotel_id = video_row.hotel_id;
      const folder_name = String(hotel_id).padStart(8, '0');
      const hotel_dir = path.join(VIDEO_BASE, folder_name);
      const upscaled_dir = path.join(hotel_dir, '004.upscaled');
      const interpolated_dir = path.join(hotel_dir, '006.interpolated');

      console.log(`\n======================================================`);
      console.log(`Processing Video ID: ${video_id} | Hotel ID: ${hotel_id}`);
      console.log(`Target Interpolated Directory: ${interpolated_dir}`);
      console.log(`======================================================`);

      const raw_image_data = video_row.image_data;
      let image_list = [];

      try {
        if (typeof raw_image_data === 'string') {
          image_list = JSON.parse(raw_image_data);
        } else if (Array.isArray(raw_image_data)) {
          image_list = raw_image_data;
        } else if (typeof raw_image_data === 'object' && raw_image_data !== null) {
          image_list = raw_image_data;
        }
      } catch (err) {
        console.error(`Failed to parse image_data JSON for video ${video_id}:`, err.message);
        stats.videos_skipped_parse_error++;
        continue;
      }

      if (!Array.isArray(image_list) || image_list.length === 0) {
        console.warn(`Video ${video_id} has empty image list. Skipping.`);
        continue;
      }

      if (!fs.existsSync(interpolated_dir)) {
        await fs.promises.mkdir(interpolated_dir, { recursive: true });
      }

      let all_selected_completed = true;

      for (const img of image_list) {
        if (!img.classification?.selected) continue;

        stats.images_evaluated++;
        const seq_num = img.classification.sequence_num;
        const seq_str = String(seq_num).padStart(3, '0');

        if (!img.interpolation || typeof img.interpolation !== 'object') {
          img.interpolation = {
            job_id: null,
            completed: false,
            filename: null,
            filepath: null,
            width: null,
            height: null,
            duration: null
          };
        }

        // Branch 1: Disk-First Check
        const existing_on_disk = await find_valid_interpolated_file_on_disk(interpolated_dir, seq_str);
        if (existing_on_disk) {
          console.log(
            `[DISK TRUTH] Valid interpolated clip on disk: ${existing_on_disk.filename} ` +
            `(${existing_on_disk.width}x${existing_on_disk.height}, ${existing_on_disk.duration.toFixed(2)}s)`
          );
          stats.clips_found_on_disk++;

          img.interpolation.completed = true;
          img.interpolation.filename = existing_on_disk.filename;
          img.interpolation.filepath = existing_on_disk.filepath;
          img.interpolation.width = existing_on_disk.width;
          img.interpolation.height = existing_on_disk.height;
          img.interpolation.duration = existing_on_disk.duration;

          await client.query(`
            UPDATE t_hotel_images
            SET interpolation_finished_at = CURRENT_TIMESTAMP,
                interpolation_filename = $1
            WHERE id = $2
          `, [existing_on_disk.filepath, img.image_id]);
          continue;
        }

        // Reset if metadata marked complete, but clip missing/invalid on disk
        if (img.interpolation.completed === true) {
          console.warn(`[DISK TRUTH] Metadata marked complete, but clip missing/invalid on disk. Resetting: ${seq_str}`);
          img.interpolation.completed = false;
          img.interpolation.filename = null;
          img.interpolation.filepath = null;
          img.interpolation.width = null;
          img.interpolation.height = null;
          img.interpolation.duration = null;
        }

        // Branch 2: In-Flight Job Polling
        let job_id = img.interpolation.job_id;
        if (!job_id) {
          const db_job_res = await client.query(
            `SELECT interpolation_job_id FROM t_hotel_images WHERE id = $1`,
            [img.image_id]
          );
          job_id = db_job_res.rows[0]?.interpolation_job_id || null;
          if (job_id) img.interpolation.job_id = job_id;
        }

        let needs_submission = !job_id;

        if (job_id) {
          console.log(`[POLL] Checking in-flight interpolation job ${job_id} for sequence ${seq_str}...`);
          try {
            const check = await check_runltx_job_status(job_id);

            if (check.status === 'COMPLETED' && check.output_url) {
              console.log(`[POLL] Job ${job_id} COMPLETED. Downloading clip...`);
              const url_ext = path.extname(new URL(check.output_url).pathname) || '.mp4';
              const target_filename = `${seq_str}${url_ext}`;
              const target_filepath = path.join(interpolated_dir, target_filename);

              await download_file(check.output_url, target_filepath);

              const meta = await get_video_metadata(target_filepath);
              if (meta && meta.duration > 5) {
                const stat = await fs.promises.stat(target_filepath);
                const size_mb = (stat.size / (1024 * 1024)).toFixed(2);

                console.log(
                  `[POLL] ✅ Clip verified & saved: ${target_filename} ` +
                  `(${meta.width}x${meta.height}, ${meta.duration.toFixed(2)}s, ${size_mb} MB) -> ${target_filepath}`
                );

                stats.clips_downloaded++;
                img.interpolation.completed = true;
                img.interpolation.filename = target_filename;
                img.interpolation.filepath = target_filepath;
                img.interpolation.width = meta.width;
                img.interpolation.height = meta.height;
                img.interpolation.duration = meta.duration;

                await client.query(`
                  UPDATE t_hotel_images
                  SET interpolation_finished_at = CURRENT_TIMESTAMP,
                      interpolation_filename = $1
                  WHERE id = $2
                `, [target_filepath, img.image_id]);
                continue;
              } else {
                console.warn(`[PROBE WARNING] Clip downloaded but failed duration check: ${target_filename}. Leaving file intact. NOT resubmitting.`);
                stats.verification_warnings++;
                all_selected_completed = false;
                continue;
              }

            } else if (check.status === 'FAILED' || check.status === 'NOT-FOUND') {
              console.warn(`[POLL] Job ${job_id} '${check.status}'. Cleaning up and triggering immediate re-submission for ${seq_str}.`);
              stats.jobs_failed_resubmitted++;
              img.interpolation.job_id = null;
              needs_submission = true;

              await client.query(`
                UPDATE t_hotel_images
                SET interpolation_job_id = NULL,
                    interpolation_started_at = NULL,
                    interpolation_filename = NULL
                WHERE id = $1
              `, [img.image_id]);

            } else {
              console.log(`[POLL] Job ${job_id} status: ${check.status}. Continuing without waiting.`);
              stats.clips_still_in_flight++;
              all_selected_completed = false;
              continue;
            }
          } catch (status_err) {
            console.error(`[POLL] Failed to check interpolation job ${job_id}:`, status_err.message);
            stats.api_errors++;
            all_selected_completed = false;
            continue;
          }
        }

        // Branch 3: Initial or Retry Submission
        if (needs_submission) {
          all_selected_completed = false;

          let source_file = img.upscaling?.filename;
          if (!source_file) {
            source_file = await find_source_upscaled_filename(upscaled_dir, seq_str);
          }

          if (!source_file) {
            console.error(`[API] Cannot submit interpolation job for sequence ${seq_str}: Source upscaled video not found in ${upscaled_dir}`);
            continue;
          }

          const remote_video_url = `${REMOTE_DATA_BASE}/${folder_name}/004.upscaled/${source_file}`;

          try {
            const new_job_id = await submit_runltx_interpolation_job(remote_video_url);
            stats.clips_queued_new++;

            img.interpolation.job_id = new_job_id;
            img.interpolation.completed = false;
            img.interpolation.filename = null;
            img.interpolation.filepath = null;
            img.interpolation.width = null;
            img.interpolation.height = null;
            img.interpolation.duration = null;
            img.interpolation.source_url = remote_video_url;

            await client.query(`
              UPDATE t_hotel_images
              SET interpolation_job_id = $1,
                  interpolation_started_at = CURRENT_TIMESTAMP,
                  interpolation_finished_at = NULL,
                  interpolation_filename = NULL
              WHERE id = $2
            `, [new_job_id, img.image_id]);

            console.log(`[API] Successfully queued interpolation job: ${new_job_id} for sequence ${seq_str}`);
          } catch (submit_err) {
            console.error(`[API] Failed to submit interpolation job for ${seq_str}:`, submit_err.message);
            stats.api_errors++;
            img.interpolation.job_id = null;
          }
        }
      }

      // Finalize video row state
      const formatted_json = JSON.stringify(image_list, null, 2);

      if (all_selected_completed) {
        console.log(`\n[ALL COMPLETE] All selected interpolated clips verified on disk for Video ID: ${video_id}`);
        stats.videos_completed++;
        await client.query(`
          UPDATE t_videos
          SET image_data = $1,
              updated_at = CURRENT_TIMESTAMP,
              interpolated_at = CURRENT_TIMESTAMP
          WHERE id = $2
        `, [formatted_json, video_id]);
      } else {
        console.log(`\n[PENDING JOBS] Video ID ${video_id} has interpolation jobs in flight, queued, or awaiting verification.`);
        stats.videos_pending++;
        await client.query(`
          UPDATE t_videos
          SET image_data = $1,
              updated_at = CURRENT_TIMESTAMP,
              interpolated_at = NULL
          WHERE id = $2
        `, [formatted_json, video_id]);
      }

    //   console.log('Debug, exiting');
    //   break;
    }

    const elapsed_seconds = ((Date.now() - start_time) / 1000).toFixed(2);

    console.log('\n======================================================');
    console.log('              THIS RUN (CYCLE DELTA)                  ');
    console.log('======================================================');
    console.table({
      'Videos Processed': stats.videos_scanned > 0 ? (stats.videos_completed + stats.videos_pending) : 0,
      'Videos Fully Completed': stats.videos_completed,
      'Selected Images Evaluated': stats.images_evaluated,
      'Clips Valid on Disk': stats.clips_found_on_disk,
      'New Clips Downloaded': stats.clips_downloaded,
      'New Video Jobs Queued': stats.clips_queued_new,
      'Jobs Still In Flight': stats.clips_still_in_flight,
      'Probe Warnings (Preserved)': stats.verification_warnings,
      'Failed Jobs Cleaned & Re-Queued': stats.jobs_failed_resubmitted,
      'API / Network Errors': stats.api_errors,
      'Run Time (seconds)': elapsed_seconds
    });

    console.log('\n======================================================');
    console.log('          OVERALL PIPELINE STATE (GLOBAL DB)           ');
    console.log('======================================================');
    const global_stats = await get_global_pipeline_state(client);
    console.table(global_stats);
    console.log('======================================================\n');

  } catch (err) {
    console.error('Pipeline error:', err);
  } finally {
    client.release();
    await pool.end();
  }
}

run_interpolation_pipeline();