#!/bin/bash
set -eo pipefail

echo "===================================================="
echo "[Startup] Initializing FFmpeg Interpolator Worker"
echo "===================================================="

# 1. Discovery
if [ -n "$VAST_CONTAINERLABEL" ] || [ -n "$CONTAINER_ID" ]; then
    export RUNNER_PLATFORM="vast"
elif [ -n "$MODAL_TASK_ID" ]; then
    export RUNNER_PLATFORM="modal"
elif [ -n "$HYPERSTACK_API_KEY" ]; then
    export RUNNER_PLATFORM="hyperstack"
else
    export RUNNER_PLATFORM="generic"
fi

MACHINE_ID=$(hostname)
API_BASE_URL="${API_BASE_URL:-https://api.runltx.com}"
CPU_MODEL=$(lscpu | grep "Model name:" | sed -r 's/Model name:\s+//g' || echo "Unknown CPU")
CPU_CORES=$(nproc)

echo "[Platform] Runtime : $RUNNER_PLATFORM"
echo "[Hardware] CPU     : $CPU_MODEL ($CPU_CORES cores)"
echo "===================================================="

# 2. Register worker startup session via /v1/worker/on
echo "[Billing] Registering worker startup session..."
SESSION_PAYLOAD=$(cat <<EOF
{
  "machine_id": "${MACHINE_ID}",
  "provider": "${RUNNER_PLATFORM}",
  "gpu_name": "${CPU_MODEL}",
  "gpu_count": ${CPU_CORES},
  "gpu_vram": "0"
}
EOF
)

SESSION_RESPONSE=$(curl -s -X POST "${API_BASE_URL}/v1/worker/on" \
    -H "Content-Type: application/json" \
    -H "worker-auth: ${WORKER_API_SECRET}" \
    -H "x-machine-id: ${MACHINE_ID}" \
    -d "${SESSION_PAYLOAD}" || echo '{"success":false}')

export WORKER_SESSION_ID=$(echo "$SESSION_RESPONSE" | node -e "
    const fs = require('fs');
    try {
        const res = JSON.parse(fs.readFileSync(0, 'utf-8'));
        if (res.success && res.session_id) process.stdout.write(res.session_id);
    } catch (_) {}
")

if [ -n "$WORKER_SESSION_ID" ]; then
    echo "[Billing] Active Session ID: ${WORKER_SESSION_ID}"
else
    echo "[Billing Warning] Could not initialize session tracking."
fi

# 3. Setup directories
export WORK_DIR="${WORK_DIR:-/tmp/interpolator}"
mkdir -p "$WORK_DIR"

# 4. Launch Worker Loop
node worker.js
WORKER_EXIT_CODE=$?

# 5. Register worker shutdown via /v1/worker/off
echo "[Billing] Finalizing session via /v1/worker/off..."
STATS_FILE="/tmp/worker_stats.json"
JOBS_PROCESSED=0
TOTAL_GEN_TIME=0

if [ -f "$STATS_FILE" ]; then
    JOBS_PROCESSED=$(node -e "const fs = require('fs'); try { console.log(JSON.parse(fs.readFileSync('$STATS_FILE')).jobs_processed || 0); } catch(_) { console.log(0); }")
    TOTAL_GEN_TIME=$(node -e "const fs = require('fs'); try { console.log(JSON.parse(fs.readFileSync('$STATS_FILE')).total_generation_time_sec || 0); } catch(_) { console.log(0); }")
fi

OFF_PAYLOAD=$(cat <<EOF
{
  "session_id": "${WORKER_SESSION_ID}",
  "machine_id": "${MACHINE_ID}",
  "jobs_processed": ${JOBS_PROCESSED},
  "total_generation_time_sec": ${TOTAL_GEN_TIME}
}
EOF
)

curl -s -X POST "${API_BASE_URL}/v1/worker/off" \
    -H "Content-Type: application/json" \
    -H "worker-auth: ${WORKER_API_SECRET}" \
    -H "x-machine-id: ${MACHINE_ID}" \
    -d "${OFF_PAYLOAD}" || true

echo "[Billing] Session closed."
exit $WORKER_EXIT_CODE
