#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: test-hoodi-bal-e2e.sh [options]

Runs the BAL generate/replay flow against a copied Hoodi datadir.

The script:
1. Refuses to run if the source datadir is already in use by another reth node.
2. Copies the source datadir to a temporary work datadir.
3. Starts a temporary `reth-bb` Hoodi node on the copied datadir.
4. Generates a BAL-backed big block payload with `generate-big-block`.
5. Stops the node and unwinds the copied datadir back to `from-block - 1`.
6. Restarts the node and replays the payload with `reth_newPayload`.
7. Verifies the replayed payload became the latest canonical head.
8. Deletes the copied datadir, leaving the source datadir untouched.

Options:
  --datadir PATH         Source Hoodi datadir (default: /home/mediocregopher/src/ithaca/data/reth/hoodi)
  --workdir PATH         Parent directory for temporary workdirs (default: sibling of --datadir)
  --node-bin PATH        node binary to use (default: <repo>/target/amp/debug/reth-bb)
  --bench-bin PATH       reth-bench binary to use (default: <repo>/target/amp/debug/reth-bench)
  --from-block N         Explicit start block. If omitted, the script picks the best recent multi-block window.
  --target-gas VALUE     Target gas passed to generate-big-block (default: 1G)
  --search-blocks N      Recent blocks to scan when auto-picking --from-block (default: 10064)
  --rpc-port PORT        Temporary HTTP RPC port (default: 18545)
  --authrpc-port PORT    Temporary auth RPC port (default: 18551)
  --p2p-port PORT        Temporary p2p port (default: 30313)
  --keep-artifacts       Keep the temporary copied datadir, payloads, and logs
  -h, --help             Show this help

Notes:
- The script uses `reth-bb` by default because true synthetic big blocks built
  from many inner blocks exceed normal `reth` gas-limit and blob-gas validation.
- The source datadir is never unwound or modified.
EOF
}

log() {
    printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*" >&2
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)

SOURCE_DATADIR=/home/mediocregopher/src/ithaca/data/reth/hoodi
WORK_PARENT=
NODE_BIN="$REPO_ROOT/target/amp/debug/reth-bb"
BENCH_BIN="$REPO_ROOT/target/amp/debug/reth-bench"
FROM_BLOCK=
TARGET_GAS=1G
SEARCH_BLOCKS=10064
RPC_PORT=18545
AUTHRPC_PORT=18551
P2P_PORT=30313
KEEP_ARTIFACTS=0
CHAIN=hoodi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --datadir)
            SOURCE_DATADIR="$2"
            shift 2
            ;;
        --workdir)
            WORK_PARENT="$2"
            shift 2
            ;;
        --node-bin|--reth-bin)
            NODE_BIN="$2"
            shift 2
            ;;
        --bench-bin)
            BENCH_BIN="$2"
            shift 2
            ;;
        --from-block)
            FROM_BLOCK="$2"
            shift 2
            ;;
        --target-gas)
            TARGET_GAS="$2"
            shift 2
            ;;
        --search-blocks)
            SEARCH_BLOCKS="$2"
            shift 2
            ;;
        --rpc-port)
            RPC_PORT="$2"
            shift 2
            ;;
        --authrpc-port)
            AUTHRPC_PORT="$2"
            shift 2
            ;;
        --p2p-port)
            P2P_PORT="$2"
            shift 2
            ;;
        --keep-artifacts)
            KEEP_ARTIFACTS=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 1
            ;;
    esac
done

for bin in "$NODE_BIN" "$BENCH_BIN"; do
    if [[ ! -x "$bin" ]]; then
        echo "Expected executable binary at $bin" >&2
        exit 1
    fi
done

if [[ ! -d "$SOURCE_DATADIR" ]]; then
    echo "Datadir does not exist: $SOURCE_DATADIR" >&2
    exit 1
fi

if [[ -z "$WORK_PARENT" ]]; then
    WORK_PARENT=$(dirname -- "$SOURCE_DATADIR")
fi
mkdir -p "$WORK_PARENT"

source_datadir_users=$(DATADIR="$SOURCE_DATADIR" python3 - <<'PY'
import os, subprocess

datadir = os.environ['DATADIR']
out = subprocess.run(
    ['ps', '-eo', 'pid=,args='],
    check=True,
    capture_output=True,
    text=True,
).stdout.splitlines()

for line in out:
    if ' reth node ' not in f' {line} ':
        continue
    if f' --datadir {datadir}' not in line:
        continue
    print(line)
PY
)
if [[ -n "$source_datadir_users" ]]; then
    echo "Another reth node is already using $SOURCE_DATADIR:" >&2
    echo "$source_datadir_users" >&2
    exit 1
fi

WORK_ROOT=$(mktemp -d "$WORK_PARENT/hoodi-bal-e2e.XXXXXX")
WORK_DATADIR="$WORK_ROOT/hoodi"
PAYLOAD_DIR="$WORK_ROOT/payloads"
NODE_LOG="$WORK_ROOT/node.log"
REPLAY_LOG="$WORK_ROOT/replay.log"
mkdir -p "$PAYLOAD_DIR"

NODE_PID=
FINAL_EXIT_CODE=0
REPLAY_EXPECTED_NUMBER=
REPLAY_EXPECTED_HASH=

rpc_url() {
    printf 'http://127.0.0.1:%s' "$RPC_PORT"
}

authrpc_url() {
    printf 'http://127.0.0.1:%s' "$AUTHRPC_PORT"
}

stop_node() {
    if [[ -z "${NODE_PID:-}" ]]; then
        return 0
    fi

    if kill -0 "$NODE_PID" 2>/dev/null; then
        kill -TERM "$NODE_PID" 2>/dev/null || true
        for _ in $(seq 1 30); do
            if ! kill -0 "$NODE_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
    fi

    if kill -0 "$NODE_PID" 2>/dev/null; then
        kill -KILL "$NODE_PID" 2>/dev/null || true
        for _ in $(seq 1 15); do
            if ! kill -0 "$NODE_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
    fi

    wait "$NODE_PID" 2>/dev/null || true
    NODE_PID=
}

clear_stale_lockfile_if_safe() {
    local lockfile=$1

    if [[ ! -f "$lockfile" ]]; then
        return 0
    fi

    local lock_pid lock_start proc_state proc_cmdline
    lock_pid=$(sed -n '1p' "$lockfile" 2>/dev/null || true)
    lock_start=$(sed -n '2p' "$lockfile" 2>/dev/null || true)

    if [[ -z "$lock_pid" || -z "$lock_start" ]]; then
        log "Removing unparsable lock file at $lockfile"
        rm -f "$lockfile"
        return 0
    fi

    if [[ ! -d "/proc/$lock_pid" ]]; then
        log "Removing stale lock file for missing pid $lock_pid at $lockfile"
        rm -f "$lockfile"
        return 0
    fi

    proc_state=$(awk '{print $3}' "/proc/$lock_pid/stat" 2>/dev/null || true)
    proc_cmdline=$(tr '\0' ' ' < "/proc/$lock_pid/cmdline" 2>/dev/null || true)

    if [[ "$proc_state" == "Z" || -z "$proc_cmdline" ]]; then
        log "Removing stale lock file held by zombie pid $lock_pid at $lockfile"
        rm -f "$lockfile"
    fi
}

prepare_work_datadir() {
    log "Copying source datadir to temporary work datadir: $WORK_DATADIR"
    rmdir "$WORK_DATADIR" 2>/dev/null || true
    cp -a --reflink=auto "$SOURCE_DATADIR" "$WORK_DATADIR"

    while IFS= read -r lockfile; do
        clear_stale_lockfile_if_safe "$lockfile"
    done < <(find "$WORK_DATADIR" -type f -name lock | sort)
}

wait_for_rpc() {
    local url=$1
    for _ in $(seq 1 60); do
        if [[ -n "${NODE_PID:-}" ]] && ! kill -0 "$NODE_PID" 2>/dev/null; then
            echo "reth node exited before RPC became ready" >&2
            tail -100 "$NODE_LOG" >&2 || true
            return 1
        fi
        if curl -fsS -H 'content-type: application/json' \
            --data '{"jsonrpc":"2.0","id":1,"method":"web3_clientVersion","params":[]}' \
            "$url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "Timed out waiting for RPC at $url" >&2
    tail -100 "$NODE_LOG" >&2 || true
    return 1
}

start_node() {
    local jwt_secret="$WORK_DATADIR/jwt.hex"
    if [[ ! -f "$jwt_secret" ]]; then
        openssl rand -hex 32 > "$jwt_secret"
    fi

    log "Starting temporary Hoodi node on copied datadir $WORK_DATADIR"
    : > "$NODE_LOG"
    nohup "$NODE_BIN" node \
        --chain "$CHAIN" \
        --datadir "$WORK_DATADIR" \
        --http \
        --http.api eth,debug,net,web3,testing \
        --authrpc.jwtsecret "$jwt_secret" \
        --authrpc.port "$AUTHRPC_PORT" \
        --http.port "$RPC_PORT" \
        --port "$P2P_PORT" \
        --no-persist-peers \
        --disable-discovery \
        > "$NODE_LOG" 2>&1 &
    NODE_PID=$!
    wait_for_rpc "$(rpc_url)"
}

query_block_by_tag() {
    local tag=$1
    curl -fsS -H 'content-type: application/json' \
        --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBlockByNumber\",\"params\":[\"$tag\",false]}" \
        "$(rpc_url)"
}

pick_from_block() {
    RPC_URL="$(rpc_url)" SEARCH_BLOCKS="$SEARCH_BLOCKS" python3 - <<'PY'
import json, os, sys, urllib.request

rpc = os.environ['RPC_URL']
search_blocks = int(os.environ['SEARCH_BLOCKS'])
max_blob = 2_752_512

def call(method, params):
    req = urllib.request.Request(
        rpc,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.load(resp)
    if 'error' in body:
        raise RuntimeError(body['error'])
    return body['result']

latest = int(call('eth_blockNumber', []), 16)
earliest = max(0, latest - search_blocks)

best = None
cur_start = None
cur_end = None
cur_gas = 0
cur_blob = 0
cur_len = 0

for number in range(earliest, latest + 1):
    block = call('eth_getBlockByNumber', [hex(number), False])
    gas = int(block['gasUsed'], 16)
    blob_hex = block.get('blobGasUsed')
    blob = int(blob_hex, 16) if blob_hex is not None else 0

    if cur_len and cur_blob + blob > max_blob:
        candidate = (cur_gas, cur_blob, cur_start, cur_end, cur_len)
        if candidate[4] > 1 and (best is None or candidate[0] > best[0]):
            best = candidate
        cur_start = number
        cur_end = number
        cur_gas = gas
        cur_blob = blob
        cur_len = 1
    else:
        if cur_len == 0:
            cur_start = number
        cur_end = number
        cur_gas += gas
        cur_blob += blob
        cur_len += 1

candidate = (cur_gas, cur_blob, cur_start, cur_end, cur_len)
if candidate[4] > 1 and (best is None or candidate[0] > best[0]):
    best = candidate

if best is None:
    raise SystemExit('No multi-block mergeable window found in the scanned range')

print(json.dumps({
    'latest': latest,
    'earliest': earliest,
    'start': best[2],
    'end': best[3],
    'gas_used': best[0],
    'blob_gas_used': best[1],
    'block_count': best[4],
}))
PY
}

cleanup() {
    local trap_exit=$?
    if (( trap_exit != 0 && FINAL_EXIT_CODE == 0 )); then
        FINAL_EXIT_CODE=$trap_exit
    fi

    set +e
    stop_node

    if (( KEEP_ARTIFACTS == 0 && FINAL_EXIT_CODE == 0 )); then
        rm -rf "$WORK_ROOT"
    else
        log "Artifacts kept at $WORK_ROOT"
    fi

    exit "$FINAL_EXIT_CODE"
}

trap cleanup EXIT

prepare_work_datadir
start_node

if [[ -z "$FROM_BLOCK" ]]; then
    selection=$(pick_from_block)
    FROM_BLOCK=$(SELECTION="$selection" python3 - <<'PY'
import json, os
data = json.loads(os.environ['SELECTION'])
print(data['start'])
PY
)
    log "Auto-selected from-block $FROM_BLOCK from recent mergeable window: $selection"
else
    log "Using explicit from-block $FROM_BLOCK"
fi

log "Generating BAL payload from block $FROM_BLOCK with target gas $TARGET_GAS"
"$BENCH_BIN" generate-big-block \
    --rpc-url "$(rpc_url)" \
    --chain "$CHAIN" \
    --from-block "$FROM_BLOCK" \
    --target-gas "$TARGET_GAS" \
    --output-dir "$PAYLOAD_DIR" \
    --bal \
    --ignore-blob-gas-limit

PAYLOAD_PATH=$(find "$PAYLOAD_DIR" -maxdepth 1 -name 'big_block_*.json' | sort | head -n1)
if [[ -z "$PAYLOAD_PATH" ]]; then
    echo "No payload file was generated in $PAYLOAD_DIR" >&2
    exit 1
fi

payload_summary=$(PAYLOAD_PATH="$PAYLOAD_PATH" python3 - <<'PY'
import json, os
path = os.environ['PAYLOAD_PATH']
with open(path, 'r', encoding='utf-8') as fh:
    data = json.load(fh)
bal = data.get('block_access_list') or []
switches = data['big_block_data']['env_switches']
payload = data['execution_data']['payload']
summary = {
    'path': path,
    'bal_accounts': len(bal),
    'env_switches': len(switches),
    'gas_used': int(payload['gasUsed'], 16),
    'block_number': int(payload['blockNumber'], 16),
    'block_hash': payload['blockHash'],
}
if summary['bal_accounts'] == 0:
    raise SystemExit(f'Payload {path} does not contain a non-empty block_access_list')
if summary['env_switches'] == 0:
    raise SystemExit(f'Payload {path} was not constructed from multiple inner blocks')
print(json.dumps(summary))
PY
)

REPLAY_EXPECTED_NUMBER=$(PAYLOAD_SUMMARY="$payload_summary" python3 - <<'PY'
import json, os
print(json.loads(os.environ['PAYLOAD_SUMMARY'])['block_number'])
PY
)
REPLAY_EXPECTED_HASH=$(PAYLOAD_SUMMARY="$payload_summary" python3 - <<'PY'
import json, os
print(json.loads(os.environ['PAYLOAD_SUMMARY'])['block_hash'])
PY
)
log "Verified generated payload: $payload_summary"

stop_node

if (( FROM_BLOCK == 0 )); then
    echo "from-block 0 cannot be replayed after unwind because there is no parent block" >&2
    exit 1
fi

UNWIND_TARGET=$((FROM_BLOCK - 1))
log "Unwinding copied datadir back to parent block $UNWIND_TARGET before replay"
"$NODE_BIN" stage unwind --datadir "$WORK_DATADIR" --chain "$CHAIN" to-block "$UNWIND_TARGET" >> "$NODE_LOG" 2>&1

start_node

latest_before_replay=$(query_block_by_tag latest)
actual_parent=$(LATEST_JSON="$latest_before_replay" python3 - <<'PY'
import json, os
print(int(json.loads(os.environ['LATEST_JSON'])['result']['number'], 16))
PY
)
if [[ "$actual_parent" != "$UNWIND_TARGET" ]]; then
    echo "Expected copied datadir to be unwound to $UNWIND_TARGET before replay, got $actual_parent" >&2
    exit 1
fi

log "Replaying payload through reth_newPayload"
"$BENCH_BIN" replay-payloads \
    --payload-dir "$PAYLOAD_DIR" \
    --engine-rpc-url "$(authrpc_url)" \
    --jwt-secret "$WORK_DATADIR/jwt.hex" \
    --reth-new-payload \
    2>&1 | tee "$REPLAY_LOG"

latest_after_replay=$(query_block_by_tag latest)
LATEST_JSON="$latest_after_replay" EXPECTED_NUMBER="$REPLAY_EXPECTED_NUMBER" EXPECTED_HASH="$REPLAY_EXPECTED_HASH" python3 - <<'PY'
import json, os, sys
latest = json.loads(os.environ['LATEST_JSON'])['result']
number = int(latest['number'], 16)
hash_ = latest['hash']
expected_number = int(os.environ['EXPECTED_NUMBER'])
expected_hash = os.environ['EXPECTED_HASH']
if number != expected_number or hash_ != expected_hash:
    raise SystemExit(
        f'replay verification failed: expected latest {expected_number}/{expected_hash}, got {number}/{hash_}'
    )
print(f'replay_verified latest={number} hash={hash_}')
PY

log "BAL e2e flow completed successfully"
