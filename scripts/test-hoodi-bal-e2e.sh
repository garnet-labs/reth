#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: test-hoodi-bal-e2e.sh [options]

Runs the BAL generate/replay flow against a local Hoodi datadir using a temporary
local reth node, then unwinds the datadir back to its original canonical head.

Options:
  --datadir PATH       Hoodi datadir to test (default: /home/mediocregopher/src/ithaca/data/reth/hoodi)
  --reth-bin PATH      reth binary to use (default: <repo>/target/amp/debug/reth)
  --bench-bin PATH     reth-bench binary to use (default: <repo>/target/amp/debug/reth-bench)
  --from-block N       Block number to generate from (default: latest canonical block)
  --target-gas VALUE   Target gas passed to generate-big-block (default: 50M)
  --rpc-port PORT      Temporary HTTP RPC port (default: 18545)
  --authrpc-port PORT  Temporary auth RPC port (default: 18551)
  --p2p-port PORT      Temporary p2p port (default: 30313)
  --keep-artifacts     Keep the temporary payload directory and node log on success
  -h, --help           Show this help

The script refuses to run if another `reth node` process is already using the
same datadir.
EOF
}

log() {
    printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*" >&2
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)

DATADIR=/home/mediocregopher/src/ithaca/data/reth/hoodi
RETH_BIN="$REPO_ROOT/target/amp/debug/reth"
BENCH_BIN="$REPO_ROOT/target/amp/debug/reth-bench"
FROM_BLOCK=
TARGET_GAS=50M
RPC_PORT=18545
AUTHRPC_PORT=18551
P2P_PORT=30313
KEEP_ARTIFACTS=0
CHAIN=hoodi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --datadir)
            DATADIR="$2"
            shift 2
            ;;
        --reth-bin)
            RETH_BIN="$2"
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

for bin in "$RETH_BIN" "$BENCH_BIN"; do
    if [[ ! -x "$bin" ]]; then
        echo "Expected executable binary at $bin" >&2
        exit 1
    fi
done

if [[ ! -d "$DATADIR" ]]; then
    echo "Datadir does not exist: $DATADIR" >&2
    exit 1
fi

JWT_SECRET="$DATADIR/jwt.hex"
if [[ ! -f "$JWT_SECRET" ]]; then
    openssl rand -hex 32 > "$JWT_SECRET"
fi

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

while IFS= read -r lockfile; do
    clear_stale_lockfile_if_safe "$lockfile"
done < <(find "$DATADIR" -type f -name lock | sort)

existing_datadir_users=$(DATADIR="$DATADIR" python3 - <<'PY'
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
if [[ -n "$existing_datadir_users" ]]; then
    echo "Another reth node is already using $DATADIR:" >&2
    echo "$existing_datadir_users" >&2
    exit 1
fi

WORKDIR=$(mktemp -d /tmp/hoodi-bal-e2e.XXXXXX)
PAYLOAD_DIR="$WORKDIR/payloads"
NODE_LOG="$WORKDIR/node.log"
REPLAY_LOG="$WORKDIR/replay.log"
mkdir -p "$PAYLOAD_DIR"

NODE_PID=
ORIGINAL_HEAD_NUMBER=
ORIGINAL_HEAD_HASH=
FINAL_EXIT_CODE=0

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
    log "Starting temporary Hoodi node on RPC port $RPC_PORT"
    : > "$NODE_LOG"
    nohup "$RETH_BIN" node \
        --chain "$CHAIN" \
        --datadir "$DATADIR" \
        --http \
        --http.api eth,debug,net,web3,testing \
        --authrpc.jwtsecret "$JWT_SECRET" \
        --authrpc.port "$AUTHRPC_PORT" \
        --http.port "$RPC_PORT" \
        --port "$P2P_PORT" \
        --no-persist-peers \
        --disable-discovery \
        > "$NODE_LOG" 2>&1 &
    NODE_PID=$!
    wait_for_rpc "$(rpc_url)"
}

query_latest_block() {
    curl -fsS -H 'content-type: application/json' \
        --data '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}' \
        "$(rpc_url)"
}

cleanup() {
    local trap_exit=$?
    if (( trap_exit != 0 && FINAL_EXIT_CODE == 0 )); then
        FINAL_EXIT_CODE=$trap_exit
    fi

    set +e

    stop_node

    if [[ -n "${ORIGINAL_HEAD_NUMBER:-}" ]]; then
        log "Unwinding datadir back to canonical head $ORIGINAL_HEAD_NUMBER"
        "$RETH_BIN" stage unwind --datadir "$DATADIR" --chain "$CHAIN" to-block "$ORIGINAL_HEAD_NUMBER" >> "$NODE_LOG" 2>&1
        if [[ $? -ne 0 ]]; then
            echo "Failed to unwind $DATADIR back to block $ORIGINAL_HEAD_NUMBER" >&2
            FINAL_EXIT_CODE=1
        else
            start_node
            latest_json=$(query_latest_block)
            latest_number=$(LATEST_JSON="$latest_json" python3 - <<'PY'
import json, os
print(int(json.loads(os.environ['LATEST_JSON'])['result']['number'], 16))
PY
)
            latest_hash=$(LATEST_JSON="$latest_json" python3 - <<'PY'
import json, os
print(json.loads(os.environ['LATEST_JSON'])['result']['hash'])
PY
)
            if [[ "$latest_number" != "$ORIGINAL_HEAD_NUMBER" || "$latest_hash" != "$ORIGINAL_HEAD_HASH" ]]; then
                echo "Cleanup verification failed: expected head $ORIGINAL_HEAD_NUMBER/$ORIGINAL_HEAD_HASH, got $latest_number/$latest_hash" >&2
                FINAL_EXIT_CODE=1
            else
                log "Cleanup verified: restored head $latest_number ($latest_hash)"
            fi
            stop_node
        fi
    fi

    if (( KEEP_ARTIFACTS == 0 && FINAL_EXIT_CODE == 0 )); then
        rm -rf "$WORKDIR"
    else
        log "Artifacts kept at $WORKDIR"
    fi

    exit "$FINAL_EXIT_CODE"
}

trap cleanup EXIT

start_node

latest_json=$(query_latest_block)
ORIGINAL_HEAD_NUMBER=$(LATEST_JSON="$latest_json" python3 - <<'PY'
import json, os
print(int(json.loads(os.environ['LATEST_JSON'])['result']['number'], 16))
PY
)
ORIGINAL_HEAD_HASH=$(LATEST_JSON="$latest_json" python3 - <<'PY'
import json, os
print(json.loads(os.environ['LATEST_JSON'])['result']['hash'])
PY
)

if [[ -z "$FROM_BLOCK" ]]; then
    FROM_BLOCK="$ORIGINAL_HEAD_NUMBER"
fi

log "Original canonical head: $ORIGINAL_HEAD_NUMBER ($ORIGINAL_HEAD_HASH)"
log "Generating BAL payload from block $FROM_BLOCK with target gas $TARGET_GAS"

"$BENCH_BIN" generate-big-block \
    --rpc-url "$(rpc_url)" \
    --chain "$CHAIN" \
    --from-block "$FROM_BLOCK" \
    --target-gas "$TARGET_GAS" \
    --output-dir "$PAYLOAD_DIR" \
    --bal

PAYLOAD_PATH=$(find "$PAYLOAD_DIR" -maxdepth 1 -name 'big_block_*.json' | sort | head -n1)
if [[ -z "$PAYLOAD_PATH" ]]; then
    echo "No payload file was generated in $PAYLOAD_DIR" >&2
    exit 1
fi

PAYLOAD_PATH="$PAYLOAD_PATH" python3 - <<'PY'
import json, os, sys
path = os.environ['PAYLOAD_PATH']
with open(path, 'r', encoding='utf-8') as fh:
    data = json.load(fh)
bal = data.get('block_access_list')
if not isinstance(bal, list) or not bal:
    raise SystemExit(f'Payload {path} does not contain a non-empty block_access_list')
print(f"verified_payload={path} bal_accounts={len(bal)}")
PY

log "Replaying payload through reth_newPayload"
"$BENCH_BIN" replay-payloads \
    --payload-dir "$PAYLOAD_DIR" \
    --engine-rpc-url "$(authrpc_url)" \
    --jwt-secret "$JWT_SECRET" \
    --reth-new-payload \
    2>&1 | tee "$REPLAY_LOG"

log "BAL e2e flow completed successfully"
