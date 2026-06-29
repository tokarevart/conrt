#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

BINARY="target/debug/conrt"

echo "=== Building ==="
cargo build 2>&1

echo ""
echo "=== Host reference ==="
echo "UID/GID:    $(id)"
echo "Hostname:   $(hostname)"
echo "PID 1 cmd:  $(cat /proc/1/cmdline | tr '\0' ' ')"
echo "PID ns:     $(ls -la /proc/self/ns/pid | awk '{print $NF}')"
echo "User ns:    $(ls -la /proc/self/ns/user | awk '{print $NF}')"
echo "UTS ns:     $(ls -la /proc/self/ns/uts | awk '{print $NF}')"
echo "CapEff:     $(cat /proc/self/status | grep '^CapEff' | awk '{print $2}')"

echo ""
echo "=== Inside container (conrt run /bin/sh -c ...) ==="
echo "---"
$BINARY run -- /bin/sh -c '
echo "UID/GID:    $(id)"
echo "Hostname:   $(hostname)"
echo "PID self:   $$"
echo "PID 1 cmd:  $(cat /proc/1/cmdline 2>/dev/null | tr '\''\0'\'' '\'' '\'' || echo "(no /proc or empty)")"
echo "PID ns:     $(ls -la /proc/self/ns/pid 2>/dev/null | awk "{print \$NF}" || echo "N/A")"
echo "User ns:    $(ls -la /proc/self/ns/user 2>/dev/null | awk "{print \$NF}" || echo "N/A")"
echo "UTS ns:     $(ls -la /proc/self/ns/uts 2>/dev/null | awk "{print \$NF}" || echo "N/A")"
echo "CapEff:     $(cat /proc/self/status 2>/dev/null | grep '\''^CapEff'\'' | awk "{print \$2}" || echo "N/A")"
echo "Mounts:"
cat /proc/self/mounts 2>/dev/null | head -5 || echo "  (no /proc mounted)"
' 2>&1

echo ""
echo "=== Simple sanity: conrt run /bin/echo hello ==="
OUTPUT=$($BINARY run -- /bin/echo hello 2>&1)
echo "$OUTPUT"
echo ""
if echo "$OUTPUT" | grep -q hello; then
    echo "PASS: hello printed"
else
    echo "FAIL: hello not found in output"
fi

echo ""
echo "=== Done ==="
