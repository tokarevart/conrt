#!/usr/bin/env bash
set -euo pipefail

# Compare stack allocation between glibc clone (1 MB pre-allocated)
# and clone3 (no pre-allocation, uses COW like fork).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

BINARY="target/debug/examples/stack_demo"

# Build the demo
echo "=== Building stack_demo example ==="
cargo build --example stack_demo 2>&1

echo ""
echo "==========================================================="
echo " 1) glibc clone with a proper 1 MB stack"
echo "==========================================================="
echo ""
strace -e mmap,munmap,clone,clone3 -f "$BINARY" glibc 2>&1 || true

echo ""
echo "==========================================================="
echo " 2) clone3 with stack=0 (no stack allocation)"
echo "==========================================================="
echo ""
strace -e mmap,munmap,clone,clone3 -f "$BINARY" clone3 2>&1 || true

echo ""
echo "==========================================================="
echo " 3) glibc clone with NULL stack (should fail with EINVAL)"
echo "==========================================================="
echo ""
strace -e mmap,munmap,clone,clone3 -f "$BINARY" null 2>&1 || true

echo ""
echo "=== Done ==="
