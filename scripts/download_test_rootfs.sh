#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <target-directory>"
    echo ""
    echo "Downloads and extracts the Alpine minirootfs to the given directory."
    echo "The directory will be created if it doesn't exist, and its contents"
    echo "will be chowned to the current user for rootless container testing."
    exit 1
fi

TARGET="$1"

# Already exists from a previous run
if [ -f "$TARGET/bin/busybox" ]; then
    exit 0
fi

ALPINE_VERSION="3.21.0"
ARCH="x86_64"
MIRROR="https://dl-cdn.alpinelinux.org/alpine/v3.21/releases/${ARCH}"
TARBALL="alpine-minirootfs-${ALPINE_VERSION}-${ARCH}.tar.gz"

TMPDIR="$(mktemp -d)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

echo "=== Downloading Alpine minirootfs ==="
echo "  Version: $ALPINE_VERSION"
echo "  Target:  $TARGET"
echo ""

TARBALL_PATH="$TMPDIR/$TARBALL"
curl -fsSL -o "$TARBALL_PATH" "$MIRROR/$TARBALL"
echo "  Downloaded: $(du -h "$TARBALL_PATH" | cut -f1)"

echo ""
echo "=== Extracting ==="
mkdir -p "$TARGET"
sudo tar xzf "$TARBALL_PATH" -C "$TARGET"
echo "  Extracted to $TARGET"

echo ""
echo "=== Setting ownership ==="
sudo chown -R "$(id -u):$(id -g)" "$TARGET"
echo "  Chowned to $(id -u):$(id -g)"

echo ""
echo "=== Verifying ==="
if [ -f "$TARGET/bin/busybox" ]; then
    echo "  OK: $TARGET/bin/busybox exists"
    echo "  OK: $(du -sh "$TARGET" | cut -f1) total"
else
    echo "  FAIL: $TARGET/bin/busybox not found — extraction may have failed"
    exit 1
fi

echo ""
echo "Done. Rootfs ready at $TARGET"
