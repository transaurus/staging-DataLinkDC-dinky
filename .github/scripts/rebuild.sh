#!/usr/bin/env bash
set -euo pipefail

# Rebuild script for DataLinkDC/dinky
# Runs on existing source tree (no clone). Current directory is docs/.
# Installs deps and builds the Docusaurus site.

# --- pnpm (version 10, as declared in packageManager field) ---
if ! command -v pnpm &>/dev/null || ! pnpm --version | grep -q "^10"; then
    echo "[INFO] Installing pnpm 10..."
    npm install -g pnpm@10
fi

# --- Dependencies ---
pnpm install --no-frozen-lockfile

# --- Build ---
pnpm run build

echo "[DONE] Build complete."
