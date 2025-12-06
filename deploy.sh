#!/usr/bin/env bash
set -euo pipefail
# deploy.sh - simple idempotent deploy script for VPS
# Usage: ./deploy.sh [branch]
# Example: ./deploy.sh main

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
BRANCH="${1:-main}"
VENV_DIR="${REPO_DIR}/.venv"
PYTHON_BIN="${VENV_DIR}/bin/python"
PIP_BIN="${VENV_DIR}/bin/pip"

echo "== deploy.sh starting =="
echo "repo: $REPO_DIR"
echo "branch: $BRANCH"

cd "$REPO_DIR"

# ensure worktree clean (stop if there are local changes)
if [[ -n "$(git status --porcelain)" ]]; then
  echo "Warning: working tree not clean. Stashing local changes."
  git stash save --include-untracked "pre-deploy-$(date +%s)" || true
fi

# fetch and checkout branch
git fetch origin || true
git checkout "$BRANCH"
git pull origin "$BRANCH"

# ensure python venv exists
if [[ ! -d "$VENV_DIR" ]]; then
  echo "Creating virtualenv at $VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi

# upgrade pip and install pinned requirements
"$PIP_BIN" install -U pip setuptools wheel
if [[ -f requirements.txt ]]; then
  echo "Installing requirements.txt"
  "$PIP_BIN" install -r requirements.txt
else
  echo "No requirements.txt found, skipping pip install"
fi

# run DB migrations / init if present
if [[ -x scripts/init_db.py || -f scripts/init_db.py ]]; then
  echo "Running DB init (scripts/init_db.py)"
  # run with python module or script
  "$PYTHON_BIN" scripts/init_db.py || echo "init_db exit code $? (continuing)"
fi

# restart systemd service if exists
SERVICE_NAME="price-harvester.service"
if systemctl --version >/dev/null 2>&1; then
  if systemctl list-unit-files | grep -q "$SERVICE_NAME"; then
    echo "Restarting $SERVICE_NAME"
    sudo systemctl daemon-reload || true
    sudo systemctl restart "$SERVICE_NAME"
    sudo systemctl status --no-pager "$SERVICE_NAME" || true
  else
    echo "systemd service $SERVICE_NAME not found; skip restart"
  fi
else
  echo "systemd not available; skip service restart. Consider using screen/tmux/docker."
fi

echo "== deploy.sh finished =="
