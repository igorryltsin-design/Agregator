#!/usr/bin/env bash
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"

log() {
  echo "[start] $*"
}

build_ui() {
  local dir="$1"
  local label="$2"
  local dist="$here/$dir/dist/index.html"
  if [ -f "$dist" ]; then
    log "$label build exists."
    return
  fi
  if ! command -v npm >/dev/null 2>&1; then
    log "npm not found; cannot build $label (dist missing)"
    return
  fi
  pushd "$here/$dir" >/dev/null
  if [ ! -d node_modules ]; then
    log "Installing npm dependencies for $label..."
    npm install
  fi
  log "Building $label..."
  npm run build
  popd >/dev/null
}

build_ui "frontend" "Frontend"
build_ui "AiWord" "AiWord"

log "Initializing database and starting Flask..."
log "UI: http://localhost:5050/app "
export PYTHONUNBUFFERED=1

cmd=()
if [ -n "${PY_CMD:-}" ]; then
  read -r -a cmd <<<"$PY_CMD"
else
  if [ -n "${VIRTUAL_ENV:-}" ]; then
    cmd=(python)
  elif [ -x "$here/.venv/bin/python" ]; then
    # shellcheck disable=SC1090
    source "$here/.venv/bin/activate"
    cmd=(python)
  elif command -v poetry >/dev/null 2>&1; then
    cmd=(poetry run python)
  elif command -v pipenv >/dev/null 2>&1; then
    cmd=(pipenv run python)
  elif command -v conda >/dev/null 2>&1 && conda info --envs | grep -q "flask_catalog"; then
    cmd=(conda run -n flask_catalog python)
  else
    cmd=(python)
  fi
fi

cmd+=("$here/app.py")
"${cmd[@]}"
