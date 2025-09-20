#!/usr/bin/env bash
set -euo pipefail

APP_PORT="${PORT:-3000}"

# 1) Проверка Node
if ! command -v node >/dev/null 2>&1; then
  echo "[!] Node.js не найден. Установи LTS с https://nodejs.org/"
  exit 1
fi

# 2) Версия Node
NODE_MAJOR=$(node -p "process.versions.node.split('.')[0]")
if [ "$NODE_MAJOR" -lt 18 ]; then
  echo "[!] Найдена Node $(node -v). Нужна >= 18. Установи nvm и сделай: nvm install --lts"
  exit 1
fi

# 3) Установка зависимостей (если нужно)
if [ ! -d node_modules ]; then
  echo "[setup] Installing dependencies…"
  npm install
fi

# 4) Выбор команды запуска (Vite/CRA/Next)
if [ -f vite.config.ts ] || [ -f vite.config.js ]; then
  CMD="vite"
elif grep -q "\"react-scripts\"\\s*:" package.json 2>/dev/null; then
  CMD="react-scripts start"
elif grep -q "\"next\"\\s*:" package.json 2>/dev/null; then
  CMD="next dev -p ${APP_PORT}"
else
  # крайний случай: попробуем стандартный script
  if npm run | grep -q "start"; then
    CMD="npm start --"
  else
    echo "[!] Не удалось определить команду запуска. Добавь vite или react-scripts/next."
    exit 1
  fi
fi

# 5) Освобождаем порт (опционально для macOS)
if command -v lsof >/dev/null 2>&1; then
  PID=$(lsof -ti tcp:${APP_PORT} || true)
  if [ -n "$PID" ]; then
    echo "[i] Порт ${APP_PORT} занят (PID $PID). Завершаю процесс…"
    kill -9 $PID || true
  fi
fi

echo "[run] Starting on http://localhost:${APP_PORT}"
exec npx ${CMD}