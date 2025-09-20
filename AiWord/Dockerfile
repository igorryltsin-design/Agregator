# --- Stage 1: Build ---
FROM node:20-alpine AS build
WORKDIR /app
# Ускоряем установку: только prod lockfiles копируем сначала
COPY package.json package-lock.json* pnpm-lock.yaml* yarn.lock* ./
RUN \
  if [ -f package-lock.json ]; then npm ci; \
  elif [ -f pnpm-lock.yaml ]; then npm i -g pnpm && pnpm i --frozen-lockfile; \
  elif [ -f yarn.lock ]; then yarn install --frozen-lockfile; \
  else npm i; fi

# Копируем исходники и билдим
COPY . .
# Если у вас кастомный базовый путь, пробросьте VITE_ переменные через --build-arg/ENV
RUN npm run build  # получим папку dist/

# --- Stage 2: Serve via Nginx ---
FROM nginx:alpine
# Конфиг для SPA-роутинга (fallback на index.html)
COPY nginx.conf /etc/nginx/conf.d/default.conf
# Копируем статический билд
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]