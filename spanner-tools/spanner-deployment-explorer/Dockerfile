# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ==========================================
# Stage 1: Build production frontend bundle
# ==========================================
FROM node:20-alpine AS frontend-builder

WORKDIR /app/frontend

COPY frontend/package*.json ./
RUN npm ci || npm install

COPY frontend/ ./
RUN npm run build

# ==========================================
# Stage 2: Production Python runtime
# ==========================================
FROM python:3.12-slim AS runtime

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application backend, config, and launcher files
COPY backend ./backend
COPY config.json main.py run.py ./

# Copy compiled frontend assets from builder stage
COPY --from=frontend-builder /app/backend/app/static ./backend/app/static

# App Engine Flexible environment defaults
ENV PORT=8080 \
    HOST=0.0.0.0 \
    RELOAD=false \
    ENABLE_LOAD_TESTING=false \
    ENABLE_AI=false

EXPOSE 8080

CMD ["python3", "main.py"]
