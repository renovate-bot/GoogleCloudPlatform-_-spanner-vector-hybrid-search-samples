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

from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from app.api.configs import router as configs_router
from app.api.deployments import router as deployments_router
from app.api.export import router as export_router
from app.api.system import router as system_router
from app.api.benchmark import router as benchmark_router
from app.api.discovery import router as discovery_router
from app.api.ai import router as ai_router
from app.core.config import get_settings

STATIC_DIR = Path(__file__).resolve().parent / "static"

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        if request.url.path == "/" or request.url.path.endswith(".html"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

def create_app() -> FastAPI:
    settings = get_settings(reload=True)

    app = FastAPI(
        title="Google Cloud Spanner Deployment Explorer",
        description="Modernized Google Cloud Console styled visualizer for Spanner instance configurations and topology",
        version="1.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
    )

    # Security headers
    app.add_middleware(SecurityHeadersMiddleware)

    # CORS configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:3000", "http://localhost:3000", "http://127.0.0.1:8080", "http://localhost:8080"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include API Routers
    app.include_router(system_router, prefix="/api/v1")
    app.include_router(configs_router, prefix="/api/v1")
    app.include_router(deployments_router, prefix="/api/v1")
    app.include_router(export_router, prefix="/api/v1")
    app.include_router(discovery_router, prefix="/api/v1")

    # Conditionally mount AI assistant router based on feature flag
    if settings.features.enable_ai:
        app.include_router(ai_router, prefix="/api/v1")

    # Conditionally mount benchmark router based on feature flag
    if settings.features.enable_load_testing:
        app.include_router(benchmark_router, prefix="/api/v1")

    @app.get("/api/health", tags=["Health"])
    @app.get("/api/status", tags=["Health"])
    @app.get("/health", tags=["Health"])
    @app.get("/status", tags=["Health"])
    async def health_check():
        return {
            "status": "healthy",
            "service": "spanner-deployment-explorer",
            "version": "1.0.0",
        }

    @app.get("/sw.js", include_in_schema=False)
    @app.get("/service-worker.js", include_in_schema=False)
    async def unregister_service_worker():
        # Cleanly unregisters any rogue service worker registered by other projects on this host/port
        content = (
            "self.addEventListener('install', () => self.skipWaiting());\n"
            "self.addEventListener('activate', (event) => {\n"
            "  event.waitUntil(\n"
            "    self.registration.unregister()\n"
            "      .then(() => self.clients.matchAll())\n"
            "      .then((clients) => clients.forEach((c) => c.navigate(c.url)))\n"
            "  );\n"
            "});\n"
        )
        return Response(
            content=content,
            media_type="application/javascript",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"}
        )

    # Static file mounting & SPA fallback
    if (STATIC_DIR / "index.html").exists():
        app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

        @app.get("/{full_path:path}")
        async def serve_spa(full_path: str):
            # Do not intercept API routes
            if full_path.startswith("api/"):
                return JSONResponse(status_code=404, content={"detail": "Not found"})
            file_path = STATIC_DIR / full_path
            if file_path.exists() and file_path.is_file() and not file_path.name.endswith(".html"):
                return FileResponse(file_path)
            
            response = FileResponse(STATIC_DIR / "index.html")
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
            return response

    return app

app = create_app()
