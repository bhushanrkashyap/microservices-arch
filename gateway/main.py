import logging
import time

import httpx
import jwt
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from shared.config.logging import JsonFormatter
from shared.config.jwt import JWT_SECRET, JWT_ALGORITHM

logger = logging.getLogger("gateway")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(JsonFormatter())
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)

app = FastAPI(title="API Gateway")

SERVICES = {
    "users": "http://user-service:8005",
    "orders": "http://order-service:8001",
    "inventory": "http://inventory-service:8002",
    "payments": "http://payment-service:8003",
    "notifications": "http://notification-service:8004",
}

PUBLIC_PREFIXES = (
    "/health",
    "/api/users/login",
    "/api/users/register",
    "/api/users/verify-token",
)


@app.middleware("http")
async def request_logging(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    logger.info(
        "http_request %s %s status=%s duration_ms=%s",
        request.method,
        request.url.path,
        response.status_code,
        round((time.time() - start) * 1000, 2),
    )
    return response


@app.middleware("http")
async def jwt_validation(request: Request, call_next):
    path = request.url.path
    if request.method == "OPTIONS" or path.startswith(PUBLIC_PREFIXES):
        return await call_next(request)

    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return JSONResponse({"error": "Missing bearer token"}, status_code=401)
    try:
        jwt.decode(auth.split(" ", 1)[1], JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except Exception as e:
        return JSONResponse({"error": f"Invalid token: {e}"}, status_code=401)
    return await call_next(request)


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.api_route("/api/{service}/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy(service: str, path: str, request: Request):
    base = SERVICES.get(service)
    if not base:
        return JSONResponse({"error": "Unknown service"}, status_code=404)

    url = f"{base}/{path}"
    body = await request.body()
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}

    async with httpx.AsyncClient(timeout=30.0) as client:
        upstream = await client.request(
            request.method,
            url,
            params=request.query_params,
            content=body,
            headers=headers,
        )
    return JSONResponse(
        status_code=upstream.status_code,
        content=_safe_json(upstream),
    )


def _safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}import logging
import time

import httpx
import jwt
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from shared.config.logging import JsonFormatter
from shared.config.jwt import JWT_SECRET, JWT_ALGORITHM

logger = logging.getLogger("gateway")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(JsonFormatter())
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)

app = FastAPI(title="API Gateway")

SERVICES = {
    "users": "http://user-service:8005",
    "orders": "http://order-service:8001",
    "inventory": "http://inventory-service:8002",
    "payments": "http://payment-service:8003",
    "notifications": "http://notification-service:8004",
}

PUBLIC_PREFIXES = (
    "/health",
    "/api/users/login",
    "/api/users/register",
    "/api/users/verify-token",
)


@app.middleware("http")
async def request_logging(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    logger.info(
        "http_request %s %s status=%s duration_ms=%s",
        request.method,
        request.url.path,
        response.status_code,
        round((time.time() - start) * 1000, 2),
    )
    return response


@app.middleware("http")
async def jwt_validation(request: Request, call_next):
    path = request.url.path
    if request.method == "OPTIONS" or path.startswith(PUBLIC_PREFIXES):
        return await call_next(request)

    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return JSONResponse({"error": "Missing bearer token"}, status_code=401)
    try:
        jwt.decode(auth.split(" ", 1)[1], JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except Exception as e:
        return JSONResponse({"error": f"Invalid token: {e}"}, status_code=401)
    return await call_next(request)


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.api_route("/api/{service}/{path:path}",
               methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy(service: str, path: str, request: Request):
    base = SERVICES.get(service)
    if not base:
        return JSONResponse({"error": "Unknown service"}, status_code=404)

    url = f"{base}/{path}"
    body = await request.body()
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}

    async with httpx.AsyncClient(timeout=30.0) as client:
        upstream = await client.request(
            request.method, url,
            params=request.query_params,
            content=body,
            headers=headers,
        )
    return JSONResponse(
        status_code=upstream.status_code,
        content=_safe_json(upstream),
    )


def _safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}
