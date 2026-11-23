"""
Uniform JSON error handling.

The spec requires "a meaningful JSON message" for errors. Every error the API
returns - ours, FastAPI's validation errors, unknown URLs, crashes - has this
shape, so clients only need ONE parsing path:

    {"error": "<machine_readable_code>", "message": "<human readable text>"}
"""
import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

log = logging.getLogger("controller.errors")


class ApiError(Exception):
    """Raise this anywhere in request handling to produce a clean JSON error."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def _json(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": code, "message": message})


def install_error_handlers(app: FastAPI) -> None:
    """Register handlers on the app. Called once from main.py."""

    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError):
        return _json(exc.status, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError):
        # FastAPI's default is 422; the spec says invalid requests are 400.
        details = "; ".join(
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()
        )
        return _json(400, "invalid_request", details)

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException):
        # Covers unknown URLs (404), wrong method (405), ...
        code = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "http_error")
        return _json(exc.status_code, code, str(exc.detail))

    @app.exception_handler(Exception)
    async def _unexpected(_: Request, exc: Exception):
        # Log the traceback for the operator, hide internals from the client.
        log.exception("unhandled error")
        return _json(500, "internal_error", "internal server error")
