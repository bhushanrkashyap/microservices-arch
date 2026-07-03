import json
import logging
import time


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
        }
        if record.exc_info:
            log["exc"] = self.formatException(record.exc_info)
        return json.dumps(log)


LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"json": {"()": "shared.config.logging.JsonFormatter"}},
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "json"},
    },
    "root": {"handlers": ["console"], "level": "INFO"},
}

logger = logging.getLogger("service")


class RequestLoggingMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        start = time.time()
        response = self.get_response(request)
        logger.info(
            "http_request %s",
            json.dumps(
                {
                    "method": request.method,
                    "path": request.path,
                    "status": response.status_code,
                    "duration_ms": round((time.time() - start) * 1000, 2),
                }
            ),
        )
        return response
