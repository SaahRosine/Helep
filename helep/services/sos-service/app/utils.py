import time
from functools import wraps
import structlog

log = structlog.get_logger("sos-service.perf")

def log_execution_time(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.time()
        result = await func(*args, **kwargs)
        duration_ms = (time.time() - start) * 1000
        log.info("execution_time", func=func.__name__, duration_ms=round(duration_ms, 2))
        return result
    return wrapper
