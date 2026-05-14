from __future__ import annotations
import asyncio
import json
import os
import time
from typing import Awaitable, Callable, Iterable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

_producer: AIOKafkaProducer | None = None


async def producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            enable_idempotence=True,
            acks="all",
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode() if k else None,
        )
        await _producer.start()
    return _producer


async def stop_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


async def health() -> bool:
    try:
        p = await producer()
        await p.client.fetch_all_metadata()
        return True
    except Exception:
        return False


class CircuitBreaker:
    def __init__(self, fail_threshold: int = 5, reset_after_s: float = 10.0):
        self.fail_threshold = fail_threshold
        self.reset_after_s = reset_after_s
        self.fails = 0
        self.opened_at: float | None = None

    def allow(self) -> bool:
        if self.fails < self.fail_threshold:
            return True
        if self.opened_at and (time.time() - self.opened_at) > self.reset_after_s:
            return True
        return False

    def record_success(self) -> None:
        self.fails = 0
        self.opened_at = None

    def record_failure(self) -> None:
        self.fails += 1
        if self.fails >= self.fail_threshold:
            self.opened_at = time.time()


_breaker = CircuitBreaker()


async def publish(topic: str, event: dict, key: str | None = None) -> None:
    if not _breaker.allow():
        raise RuntimeError(f"circuit-open: {topic}")
    max_retries = 3
    for attempt in range(max_retries):
        try:
            p = await producer()
            await p.send_and_wait(topic, value=event, key=key)
            _breaker.record_success()
            return
        except Exception:
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                _breaker.record_failure()
                raise


Handler = Callable[[dict], Awaitable[None]]


async def consume(topics: Iterable[str], group: str, handler: Handler) -> None:
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = msg.value
            payload["_stream"] = msg.topic
            try:
                await handler(payload)
                await consumer.commit()
            except Exception:
                pass
    finally:
        await consumer.stop()
