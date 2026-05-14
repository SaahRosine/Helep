# L3 Patterns-in-Code Document — HELEP

---

## Part A — Pre-implemented patterns

### A.1 Choreographed Saga
- **Where:** Spans across `services/sos-service/app/main.py:78` (trigger), `services/dispatch-service/app/main.py:52` (match and assign), and `services/notification-service/app/main.py:18` (notify).
- **Compensation step:** The compensation step occurs in `services/dispatch-service/app/main.py:96` within `handle_cancel()`. The state that is rolled back is the responder assignment (`release_assignment(iid)` frees the responder and marks the assignment as `RELEASED`).
- **What event is the rollback trigger?** The rollback is triggered by the `sos.cancelled` event emitted by `sos-service`.

### A.2 Pub/Sub via Apache Kafka
- **Where:** `services/sos-service/app/events.py:77` (producer) and `services/sos-service/app/events.py:95` (consumer).
- **Consumer group semantics:** At-least-once delivery is guaranteed because `enable_auto_commit=False` is set. By manually awaiting `consumer.commit()` ONLY after `await handler(payload)` successfully completes, any failure in the handler leaves the message uncommitted, causing Kafka to re-deliver it upon the next poll.
- **Partition keying:** Publishing with `key=incident_id` ensures that all events related to the same incident are hashed to the same Kafka partition. Since a partition is consumed by only one consumer within a group, this preserves strict ordering of events for that incident, preventing race conditions like double-dispatching.

### A.3 Repository
- **Where:** `services/sos-service/app/db.py:1` (and all other `db.py` files).
- **Why:** If route handlers queried SQLite directly, the business logic would become tightly coupled to the SQL syntax and the specific database engine. A Repository abstracts the data access layer, meaning if we migrate from SQLite to Postgres later, we only need to update `db.py` without touching the route handlers.

### A.4 Strategy
- **Where:** `services/dispatch-service/app/matching.py:58`
- **How to switch:** Environment variable `MATCHER` (e.g. `MATCHER=nearest`, `MATCHER=credibility`, or `MATCHER=round-robin`).
- **Add a third strategy:** A `RoundRobinMatcher` was added at `services/dispatch-service/app/matching.py:55-65`.

### A.5 Outbox-lite
- **Where:** `services/sos-service/app/main.py:78` inside `trigger()`.
- **Why is this "lite"?** It is "lite" because the database insert (`insert_incident`) and the Kafka publish (`await publish`) happen sequentially in the application logic rather than within a single atomic database transaction. If the app crashes after the DB write but before the Kafka publish, the event is permanently lost. A real Outbox pattern would write the event to an "outbox" table in the exact same DB transaction as the incident, and a separate background relay process would read the table to safely publish to Kafka.

### A.6 Circuit Breaker
- **Where:** `services/sos-service/app/events.py:58` inside `class CircuitBreaker`.
- **Task:** The `allow()` state machine was completed at `services/sos-service/app/events.py:64-69`.
- **State transitions:** 
  - **CLOSED to OPEN:** Triggered when `self.fails >= self.fail_threshold` inside `record_failure()`.
  - **OPEN to HALF_OPEN:** Triggered inside `allow()` when `time.time() - self.opened_at > self.reset_after_s`. It allows one request through.
  - **HALF_OPEN to CLOSED:** Triggered by `record_success()` which resets `fails` to 0 and clears `opened_at`.

## Part B — Patterns you added (minimum 2)

### B.1 Decorator Pattern
- **Where:** Added `log_execution_time` in `services/sos-service/app/utils.py:7` and applied at `services/sos-service/app/main.py:78`.
- **Problem it solves in HELEP:** It solves the problem of cross-cutting concerns (like performance monitoring or logging execution time) cluttering the core business logic of route handlers.
- **Trade-off vs alternative pattern:** Compared to embedding timing logic directly into every handler (or using a Middleware pattern which applies globally to all routes), the Decorator is explicit but requires modifying the definition of every function we want to measure. It adds a slight functional overhead due to the wrapper function.

### B.2 Retry Pattern
- **Where:** Added inline retry loop within `services/sos-service/app/events.py:78` inside the `publish()` function.
- **Problem it solves in HELEP:** Resolves transient network blips or momentary Kafka broker unavailability by catching the exception and waiting with exponential backoff (`asyncio.sleep(2 ** attempt)`) before retrying, instead of immediately tripping the Circuit Breaker or failing the HTTP request.
- **Trade-off vs alternative pattern:** A trade-off is increased latency for the user when a failure occurs (due to sleeping), compared to the "Fail Fast" alternative where the error is immediately returned.

## Part C — Anti-patterns avoided

**Shared Database Anti-Pattern:** The architecture explicitly avoids having a single monolithic database shared by all services. 
- **Citation:** Demonstrated by `services/sos-service/app/db.py:18` connecting to its own `sqlite:///./data/sos.db` and `services/dispatch-service/app/db.py:20` connecting to `sqlite:///./data/dispatch.db`. This Database-Per-Service pattern ensures loose coupling and prevents a single point of failure at the data layer.
