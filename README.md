# Render Farm Slot Scheduler

A small FastAPI + SQLite scheduling API for a render farm. Queued jobs get
backfilled into CPU slots that are temporarily idle, while the queue head's
earliest start time is preserved. Jobs may request a **preparation window**:
after dispatch the node first loads its assets while holding the slots, and
the maximum lease only starts once the node confirms it is rendering.

Interactive API docs: **http://localhost:8000/docs**

---

## Run with Docker Compose

```bash
docker compose up --build -d
# then open http://localhost:8000/docs
```

The SQLite file lives in the `scheduler-data` volume
(`SCHEDULER_DB_PATH=/data/scheduler.db`). Override the host port with
`SCHEDULER_PORT=9000 docker compose up`, or the reaper interval with the
`SCHEDULER_TICK_SECONDS` environment variable.

Stop and keep data: `docker compose down`.
Reset all data: `docker compose down -v`.

Run the tests in a container:

```bash
docker build -f Dockerfile.test -t render-scheduler-tests .
docker run --rm render-scheduler-tests
```

### Local run without Docker

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:create_app --factory --reload
pytest -q
```

`app.main:create_app` is an application factory, so it must be started with
`--factory` (the Docker image already does this).

---

## Model

* **Node** — a machine with `capacity` CPU slots.
* **Job** — submitted with a client-provided `job_id`, `required_slots`,
  `max_duration_seconds`, and an optional `prep_timeout_seconds`. A job
  leases *all* of its slots on **one** node; a request is never split across
  nodes.
* **Lease / reservation** — a held job is in one of two states:
  * `pending`: slots are reserved but the node is still loading assets. It
    has a `prep_deadline` (dispatch time + preparation timeout) but no lease
    expiry yet.
  * `running`: the node confirmed the start, `started_at`/`confirmed_at` are
    set and the lease runs until `lease_expires_at`
    (confirmation time + `max_duration_seconds`). It can be returned early,
    otherwise it expires automatically.
* **Queue** — jobs are ordered by submission (`seq`). Jobs that cannot start
  stay `queued`; they do not block jobs that fit except as described below.

### Preparation windows and confirmation

A node needs time to load a job's assets after it wins slots. A job submitted
with `prep_timeout_seconds` models that:

1. On dispatch the job enters `pending`: its slots are reserved immediately
   (they count against node capacity and block other jobs), but the maximum
   lease clock has not started.
2. The node calls `POST /jobs/{job_id}/confirm` once loading finishes and
   rendering begins — but only if it does so before the preparation deadline.
   Confirmation moves the job to `running` and sets the lease expiry to
   `now + max_duration_seconds`.
3. If no confirmation arrives by the deadline, the reservation is reclaimed:
   the job returns to `queued`, the slots are released, and the queue is
   re-evaluated. A reclaimed job skips re-dispatch for the remainder of that
   scheduling pass (the next tick treats it normally), so a missed deadline
   is observable rather than instantly replaced by a fresh window.
4. Confirmation rules:
   * Re-confirming an already `running` job is idempotent and returns the
     original lease expiry.
   * A job past its deadline, or one that is `queued`/`completed`/`expired`,
     cannot confirm — a timed-out or released job cannot re-acquire slots
     through confirmation (`409`).
   * Only a `running` lease can be returned via `/release`; a pending
     reservation is reclaimed automatically instead.
5. A job submitted **without** `prep_timeout_seconds` dispatches straight to
   `running` (confirmed at dispatch time), preserving the original behaviour.

While a job is pending, the scheduler predicts its slot release as
`dispatch_time + prep_timeout + max_duration_seconds` (the node confirming at
the last instant). Head estimates and backfill use that conservative
prediction; after confirmation they switch to the actual lease expiry.

The preparation deadline and all timestamps are persisted, so a restart keeps
using the saved windows.

### Scheduling policy

Every scheduling pass runs in a single `BEGIN IMMEDIATE` SQLite transaction:

1. Reclaim `pending` reservations whose `prep_deadline <= now` and expire
   `running` leases whose `lease_expires_at <= now`, releasing their slots.
2. If the queue head fits on some node right now, dispatch it (best fit:
   smallest node with enough free slots). Repeat.
3. If the head cannot start yet, compute its **earliest start time** from the
   predicted release of the jobs currently holding slots, then walk the rest
   of the queue in order. A later job is dispatched only if it can start now
   on one node and its full predicted lease ends by the head's earliest start
   — i.e. it provably does not delay the head. Jobs that fail this check keep
   their queue position.
4. If the head can never fit on any configured node, it has no finite start
   time to protect; it stays queued at position 1, and later jobs that fit
   may use idle slots (they run around the head, never put it further back).

Notes:

* Best-fit packing means a job takes the smallest node that can hold it, so
  large jobs keep large nodes available.
* A job larger than every node stays queued and its head estimate reports
  `insufficient_capacity`; increasing a node's capacity dispatches it.
* Node capacity can be changed at any time, but not below the slots currently
  held (pending or running) on that node (`409`).
* A job reclaimed in a pass is not redispatched within that same pass; the
  next tick re-evaluates it like any other queued job.

### Concurrency and failure handling

* In-process: every read-modify-write goes through a process-wide `RLock`.
* Across processes: `BEGIN IMMEDIATE` serialises scheduling passes
  (`busy_timeout=30s`), and the allocation `UPDATE ... WHERE status='queued'`
  and confirmation `UPDATE ... WHERE status='pending'` are guarded by their
  row counts.
* Database triggers reject any held (pending or running) row that would
  exceed node capacity, and any capacity shrink below current usage — a
  second safety net behind the scheduler's accounting.
* Each dispatch pass is a single transaction that includes the reclaim/expiry
  sweep. If anything raises, the transaction rolls back: queue order,
  statuses, and occupancy are exactly as before, and the next tick/request
  retries. A failed confirmation likewise rolls back and leaves the original
  pending state and deadline untouched.
* Releases are committed before the follow-up dispatch pass, so a dispatch
  failure cannot undo a successful release.
* Duplicate `job_id` submissions return `409`; the unique constraint is the
  final arbiter under concurrent submissions.

---

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/nodes` | Create or update a node's slot capacity |
| `GET` | `/nodes` | List nodes with used/free slots |
| `GET` | `/nodes/{node_id}` | One node's capacity and usage |
| `POST` | `/jobs` | Submit a job (dispatches immediately if possible) |
| `GET` | `/jobs?status=` | List jobs in submission order (`queued`/`pending`/`running`/`completed`/`expired`) |
| `GET` | `/jobs/{job_id}` | Status, node, prep deadline / lease expiry, remaining time |
| `POST` | `/jobs/{job_id}/confirm` | Confirm a pending job started; begins the maximum lease |
| `POST` | `/jobs/{job_id}/release` | Return a running lease early |
| `GET` | `/queue/head` | Queue head's estimated earliest start |
| `GET` | `/queue` | Queued jobs plus the head estimate |
| `POST` | `/admin/tick` | Reclaim/expire due reservations and re-dispatch now |
| `GET` | `/healthz` | Liveness + database check |

### Example walkthrough

```bash
# One node with six slots
curl -s -X POST localhost:8000/nodes \
  -H 'content-type: application/json' \
  -d '{"node_id":"farm-1","capacity":6}'

# A job that will hold 2 slots for two minutes
curl -s -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"job_id":"plate-001","required_slots":2,"max_duration_seconds":120}'

# Queue head needs 5 slots: it must wait for plate-001 to release its 2
curl -s -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"job_id":"plate-002","required_slots":5,"max_duration_seconds":30}'

# The head's ETA (about 120s away)
curl -s localhost:8000/queue/head

# A 2-slot / 5s backfill runs now: it is done long before the head can start
curl -s -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"job_id":"preview-001","required_slots":2,"max_duration_seconds":5}'

# A 4-slot / 30s job is *not* dispatched: it would still hold slots when the
# head could start, delaying it.
curl -s -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"job_id":"preview-002","required_slots":4,"max_duration_seconds":30}'

# Return the blocker early; plate-002 starts immediately
curl -s -X POST localhost:8000/jobs/plate-001/release
curl -s localhost:8000/jobs/plate-002   # status: running, node: farm-1
```

### Preparation-window walkthrough

```bash
# A job that gets 30 s to load its assets, then may render for up to 120 s.
curl -s -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"job_id":"plate-100","required_slots":2,"max_duration_seconds":120,
       "prep_timeout_seconds":30}'
# -> status "pending", prep_deadline = dispatch + 30 s, slots already held

# The node finishes loading and confirms before the deadline:
curl -s -X POST localhost:8000/jobs/plate-100/confirm
# -> status "running", lease_expires_at = now + 120 s

# Confirming again is safe and returns the same expiry.
# Missing the deadline instead reclaims the slots (a "released" tick event)
# and returns the job to the queue; confirming it then yields 409.
```

While `plate-100` is pending, head estimates and backfill assume it frees its
slots no later than `dispatch + 30 + 120` seconds; after confirmation they use
the real `lease_expires_at`.

`GET /queue/head` returns:

```json
{
  "head": {"job_id": "plate-002", "required_slots": 5, "status": "queued",
           "queue_position": 1, "...": "..."},
  "earliest_start_at": "2026-11-20T12:02:00.123456Z",
  "earliest_start_in_seconds": 119.8,
  "blocked_reason": "waiting_for_free_slots"
}
```

`blocked_reason` is one of `waiting_for_free_slots`,
`insufficient_capacity` (no node is large enough), or
`no_nodes_configured`; it is `null` when the head is not blocked.

---

## Implementation map

```
app/db.py          SQLite schema, transactions, invariant triggers, legacy migration
app/scheduler.py   FIFO + backfill, prep windows, confirmation, reclaim, leases
app/models.py      Pydantic request/response models (drives /docs)
app/main.py        FastAPI routes, error mapping, background reaper
tests/             69 tests: policy, prep/confirm/reclaim, rollback, HTTP, SQL
Dockerfile         production image (non-root, healthcheck)
Dockerfile.test    image that runs pytest
docker-compose.yml single service + named volume for the SQLite file
```

The background reaper wakes every `SCHEDULER_TICK_SECONDS` and runs the same
scheduling pass (reclaim + expiry + dispatch); every API read also runs a
pass first, so the reported state is current even with a long tick interval.
