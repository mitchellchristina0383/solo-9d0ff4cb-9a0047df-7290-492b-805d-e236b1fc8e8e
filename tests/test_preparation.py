"""Preparation windows: pending reservations, confirmation and reclaim.

Covers the lifecycle added on top of the plain FIFO/backfill scheduler:

* dispatch reserves slots as ``pending`` with a preparation deadline;
* confirmation starts the maximum lease clock and swaps the predicted lease
  end for the real one;
* a missed deadline reclaims the slots and requeues the job;
* head estimates and backfill use dispatch + prep + max duration while the
  job is pending, then the actual expiry once confirmed;
* confirmation races against the reaper, and only running leases can be
  released.
"""

from __future__ import annotations

import pytest

from app.db import Store
from app.scheduler import (
    ConfirmationNotPossibleError,
    JobNotFoundError,
    LeaseNotHeldError,
    Scheduler,
)


def view(scheduler, job_id):
    return scheduler.get_job(job_id)


# ---------------------------------------------------------------------------
# Dispatch and confirmation lifecycle
# ---------------------------------------------------------------------------


def test_job_without_prep_window_dispatches_as_running(scheduler):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("a", 2, max_duration_seconds=30)

    job = view(scheduler, "a")
    assert job["status"] == "running"
    assert job["prep_timeout_seconds"] is None
    assert job["prep_deadline"] is None
    assert job["confirmed_at"] is not None
    assert job["lease_expires_at"] is not None
    assert job["remaining_seconds"] == pytest.approx(30)


def test_job_with_prep_window_dispatches_as_pending(scheduler, clock):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("a", 2, max_duration_seconds=30, prep_timeout_seconds=10)

    job = view(scheduler, "a")
    assert job["status"] == "pending"
    assert job["assigned_node"] == "n1"
    assert job["prep_timeout_seconds"] == pytest.approx(10)
    assert job["prep_deadline"] is not None
    assert job["lease_expires_at"] is None
    assert job["confirmed_at"] is None
    # While pending, "remaining" counts down the preparation window.
    assert job["remaining_seconds"] == pytest.approx(10)

    clock.advance(4)
    assert view(scheduler, "a")["remaining_seconds"] == pytest.approx(6)
    # The reservation already counts against node capacity.
    node = scheduler.list_nodes()[0]
    assert node["used_slots"] == 2
    assert node["free_slots"] == 2
    assert node["pending_jobs"] == 1
    assert node["running_jobs"] == 0


def test_confirmation_starts_lease_clock(scheduler, clock):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("a", 2, max_duration_seconds=30, prep_timeout_seconds=10)

    clock.advance(4)
    confirmed = scheduler.confirm_job("a")
    assert confirmed["status"] == "running"
    assert confirmed["confirmed_at"] is not None
    assert confirmed["prep_deadline"] is None
    assert confirmed["lease_expires_at"] is not None
    # The 30 s lease starts at confirmation time, not at dispatch time.
    assert confirmed["remaining_seconds"] == pytest.approx(30)

    clock.advance(30)
    events = scheduler.tick()
    assert ("expired", "a") in {(e["type"], e["job_id"]) for e in events}
    assert view(scheduler, "a")["status"] == "expired"


def test_repeated_confirmation_is_idempotent_and_keeps_expiry(scheduler, clock):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("a", 2, max_duration_seconds=30, prep_timeout_seconds=10)

    first = scheduler.confirm_job("a")
    clock.advance(5)
    second = scheduler.confirm_job("a")
    assert second["status"] == "running"
    assert second["lease_expires_at"] == first["lease_expires_at"]
    assert second["remaining_seconds"] == pytest.approx(25)


def test_confirmation_unknown_job_is_404(scheduler):
    with pytest.raises(JobNotFoundError):
        scheduler.confirm_job("missing")


def test_queued_or_finished_job_cannot_be_confirmed(scheduler):
    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 3, max_duration_seconds=10, prep_timeout_seconds=5)
    assert view(scheduler, "a")["status"] == "queued"
    with pytest.raises(ConfirmationNotPossibleError):
        scheduler.confirm_job("a")

    scheduler.submit_job("b", 1, max_duration_seconds=2)
    scheduler.release_job("b")
    with pytest.raises(ConfirmationNotPossibleError):
        scheduler.confirm_job("b")


def test_missed_deadline_reclaims_slots_and_requeues(scheduler, clock):
    # n1 (4 slots): R holds 2 (expires at t+12); the 4-slot head H is blocked;
    # P is a 2-slot backfill whose predicted end (prep 5 + run 4 = t+9)
    # precedes the head's t+12 start, so it is dispatched as pending.
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("R", 2, max_duration_seconds=12)
    scheduler.submit_job("H", 4, max_duration_seconds=30)  # queued head
    scheduler.submit_job(
        "P", 2, max_duration_seconds=4, prep_timeout_seconds=5
    )
    assert view(scheduler, "H")["status"] == "queued"
    assert view(scheduler, "P")["status"] == "pending"

    # Cross R's expiry and P's deadline on the first explicit pass so the
    # sweep's reclaim event is observable.
    clock.advance(12)
    events = scheduler.tick()
    kinds = {(e["type"], e["job_id"]): e for e in events}
    assert ("expired", "R") in kinds
    assert ("released", "P") in kinds
    assert ("dispatched", "H") in kinds
    # The reclaim event is stamped at the preparation deadline.
    assert kinds[("released", "P")]["node_id"] == "n1"
    assert kinds[("released", "P")]["at"] is not None

    # Freed slots let the long-waiting head start; P returns to the queue.
    job = view(scheduler, "P")
    assert job["status"] == "queued"
    assert job["queue_position"] == 1
    assert job["assigned_node"] is None
    assert job["started_at"] is None
    assert job["prep_deadline"] is None
    assert job["remaining_seconds"] is None
    # The preparation timeout configured at submission is retained.
    assert job["prep_timeout_seconds"] == pytest.approx(5)
    assert view(scheduler, "H")["status"] == "running"
    node = scheduler.list_nodes()[0]
    assert node["used_slots"] == 4
    assert node["pending_jobs"] == 0


def test_confirmation_after_deadline_is_rejected(scheduler, clock):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("R", 2, max_duration_seconds=12)
    scheduler.submit_job("H", 4, max_duration_seconds=30)
    scheduler.submit_job(
        "P", 2, max_duration_seconds=4, prep_timeout_seconds=5
    )
    assert view(scheduler, "P")["status"] == "pending"
    clock.advance(12)
    scheduler.tick()  # R expires, P reclaimed, H takes the node
    assert view(scheduler, "P")["status"] == "queued"

    # A timed-out (now queued) job cannot grab slots back via confirmation.
    with pytest.raises(ConfirmationNotPossibleError):
        scheduler.confirm_job("P")


def test_confirmation_past_deadline_but_before_sweep_is_rejected(scheduler, clock):
    # The deadline guard must hold even if the reaper has not run yet:
    # the row is still 'pending' in the database, but confirmation is lost.
    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, max_duration_seconds=30, prep_timeout_seconds=10)
    clock.advance(10.0001)

    with pytest.raises(ConfirmationNotPossibleError):
        scheduler.confirm_job("a")
    # The failed confirmation did not mutate the row.
    job = view(scheduler, "a")
    assert job["status"] == "pending"
    assert job["confirmed_at"] is None

    scheduler.tick()  # the reaper now reclaims the stale reservation
    assert view(scheduler, "a")["status"] in {"pending", "queued"}


def test_reclaimed_job_gets_a_fresh_window_when_slots_return(scheduler, clock):
    # R runs 12 s; head H needs all 4 slots; backfill P (prep 5 + run 4)
    # finishes before H's t+12 start, so it dispatches as pending.
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("R", 2, max_duration_seconds=12)
    scheduler.submit_job("H", 4, max_duration_seconds=8)
    scheduler.submit_job(
        "P", 2, max_duration_seconds=4, prep_timeout_seconds=5
    )
    assert view(scheduler, "P")["status"] == "pending"
    clock.advance(12)
    scheduler.tick()  # R expires, P reclaimed, H starts; P back in the queue
    assert view(scheduler, "P")["status"] == "queued"
    with pytest.raises(ConfirmationNotPossibleError):
        scheduler.confirm_job("P")

    # Once H ends, P reserves again with a brand-new preparation window.
    clock.advance(8)
    events = scheduler.tick()
    assert ("dispatched", "P") in {(e["type"], e["job_id"]) for e in events}
    second = view(scheduler, "P")
    assert second["status"] == "pending"
    assert second["prep_deadline"] is not None
    assert second["remaining_seconds"] == pytest.approx(5)
    # And this time it confirms normally, starting a full-length lease.
    confirmed = scheduler.confirm_job("P")
    assert confirmed["status"] == "running"
    assert confirmed["remaining_seconds"] == pytest.approx(4)


def test_pending_job_cannot_be_released(scheduler):
    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, max_duration_seconds=30, prep_timeout_seconds=10)
    with pytest.raises(LeaseNotHeldError):
        scheduler.release_job("a")
    assert view(scheduler, "a")["status"] == "pending"


def test_release_only_works_after_confirmation(scheduler):
    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, max_duration_seconds=30, prep_timeout_seconds=10)
    scheduler.confirm_job("a")
    released = scheduler.release_job("a")
    assert released["status"] == "completed"
    assert scheduler.list_nodes()[0]["used_slots"] == 0


# ---------------------------------------------------------------------------
# Head estimate and backfill while slots are reserved but not confirmed
# ---------------------------------------------------------------------------


def test_head_estimate_uses_prep_plus_duration_for_pending_holder(scheduler):
    scheduler.configure_node("n1", 6)
    # Pending holder: 2 slots, 10 s prep, then 10 s max run.  Predicted
    # release is dispatch + 20 s.
    scheduler.submit_job(
        "holder", 2, max_duration_seconds=10, prep_timeout_seconds=10
    )
    scheduler.submit_job("head", 5, max_duration_seconds=10)

    head = scheduler.head_estimate()
    assert head["head"]["job_id"] == "head"
    assert head["blocked_reason"] == "waiting_for_free_slots"
    assert head["earliest_start_in_seconds"] == pytest.approx(20)


def test_confirmation_shrinks_head_estimate_to_actual_expiry(scheduler, clock):
    scheduler.configure_node("n1", 6)
    scheduler.submit_job(
        "holder", 2, max_duration_seconds=10, prep_timeout_seconds=10
    )
    scheduler.submit_job("head", 5, max_duration_seconds=10)

    # Confirm immediately: the holder now expires 10 s from now, not 20.
    scheduler.confirm_job("holder")
    head = scheduler.head_estimate()
    assert head["earliest_start_in_seconds"] == pytest.approx(10)


def test_backfill_accounts_for_pending_reservation(scheduler):
    scheduler.configure_node("n1", 6)
    # Pending holder reserves 2 slots until t+10, then may run 10 s, so its
    # predicted release is t+20.
    scheduler.submit_job(
        "holder", 2, max_duration_seconds=10, prep_timeout_seconds=10
    )
    scheduler.submit_job("head", 5, max_duration_seconds=10)  # blocked
    # A 4-slot candidate whose predicted end (10 s prep + 20 s run = t+30)
    # would outlive the head's t+20 start: rejected.
    scheduler.submit_job(
        "long", 4, max_duration_seconds=20, prep_timeout_seconds=10
    )
    # A 2-slot candidate done by t+6 (no prep window): accepted.
    scheduler.submit_job("short", 2, max_duration_seconds=6)

    assert view(scheduler, "long")["status"] == "queued"
    assert view(scheduler, "short")["status"] == "running"
    assert view(scheduler, "holder")["status"] == "pending"
    assert scheduler.head_estimate()["earliest_start_in_seconds"] == pytest.approx(20)


def test_backfill_with_prep_window_uses_dispatch_plus_prep_plus_duration(scheduler):
    scheduler.configure_node("n1", 6)
    scheduler.submit_job(
        "holder", 2, max_duration_seconds=10, prep_timeout_seconds=10
    )  # predicted free at t+20
    scheduler.submit_job("head", 5, max_duration_seconds=10)
    # 2 slots free; candidate prep 3 s + run 15 s -> predicted end t+18 <= 20
    scheduler.submit_job(
        "fits", 2, max_duration_seconds=15, prep_timeout_seconds=3
    )
    # Candidate prep 3 s + run 18 s -> predicted end t+21 > 20
    scheduler.submit_job(
        "misses", 2, max_duration_seconds=18, prep_timeout_seconds=3
    )

    fits = view(scheduler, "fits")
    assert fits["status"] == "pending"
    assert fits["prep_deadline"] is not None
    assert view(scheduler, "misses")["status"] == "queued"
    assert scheduler.head_estimate()["earliest_start_in_seconds"] == pytest.approx(20)


def test_reclaimed_pending_job_keeps_queue_position(scheduler, clock):
    scheduler.configure_node("n1", 2)
    scheduler.submit_job(
        "first", 2, max_duration_seconds=30, prep_timeout_seconds=5
    )
    scheduler.submit_job("second", 1, max_duration_seconds=30)
    assert view(scheduler, "second")["status"] == "queued"

    clock.advance(5)
    events = scheduler.tick()  # first reclaimed; skips redispatch this pass
    kinds = {(e["type"], e["job_id"]) for e in events}
    assert ("released", "first") in kinds
    # "first" is not re-reserved in the same pass...
    assert ("dispatched", "first") not in kinds
    # ...but the still-queued "second" may backfill the freed slots.
    assert ("dispatched", "second") in kinds

    # "first" remains the queue head, ahead of nothing; once "second" ends
    # the head reserves again with a fresh window, FIFO order intact.
    assert view(scheduler, "first")["queue_position"] == 1
    clock.advance(30)
    scheduler.tick()  # "second" expires
    assert view(scheduler, "first")["status"] == "pending"
    assert view(scheduler, "first")["remaining_seconds"] == pytest.approx(5)


# ---------------------------------------------------------------------------
# Interleaved confirmation / reclaim / backfill
# ---------------------------------------------------------------------------


def test_confirm_while_other_pending_jobs_time_out(scheduler, clock):
    # n1 (6 slots): keep and lose are both pending, next runs immediately.
    scheduler.configure_node("n1", 6)
    scheduler.submit_job(
        "keep", 2, max_duration_seconds=30, prep_timeout_seconds=10
    )
    scheduler.submit_job(
        "lose", 2, max_duration_seconds=30, prep_timeout_seconds=5
    )
    scheduler.submit_job("next", 2, max_duration_seconds=30)  # running: 6 held

    # Confirm "keep" before either deadline passes.
    keep = scheduler.confirm_job("keep")
    assert keep["status"] == "running"
    assert keep["remaining_seconds"] == pytest.approx(30)
    keep_expiry = keep["lease_expires_at"]

    clock.advance(6)  # "lose" now passes its 5 s deadline; "keep" is running
    events = scheduler.tick()
    kinds = {(e["type"], e["job_id"]) for e in events}
    assert ("released", "lose") in kinds
    # The missed deadline ends this reservation attempt; "lose" is not
    # re-dispatched within the same pass.
    assert ("dispatched", "lose") not in kinds

    # The next pass re-evaluates the queued job and re-reserves it with a
    # fresh preparation window.
    scheduler.tick()
    lose = view(scheduler, "lose")
    assert lose["status"] == "pending"
    assert lose["remaining_seconds"] == pytest.approx(5)

    # "keep" was untouched by the sweep and keeps its confirmed lease.
    assert view(scheduler, "keep")["status"] == "running"
    assert view(scheduler, "keep")["lease_expires_at"] == keep_expiry
    assert view(scheduler, "next")["status"] == "running"
    assert scheduler.list_nodes()[0]["used_slots"] == 6


def test_deadline_exactly_now_is_reclaimable(scheduler, clock):
    # Boundary: prep_deadline <= now means the reservation is gone.
    # R runs 10 s; head H needs all 4 slots; backfill P (prep 5 + run 4)
    # is pending with a deadline at t+5, but the first sweep runs at t+5.
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("R", 2, max_duration_seconds=10)
    scheduler.submit_job("H", 4, max_duration_seconds=30)  # blocked head
    scheduler.submit_job(
        "P", 2, max_duration_seconds=4, prep_timeout_seconds=5
    )
    assert view(scheduler, "P")["status"] == "pending"
    clock.advance(5)  # P's deadline is exactly now (R still runs until t+10)
    events = scheduler.tick()
    kinds = {(e["type"], e["job_id"]) for e in events}
    assert ("released", "P") in kinds
    # R is still healthy, so H stays blocked and P remains the queue head.
    assert ("dispatched", "H") not in kinds
    assert view(scheduler, "P")["status"] == "queued"
    with pytest.raises(ConfirmationNotPossibleError):
        scheduler.confirm_job("P")


def test_confirm_one_second_before_deadline_succeeds(scheduler, clock):
    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, max_duration_seconds=30, prep_timeout_seconds=10)
    clock.advance(9.999)
    confirmed = scheduler.confirm_job("a")
    assert confirmed["status"] == "running"
    assert confirmed["remaining_seconds"] == pytest.approx(30, abs=0.01)


def test_confirmation_write_failure_keeps_original_state(scheduler, monkeypatch):
    import sqlite3

    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, max_duration_seconds=30, prep_timeout_seconds=10)
    before = scheduler.get_job("a")
    real_connect = scheduler.store._connect

    class FailingConnection:
        """Proxy that fails only the confirmation UPDATE."""

        def __init__(self, inner: sqlite3.Connection):
            object.__setattr__(self, "_inner", inner)

        def execute(self, sql, params=()):
            if isinstance(sql, str) and "SET status = 'running'" in sql:
                raise sqlite3.OperationalError("simulated write failure")
            return self._inner.execute(sql, params)

        def __getattr__(self, name):
            return getattr(object.__getattribute__(self, "_inner"), name)

    monkeypatch.setattr(
        scheduler.store, "_connect", lambda: FailingConnection(real_connect())
    )
    with pytest.raises(sqlite3.OperationalError):
        scheduler.confirm_job("a")
    monkeypatch.undo()

    job = scheduler.get_job("a")
    assert job["status"] == "pending"
    assert job["prep_deadline"] == before["prep_deadline"]
    assert job["confirmed_at"] is None
    # The stored deadline survived and confirmation can still happen.
    confirmed = scheduler.confirm_job("a")
    assert confirmed["status"] == "running"


def test_state_survives_scheduler_restart(db_path, clock):
    scheduler = Scheduler(Store(db_path), clock=clock)
    scheduler.configure_node("n1", 4)
    scheduler.submit_job(
        "a", 2, max_duration_seconds=30, prep_timeout_seconds=10
    )
    original_deadline = scheduler.get_job("a")["prep_deadline"]

    # A brand-new scheduler over the same database file simulates a restart.
    restarted = Scheduler(Store(db_path), clock=clock)
    a = restarted.get_job("a")
    assert a["status"] == "pending"
    # The preparation deadline written before the restart is reused as-is.
    assert a["prep_deadline"] == original_deadline
    assert a["remaining_seconds"] == pytest.approx(10)

    # The stored deadline (not a freshly computed one) governs the sweep.
    clock.advance(10)
    events = restarted.tick()
    released = next(e for e in events if e["type"] == "released")
    assert released["job_id"] == "a"
    assert released["at"] == original_deadline

    # Being head with its node free again, "a" re-reserves with a new window.
    a = restarted.get_job("a")
    assert a["status"] == "pending"
    assert a["prep_deadline"] != original_deadline
    assert a["remaining_seconds"] == pytest.approx(10)


def test_capacity_shrink_counts_pending_reservations(scheduler):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job(
        "a", 3, max_duration_seconds=30, prep_timeout_seconds=10
    )
    with pytest.raises(Exception):
        scheduler.configure_node("n1", 2)
    assert scheduler.configure_node("n1", 3)["capacity"] == 3


def test_pending_listing_filter(scheduler):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job(
        "a", 2, max_duration_seconds=30, prep_timeout_seconds=10
    )
    scheduler.submit_job("b", 1, max_duration_seconds=30)
    pending = scheduler.list_jobs(status="pending")
    running = scheduler.list_jobs(status="running")
    assert [j["job_id"] for j in pending] == ["a"]
    assert [j["job_id"] for j in running] == ["b"]


# ---------------------------------------------------------------------------
# Cross-process races and SQL invariants
# ---------------------------------------------------------------------------


def test_confirm_racing_reaper_never_oversubscribes(db_path, clock):
    import threading

    scheduler = Scheduler(Store(db_path), clock=clock)
    scheduler.configure_node("n1", 5)
    for i in range(8):
        scheduler.submit_job(
            f"j{i}", 1 + (i % 3), max_duration_seconds=5,
            prep_timeout_seconds=2,
        )

    errors: list[BaseException] = []
    barrier = threading.Barrier(6)

    def worker(kind: int) -> None:
        local = Scheduler(Store(db_path), clock=clock)
        barrier.wait()
        try:
            for _ in range(10):
                if kind % 2 == 0:
                    local.tick()
                else:
                    for job in local.list_jobs(status="pending"):
                        try:
                            local.confirm_job(job["job_id"])
                        except Exception:
                            # A reclaim winning the race is a valid outcome.
                            pass
        except BaseException as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    clock.advance(3)
    for t in threads:
        t.join()

    assert not errors
    for node in scheduler.list_nodes():
        assert node["used_slots"] <= node["capacity"]
    held = scheduler.list_jobs()
    by_node: dict[str, int] = {}
    for item in held:
        if item["assigned_node"]:
            by_node[item["assigned_node"]] = (
                by_node.get(item["assigned_node"], 0) + item["required_slots"]
            )
    for node_id, used in by_node.items():
        assert used <= scheduler.get_node(node_id)["capacity"]


def test_pending_reservation_blocks_oversubscription_at_sql_level(scheduler):
    import sqlite3

    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, max_duration_seconds=30, prep_timeout_seconds=10)
    assert view(scheduler, "a")["status"] == "pending"
    conn = scheduler.store._connect()
    try:
        conn.execute(
            "INSERT INTO jobs (job_id, required_slots, max_duration_us,"
            " prep_timeout_us, status, submitted_at_us) VALUES"
            " ('b', 1, 1, 0, 'queued', 1)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE jobs SET status='pending', assigned_node='n1',"
                " started_at_us=1, prep_deadline_us=2, expires_at_us=3"
                " WHERE job_id='b'"
            )
    finally:
        conn.close()
