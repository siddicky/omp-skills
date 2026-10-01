"""Seeded property test for run_dag: random DAGs with random worker and critic behaviour, checked
against a small independent simulation of the rules in the spec (not of the implementation)."""

import asyncio
import json
import os
import random
import shutil
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fake_prelude as fp  # noqa: E402
from fake_prelude import Fail, FailSpawn, Raise, Ret, approve, finding, make_dag, node, revise, worker_ok  # noqa: E402

# Worker behaviours: (builder, outcome). "ok" results are coerced and go to the critic; "fail" is a
# failed attempt (RuntimeError/ValueError/TimeoutError from wait, or a report with status "failed");
# "crash" is any other exception.
WORKERS = [
    (lambda: Ret(worker_ok()), "ok", 52),
    (lambda: Ret(None), "ok", 6),
    (lambda: Ret("plain text report"), "ok", 6),
    (lambda: Ret(json.dumps(worker_ok(files=["x"]))), "ok", 5),
    (lambda: Fail("provider error"), "fail", 15),
    (lambda: Raise(ValueError("Expecting value")), "fail", 6),
    (lambda: Raise(TimeoutError("still running")), "fail", 3),
    (lambda: Ret({**worker_ok(), "status": "failed", "summary": "gave up"}), "fail", 5),  # says it did not finish: no critic
    (lambda: Raise(OSError("bridge went away")), "crash", 3),
]

# Critic behaviours -> symbolic outcome.
CRITICS = [
    (lambda: Ret(approve()), "approve", 30),
    (lambda: Ret(revise(finding("major", "needs work"))), "revise", 14),
    (lambda: Ret(approve(findings=[finding("blocker", "contradiction")])), "revise", 7),  # approve string, blocker finding
    (lambda: Ret(revise(finding("minor", "nit"))), "approve", 7),  # revise string, only a minor finding
    (lambda: Ret(revise(finding("blocker", "criterion is wrong", target="plan"))), "plan", 6),
    (lambda: Ret(None), "bad", 4),
    (lambda: Ret(["approve"]), "bad", 3),
    (lambda: Ret("I approve"), "bad", 3),
    (lambda: Ret({"verdict": "reject", "summary": "criterion 1 fails", "findings": "x missing"}), "bad", 3),  # not approve/revise
    (lambda: Ret({"verdict": "approve"}), "bad", 2),  # no findings list: not a valid approval
    (lambda: Ret({"verdict": "revise", "summary": "again", "findings": []}), "revise", 3),  # revise with nothing to act on
    (lambda: Fail("exited without calling yield"), "bad", 8),
    (lambda: Raise(ValueError("Expecting value")), "bad", 4),
    (lambda: FailSpawn("Unknown agent 'critic'"), "spawn_failed", 4),
    (lambda: Raise(OSError("bridge went away")), "crash", 2),
]


def pick(rng, table):
    total = sum(w for _, _, w in table)
    roll = rng.uniform(0, total)
    for build, outcome, weight in table:
        roll -= weight
        if roll <= 0:
            return build, outcome
    return table[-1][0], table[-1][1]


def make_case(rng):
    n = rng.randint(2, 8)
    ids = [f"N{i}" for i in range(n)]
    nodes = []
    for i, nid in enumerate(ids):
        deps = [d for d in ids[:i] if rng.random() < 0.35]
        nodes.append(node(nid, deps=deps, files=[] if rng.random() < 0.2 else [f"f{i}.txt"]))
    return nodes


def simulate(nodes, workers, critics, max_attempts, already_done=()):
    """Expected (status, attempts, worker_spawns, critic_spawns, reason_prefix) per node, from the rules."""
    status, out = {}, {}
    w_at = {nid: 0 for nid in workers}
    c_at = {nid: 0 for nid in critics}
    for n in nodes:
        nid = n["id"]
        if nid in already_done:
            status[nid] = "done"
            out[nid] = ("done", 0, 0, 0, "")
            continue
        if any(status[d] != "done" for d in n["depends_on"]):
            status[nid] = "skipped"
            out[nid] = ("skipped", 0, 0, 0, "dependency")
            continue
        spawns_w = spawns_c = 0
        result = None
        for _attempt in range(max_attempts):
            outcome = workers[nid][w_at[nid]][1]
            w_at[nid] += 1
            spawns_w += 1
            if outcome == "crash":
                result = ("blocked", "OSError")
                break
            if outcome == "fail":
                result = "retry"
                continue
            verdict = None
            for _try in range(2):
                c_outcome = critics[nid][c_at[nid]][1]
                c_at[nid] += 1
                if c_outcome != "spawn_failed":
                    spawns_c += 1  # a crashing critic was still spawned
                if c_outcome == "crash":
                    verdict = "crash"
                    break
                if c_outcome in ("bad", "spawn_failed"):
                    continue
                verdict = c_outcome
                break
            if verdict == "crash":
                result = ("blocked", "OSError")
                break
            if verdict is None:
                result = ("blocked", "critic failed")
                break
            if verdict == "approve":
                result = ("done", "")
                break
            if verdict == "plan":
                result = ("blocked", "plan defect:")
                break
            result = "retry"
        if result == "retry" or result is None:
            result = ("blocked", "not approved after")
        status[nid] = result[0]
        out[nid] = (result[0], spawns_w, spawns_w, spawns_c, result[1])
    return out


SEED = int(os.environ.get("FUZZ_SEED", "20260930"))  # override to explore other cases
CASES = int(os.environ.get("FUZZ_CASES", "120"))


class RunDagProperties(unittest.IsolatedAsyncioTestCase):
    async def test_random_dags_match_the_rules(self):
        rng = random.Random(SEED)
        env = mock.patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("OMP_SKILLS_TYPESAFE", None)
        mismatches = []
        for case in range(CASES):
            nodes = make_case(rng)
            max_attempts = rng.choice([1, 2, 3])
            limit = rng.choice([1, 2, 4])
            workers = {n["id"]: [pick(rng, WORKERS) for _ in range(max_attempts)] for n in nodes}
            critics = {n["id"]: [pick(rng, CRITICS) for _ in range(2 * max_attempts)] for n in nodes}
            expected = simulate(nodes, workers, critics, max_attempts)

            host = fp.FakeHost()
            ns = fp.make_namespace(host)
            ns["JOB_LIMIT_DELAY"] = 0  # never really sleep in tests
            tmp = tempfile.mkdtemp()
            try:
                for n in nodes:
                    nid = n["id"]
                    host.script(nid, *[build() for build, _ in workers[nid]])
                    host.script(f"critic:{nid}", *[build() for build, _ in critics[nid]])
                dag = make_dag(*nodes)
                state = os.path.join(tmp, "state.json")
                await asyncio.wait_for(ns["run_dag"](dag, state_path=state, max_attempts=max_attempts, max_concurrency=limit), 30)
                problems = self.check(case, nodes, dag, host, ns, expected, state, max_attempts, limit)
                mismatches.extend(problems)
                # a second run over the finished state does nothing
                calls = len(host.calls)
                await asyncio.wait_for(ns["run_dag"](ns["load_dag"](state), state_path=state, max_attempts=max_attempts, max_concurrency=limit), 30)
                if len(host.calls) != calls:
                    mismatches.append(f"case {case}: a finished state was re-run")
            finally:
                host.close()
                shutil.rmtree(tmp, ignore_errors=True)
        self.assertEqual(mismatches[:10], [], f"{len(mismatches)} mismatches")

    def check(self, case, nodes, dag, host, ns, expected, state, max_attempts, limit):
        problems = []
        by_id = {n["id"]: n for n in dag["nodes"]}
        order = [c.label for c in host.calls]
        for n in nodes:
            nid = n["id"]
            got = by_id[nid]
            want_status, want_attempts, want_workers, want_critics, want_reason = expected[nid]
            label = f"case {case} {nid} (deps {n['depends_on']}, max_attempts {max_attempts})"
            if got["status"] != want_status:
                problems.append(f"{label}: status {got['status']} != {want_status}")
                continue
            if len(host.spawns(nid)) != want_workers:
                problems.append(f"{label}: {len(host.spawns(nid))} worker spawns != {want_workers}")
            if len(host.spawns(f"critic:{nid}")) != want_critics:
                problems.append(f"{label}: {len(host.spawns(f'critic:{nid}'))} critic spawns != {want_critics}")
            if want_status != "skipped" and got.get("attempts", 0) != want_attempts:
                problems.append(f"{label}: attempts {got.get('attempts')} != {want_attempts}")
            reason = got.get("blocked_reason") or ""
            if want_status in ("blocked", "skipped") and want_reason not in reason:
                problems.append(f"{label}: reason {reason!r} lacks {want_reason!r}")
            if want_status == "done":
                if got["verdict"]["verdict"] != "approve" or any(f["severity"] in ("blocker", "major") for f in got["verdict"]["findings"]):
                    problems.append(f"{label}: done without a clean approval")
                if any(by_id[d]["status"] != "done" for d in n["depends_on"]):
                    problems.append(f"{label}: done although a dependency is not")
                for dep in n["depends_on"]:  # dependency order: the dependency's critic spawned before this worker
                    if order.index(f"critic:{dep}") > order.index(nid):
                        problems.append(f"{label}: ran before {dep} was reviewed")
            if want_status == "skipped" and (host.spawns(nid) or host.spawns(f"critic:{nid}")):
                problems.append(f"{label}: a skipped node spawned agents")
        # persisted state equals memory, every node terminal, nothing left running
        with open(state, encoding="utf-8") as f:
            saved = json.load(f)
        if saved != json.loads(json.dumps(dag)):
            problems.append(f"case {case}: saved state differs from the returned dag")
        if any(n["status"] not in ("done", "blocked", "skipped") for n in dag["nodes"]):
            problems.append(f"case {case}: a node is not terminal")
        if host.peak > limit:
            problems.append(f"case {case}: {host.peak} jobs at once with limit {limit}")
        if host.running != 0:
            problems.append(f"case {case}: {host.running} jobs still running")
        return problems

    async def test_a_retry_pass_after_a_failed_pass_matches_the_rules(self):
        """Pass 2 sets retry_blocked: done nodes stay, everything else is evaluated again; pass 3 has nothing to do."""
        rng = random.Random(SEED + 1)
        env = mock.patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("OMP_SKILLS_TYPESAFE", None)
        mismatches, rerun_cases = [], 0
        for case in range(CASES):
            nodes = make_case(rng)
            max_attempts = rng.choice([1, 2, 3])
            limit = rng.choice([1, 2, 4])
            host = fp.FakeHost()
            ns = fp.make_namespace(host)
            ns["JOB_LIMIT_DELAY"] = 0  # never really sleep in tests
            tmp = tempfile.mkdtemp()
            try:
                state = os.path.join(tmp, "state.json")
                dag = make_dag(*nodes)

                def queue(already_done=()):
                    host.scripts.clear()
                    workers = {n["id"]: [pick(rng, WORKERS) for _ in range(max_attempts)] for n in nodes}
                    critics = {n["id"]: [pick(rng, CRITICS) for _ in range(2 * max_attempts)] for n in nodes}
                    for n in nodes:
                        host.script(n["id"], *[build() for build, _ in workers[n["id"]]])
                        host.script(f"critic:{n['id']}", *[build() for build, _ in critics[n["id"]]])
                    return simulate(nodes, workers, critics, max_attempts, already_done)

                first = queue()
                await asyncio.wait_for(ns["run_dag"](dag, state_path=state, max_attempts=max_attempts, max_concurrency=limit), 30)
                after_first = {n["id"]: dict(n) for n in dag["nodes"]}
                calls_first = {n["id"]: (len(host.spawns(n["id"])), len(host.spawns(f"critic:{n['id']}"))) for n in nodes}
                done_first = {nid for nid, got in after_first.items() if got["status"] == "done"}
                if any(got["status"] != "done" for got in after_first.values()):
                    rerun_cases += 1

                # pass 2: the user asks for blocked nodes to be retried
                second = queue(done_first)
                reloaded = ns["load_dag"](state)
                reloaded["retry_blocked"] = True
                await asyncio.wait_for(ns["run_dag"](reloaded, state_path=state, max_attempts=max_attempts, max_concurrency=limit), 30)
                for n in nodes:
                    nid = n["id"]
                    got = {x["id"]: x for x in reloaded["nodes"]}[nid]
                    want_status, _a, want_workers, want_critics, want_reason = second[nid]
                    spawned_w = len(host.spawns(nid)) - calls_first[nid][0]
                    spawned_c = len(host.spawns(f"critic:{nid}")) - calls_first[nid][1]
                    label = f"case {case} {nid} pass 2 (was {after_first[nid]['status']}, max_attempts {max_attempts})"
                    if got["status"] != want_status:
                        mismatches.append(f"{label}: status {got['status']} != {want_status}")
                    elif (spawned_w, spawned_c) != (want_workers, want_critics):
                        mismatches.append(f"{label}: spawned {(spawned_w, spawned_c)} != {(want_workers, want_critics)}")
                    elif nid in done_first and got != after_first[nid]:
                        mismatches.append(f"{label}: a done node changed")
                    elif nid not in done_first and want_workers and got.get("attempts") != after_first[nid].get("attempts", 0) + want_workers:
                        mismatches.append(f"{label}: attempts {got.get('attempts')} are not cumulative")
                    if want_status in ("blocked", "skipped") and want_reason not in (got.get("blocked_reason") or ""):
                        mismatches.append(f"{label}: reason {got.get('blocked_reason')!r} lacks {want_reason!r}")
                if "retry_blocked" in reloaded or "retry_blocked" in ns["load_dag"](state):
                    mismatches.append(f"case {case}: retry_blocked was not consumed")

                # pass 3: no flag, fresh scripts that must stay unused
                queue()
                calls = len(host.calls)
                await asyncio.wait_for(ns["run_dag"](ns["load_dag"](state), state_path=state, max_attempts=max_attempts, max_concurrency=limit), 30)
                if len(host.calls) != calls:
                    mismatches.append(f"case {case}: pass 3 ran agents although nothing was retryable")
            finally:
                host.close()
                shutil.rmtree(tmp, ignore_errors=True)
        self.assertEqual(mismatches[:10], [], f"{len(mismatches)} mismatches")
        self.assertGreater(rerun_cases, CASES // 5, "too few cases needed a retry pass")

    async def test_cancelling_at_a_random_moment_leaves_a_resumable_state(self):
        rng = random.Random(SEED + 2)
        env = mock.patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("OMP_SKILLS_TYPESAFE", None)
        problems, cancelled_runs = [], 0
        for case in range(max(CASES // 3, 20)):
            nodes = make_case(rng)
            limit = rng.choice([1, 2, 3])
            host = fp.FakeHost()
            ns = fp.make_namespace(host)
            ns["JOB_LIMIT_DELAY"] = 0  # never really sleep in tests
            tmp = tempfile.mkdtemp()
            try:
                for n in nodes:  # small random delays so that a cancel can land in any phase
                    host.script(n["id"], *[Ret(worker_ok(), delay=rng.uniform(0, 0.02)) for _ in range(2)])
                    host.script(f"critic:{n['id']}", *[Ret(approve(), delay=rng.uniform(0, 0.02)) for _ in range(2)])
                dag = make_dag(*nodes)
                state = os.path.join(tmp, "state.json")
                task = asyncio.ensure_future(ns["run_dag"](dag, state_path=state, max_concurrency=limit))
                await asyncio.sleep(rng.uniform(0, 0.05))
                task.cancel()
                try:
                    await asyncio.wait_for(task, 10)
                except asyncio.CancelledError:
                    cancelled_runs += 1
                for _ in range(300):  # no executor thread may be left waiting on a job
                    if host.running == 0:
                        break
                    await asyncio.sleep(0.01)
                if host.running != 0:
                    problems.append(f"case {case}: {host.running} jobs still running after the cancel")
                if not os.path.exists(state):
                    continue  # cancelled before the first save: nothing to resume
                with open(state, encoding="utf-8") as f:
                    saved = json.load(f)
                statuses = {n["id"]: n["status"] for n in saved["nodes"]}
                if set(statuses.values()) - {"pending", "done", "blocked", "skipped"}:
                    problems.append(f"case {case}: in-flight statuses were saved: {statuses}")
                for n in saved["nodes"]:
                    if n["status"] == "done" and any(statuses[d] != "done" for d in n["depends_on"]):
                        problems.append(f"case {case}: {n['id']} is done but a dependency is not")
                    if n["status"] == "done" and not n.get("evidence") and not n.get("result"):
                        problems.append(f"case {case}: {n['id']} is done without a result")
                done_before = {nid for nid, st in statuses.items() if st == "done"}
                spawned_before = {n["id"]: len(host.spawns(n["id"])) for n in nodes}
                # resume with a healthy host: everything finishes, finished work is not redone
                host.scripts.clear()
                resumed = ns["load_dag"](state)
                await asyncio.wait_for(ns["run_dag"](resumed, state_path=state, max_concurrency=limit), 30)
                if {n["status"] for n in resumed["nodes"]} != {"done"}:
                    problems.append(f"case {case}: the resume did not finish: {[(n['id'], n['status']) for n in resumed['nodes']]}")
                for nid in done_before:
                    if len(host.spawns(nid)) != spawned_before[nid]:
                        problems.append(f"case {case}: finished node {nid} was run again")
            finally:
                host.close()
                shutil.rmtree(tmp, ignore_errors=True)
        self.assertEqual(problems[:10], [], f"{len(problems)} problems")
        self.assertGreater(cancelled_runs, 5, "the random cancels rarely landed mid-run")

    async def test_the_simulation_itself_is_not_vacuous(self):
        rng = random.Random(20260930)
        seen = set()
        for _ in range(120):
            nodes = make_case(rng)
            max_attempts = rng.choice([1, 2, 3])
            rng.choice([1, 2, 4])
            workers = {n["id"]: [pick(rng, WORKERS) for _ in range(max_attempts)] for n in nodes}
            critics = {n["id"]: [pick(rng, CRITICS) for _ in range(2 * max_attempts)] for n in nodes}
            for status, _a, _w, _c, reason in simulate(nodes, workers, critics, max_attempts).values():
                seen.add((status, reason))
        for outcome in (
            ("done", ""), ("skipped", "dependency"), ("blocked", "critic failed"), ("blocked", "plan defect:"),
            ("blocked", "not approved after"), ("blocked", "OSError"),
        ):
            self.assertIn(outcome, seen, f"the generator never produces {outcome}")


if __name__ == "__main__":
    unittest.main()
