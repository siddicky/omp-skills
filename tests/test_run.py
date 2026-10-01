"""run_dag against the scriptable fake host."""

import asyncio
import contextvars
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fake_prelude as fp  # noqa: E402
from fake_prelude import Fail, FailSpawn, Hang, Raise, Ret, approve, finding, make_dag, node, revise, worker_ok  # noqa: E402

JOB_LIMIT = "Background job limit reached (5). Wait for running jobs to finish or cancel one."


class RunCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        env = mock.patch.dict(os.environ)  # a developer's OMP_SKILLS_TYPESAFE must not change these tests
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("OMP_SKILLS_TYPESAFE", None)
        self.host = fp.FakeHost()
        self.addCleanup(self.host.close)
        self.ns = fp.make_namespace(self.host)
        self.ns["JOB_LIMIT_DELAY"] = 0  # never really sleep in tests
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.state = os.path.join(self.tmp, "dag", "state.json")

    async def run_dag(self, dag, **kw):
        kw.setdefault("state_path", self.state)
        kw.setdefault("max_concurrency", 4)
        return await asyncio.wait_for(self.ns["run_dag"](dag, **kw), timeout=30)

    def saved(self):
        with open(self.state, encoding="utf-8") as f:
            return json.load(f)

    def saved_node(self, nid):
        return next(n for n in self.saved()["nodes"] if n["id"] == nid)

    async def until(self, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while True:
            try:
                if predicate():
                    return
            except (FileNotFoundError, StopIteration):  # the state file does not exist yet
                pass
            if time.monotonic() > deadline:
                self.fail("condition not reached in time")
            await asyncio.sleep(0.01)

    def statuses(self, dag):
        return {n["id"]: n["status"] for n in dag["nodes"]}


class HappyPath(RunCase):
    async def test_example_json_runs_to_completion(self):
        src = os.path.join(self.tmp, "example.json")
        shutil.copy(os.path.join(fp.DAG_DIR, "example.json"), src)
        state_path, dag, resumed = self.ns["prepare_dag"](src, state_dir=os.path.join(self.tmp, "dag"))
        self.assertFalse(resumed)
        out = await self.run_dag(dag, state_path=state_path)
        self.assertIs(out, dag)
        self.assertEqual(self.statuses(out), {"N-001": "done", "N-002": "done", "N-003": "done", "N-004": "done"})
        self.assertEqual(len(self.host.worker_calls()), 4)
        self.assertEqual(len(self.host.critic_calls()), 4)
        for call in self.host.worker_calls():
            self.assertEqual(call.agent, "sonic")  # the node's own agent
            self.assertIs(call.schema, self.ns["WORKER_SCHEMA"])
            self.assertNotIn("isolated", call.kwargs)
        for call in self.host.critic_calls():
            self.assertIs(call.schema, self.ns["CRITIC_SCHEMA"])
        order = [c.label for c in self.host.calls]
        self.assertLess(order.index("critic:N-001"), order.index("N-003"))
        self.assertLess(order.index("critic:N-002"), order.index("N-003"))
        self.assertLess(order.index("critic:N-003"), order.index("N-004"))
        with open(state_path, encoding="utf-8") as f:
            on_disk = json.load(f)
        self.assertEqual(on_disk, json.loads(json.dumps(out)))
        self.assertNotIn("isolated", on_disk)
        self.assertEqual(self.ns["summarize"](out).split("\n")[0], "COMPLETE: all 4 nodes done")
        self.assertEqual(self.host.phases, [f"dag: {dag['goal']}"])

    async def test_labels_and_default_agent(self):
        dag = make_dag(node("A"), node("B", agent="sonic"))
        await self.run_dag(dag)
        self.assertEqual({c.label: c.agent for c in self.host.calls}, {"A": "task", "critic:A": "critic", "B": "sonic", "critic:B": "critic"})

    async def test_evidence_flows_to_dependents(self):
        self.host.script("A", Ret(worker_ok(files=["a.txt"])))
        dag = make_dag(node("A"), node("B", deps=["A"]))
        await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertIn("files changed:\n- a.txt", a["evidence"])
        self.assertIn("[1] check 1\nok 1", a["evidence"])
        prompt = self.host.prompts("B")[0]
        self.assertIn("## A\nfiles changed:\n- a.txt\n[1] check 1\nok 1", prompt)

    async def test_state_records_ids_attempts_and_the_critic_verdict(self):
        dag = make_dag(node("A"))
        await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertEqual(a["status"], "done")
        self.assertEqual(a["attempts"], 1)
        self.assertEqual(a["worker_ids"], ["A"])
        self.assertEqual(a["critic_ids"], ["criticA"])
        self.assertEqual(a["verdict"]["verdict"], "approve")
        self.assertEqual(a["result"]["status"], "done")
        self.assertNotIn("blocked_reason", a)

    async def test_every_transition_is_persisted(self):
        snapshots = []
        real = self.ns["_persist"]

        def spy(dag, path):
            snapshots.append(dag["nodes"][0]["status"])
            real(dag, path)

        self.ns["_persist"] = spy
        await self.run_dag(make_dag(node("A")))
        collapsed = [s for i, s in enumerate(snapshots) if i == 0 or s != snapshots[i - 1]]
        self.assertEqual(collapsed, ["pending", "running", "review", "done"])

    async def test_state_file_is_never_torn(self):
        for nid in "ABCDEF":
            self.host.script(nid, Ret(worker_ok(), delay=0.02))
            self.host.script(f"critic:{nid}", Ret(approve(), delay=0.02))
        problems, reads, stop = [], [0], threading.Event()

        def reader():
            while not stop.is_set():
                try:
                    with open(self.state, encoding="utf-8") as f:
                        json.load(f)
                    reads[0] += 1
                except FileNotFoundError:
                    pass
                except Exception as e:  # noqa: BLE001
                    problems.append(repr(e))

        thread = threading.Thread(target=reader)
        thread.start()
        try:
            await self.run_dag(make_dag(*[node(c) for c in "ABCDEF"]))
        finally:
            stop.set()
            thread.join()
        self.assertEqual(problems, [])
        self.assertGreater(reads[0], 5)
        self.assertEqual(os.listdir(os.path.dirname(self.state)), ["state.json"])

    async def test_text_and_empty_worker_results_are_coerced(self):
        self.host.script("A", Ret("plain prose, not json"))
        self.host.script("B", Ret(None))
        self.host.script("C", Ret(json.dumps(worker_ok(files=["c.txt"]))))
        dag = make_dag(node("A"), node("B"), node("C"))
        await self.run_dag(dag)
        self.assertEqual(set(self.statuses(dag).values()), {"done"})
        self.assertEqual(self.saved_node("A")["result"]["summary"], "plain prose, not json")
        self.assertEqual(self.saved_node("A")["evidence"], "plain prose, not json")
        self.assertEqual(self.saved_node("C")["result"]["files_changed"], ["c.txt"])
        self.assertIn('"summary": "plain prose, not json"', self.host.prompts("critic:A")[0])


class Preflight(RunCase):
    async def test_unapproved_raises_before_any_spawn(self):
        for approved in (False, None, "true", 1):
            dag = make_dag(node("A"), approved=approved)
            with self.assertRaisesRegex(ValueError, "not approved"):
                await self.run_dag(dag)
        dag = make_dag(node("A"))
        del dag["approved"]
        with self.assertRaises(ValueError):
            await self.run_dag(dag)
        self.assertEqual(self.host.calls, [])
        self.assertEqual(self.host.phases, [])
        self.assertFalse(os.path.exists(self.state))

    async def test_cyclic_raises_before_any_spawn(self):
        dag = make_dag(node("N-001", deps=["N-002"]), node("N-002", deps=["N-001"]))
        with self.assertRaisesRegex(ValueError, "invalid DAG: cycle: N-001 -> N-002 -> N-001"):
            await self.run_dag(dag)
        self.assertEqual(self.host.calls, [])
        self.assertFalse(os.path.exists(self.state))

    async def test_other_invalid_shapes_raise_before_any_spawn(self):
        missing_files = node("A")
        del missing_files["files"]
        overlapping = (node("A", files=["x"]), node("B", files=["x"]))
        for dag in (
            make_dag(node("A", deps=["ZZZ"])),
            make_dag(missing_files),
            make_dag(*overlapping),
            {"approved": True},
            {"approved": True, "nodes": []},
            make_dag(node("A", status="completed")),
        ):
            with self.assertRaises(ValueError):
                await self.run_dag(dag)
        self.assertEqual(self.host.calls, [])

    async def test_isolation_makes_overlapping_files_legal(self):
        dag = make_dag(node("A", files=["x"]), node("B", files=["x"]))
        await self.run_dag(dag, isolated=True)
        self.assertEqual(set(self.statuses(dag).values()), {"done"})

    async def test_max_attempts_must_be_a_positive_int(self):
        for bad in (0, -1, True, "2", 1.5, None):
            with self.assertRaises(ValueError, msg=repr(bad)):
                await self.run_dag(make_dag(node("A")), max_attempts=bad)
        self.assertEqual(self.host.calls, [])

    async def test_stories_shaped_dag_is_rejected_clearly(self):
        with self.assertRaises(ValueError):
            await self.run_dag({"approved": True, "stories": [node("A")]})


class IsolatedFlag(RunCase):
    async def test_isolated_key_is_omitted_when_false(self):
        # A host in plan mode rejects any isolated key, even isolated=False.
        self.host.reject_kwarg = ("isolated", "Subagent isolation, apply, and merge controls are unavailable in plan mode.")
        dag = make_dag(node("A"), node("B", deps=["A"]))
        await self.run_dag(dag, isolated=False)
        self.assertEqual(set(self.statuses(dag).values()), {"done"})
        for call in self.host.calls:
            self.assertNotIn("isolated", call.kwargs)
            self.assertEqual(set(call.kwargs), {"agent", "label", "schema"})

    async def test_isolated_true_applies_to_workers_only(self):
        dag = make_dag(node("A"))
        await self.run_dag(dag, isolated=True)
        self.assertIs(self.host.spawns("A")[0].kwargs["isolated"], True)
        self.assertNotIn("isolated", self.host.spawns("critic:A")[0].kwargs)

    async def test_isolated_is_never_persisted(self):
        dag = make_dag(node("A"), isolated=True)  # stale key from an older run
        await self.run_dag(dag, isolated=False)
        self.assertNotIn("isolated", self.saved())
        self.assertNotIn("isolated", dag)
        dag = make_dag(node("A"))
        await self.run_dag(dag, isolated=True)
        self.assertNotIn("isolated", self.saved())


class CriticGate(RunCase):
    async def test_approve_with_a_blocker_becomes_revise_and_retries(self):
        self.host.script("critic:A", Ret(approve(findings=[finding("blocker", "criterion 1 fails", "make it pass")])), Ret(approve()))
        dag = make_dag(node("A"))
        await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertEqual(a["status"], "done")
        self.assertEqual(a["attempts"], 2)
        self.assertEqual(len(self.host.spawns("A")), 2)
        self.assertIn("[blocker] criterion 1 fails - fix: make it pass", self.host.prompts("A")[1])
        self.assertTrue(any("'approve'" in m and "'revise'" in m for m in self.host.logs))
        self.assertNotIn("verdict_overridden", a)  # the final verdict agreed with its findings

    async def test_a_persistently_contradictory_critic_blocks_the_node(self):
        verdict = approve(findings=[finding("major", "still wrong", "fix it")])
        self.host.script("critic:A", Ret(verdict), Ret(verdict))
        dag = make_dag(node("A"), node("B", deps=["A"]))
        await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertEqual((a["status"], a["attempts"]), ("blocked", 2))
        self.assertIs(a["verdict_overridden"], True)
        self.assertEqual(a["verdict"]["verdict"], "revise")
        self.assertIn("[major] still wrong", a["blocked_reason"])
        self.assertEqual(self.saved_node("B")["status"], "skipped")

    async def test_revise_with_only_minor_findings_is_approved(self):
        self.host.script("critic:A", Ret(revise(finding("minor", "nit"), summary="polish")))
        dag = make_dag(node("A"))
        await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertEqual((a["status"], a["attempts"]), ("done", 1))
        self.assertIs(a["verdict_overridden"], True)
        self.assertEqual(self.ns["summarize"](dag).split("\n")[0], "COMPLETE: all 1 nodes done")

    async def test_revise_feeds_the_findings_into_the_next_worker_prompt(self):
        self.host.script("critic:A", Ret(revise(finding("major", "criterion 2 prints nothing", "print the value", evidence="saw empty"))), Ret(approve()))
        await self.run_dag(make_dag(node("A")))
        first, second = self.host.prompts("A")
        self.assertNotIn("Prior critic findings", first)
        self.assertIn("- [major] criterion 2 prints nothing - fix: print the value (evidence: saw empty)", second)

    async def test_out_of_attempts_lists_the_findings(self):
        self.host.script("critic:A", Ret(revise(finding("blocker", "first"))), Ret(revise(finding("major", "second", "do b"), finding("minor", "nit"))))
        dag = make_dag(node("A"))
        await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertEqual(a["status"], "blocked")
        self.assertEqual(a["blocked_reason"], "not approved after 2 attempts: [major] second (fix: do b)")
        self.assertEqual(len(self.host.worker_calls()), 2)

    async def test_max_attempts_is_respected(self):
        for attempts in (1, 3):
            self.host.calls.clear()
            verdict = revise(finding("major", "no"))
            for _ in range(attempts):
                self.host.script("critic:A", Ret(verdict))
            dag = make_dag(node("A"))
            await self.run_dag(dag, max_attempts=attempts)
            self.assertEqual(len(self.host.spawns("A")), attempts)
            self.assertEqual(dag["nodes"][0]["attempts"], attempts)
            self.assertIn(f"after {attempts} attempts", dag["nodes"][0]["blocked_reason"])

    async def test_a_revise_verdict_is_on_disk_before_the_next_attempt_spawns(self):
        self.host.script("critic:A", Ret(revise(finding("major", "needs work", "do more"))))
        on_disk = []

        def peek(call):
            on_disk.append(self.saved_node("A").get("verdict"))
            return Ret(worker_ok())

        self.host.script("A", None, peek)
        await self.run_dag(make_dag(node("A")))
        self.assertEqual(len(on_disk), 1)
        self.assertEqual(on_disk[0]["verdict"], "revise")
        self.assertEqual(on_disk[0]["findings"][0]["issue"], "needs work")


class CriticFailure(RunCase):
    async def test_malformed_critic_gets_one_retry_then_the_node_is_blocked(self):
        for first, second in ((None, ["x"]), ("prose", 7), ({}, "{"), ("[1]", None)):
            self.host.calls.clear()
            self.host.script("critic:A", Ret(first), Ret(second))
            dag = make_dag(node("A"), node("B", deps=["A"]))
            with self.subTest(first=first, second=second):
                await self.run_dag(dag)
                a = self.saved_node("A")
                self.assertEqual(a["status"], "blocked")
                self.assertTrue(a["blocked_reason"].startswith("critic failed: "), a["blocked_reason"])
                self.assertEqual(len(self.host.spawns("critic:A")), 2)  # one retry, not more
                self.assertEqual(len(self.host.spawns("A")), 1)  # the worker is never re-run for a critic problem
                self.assertEqual(a["attempts"], 1)
                self.assertEqual(a["result"]["summary"], "done")  # the worker's report is kept
                self.assertEqual(self.saved_node("B")["status"], "skipped")

    async def test_a_critic_that_does_not_say_approve_or_revise_never_approves(self):
        # audit adversarial-review B1: omp passes the raw data through after three schema failures
        shapes = (
            {"verdict": "reject", "summary": "criterion 1 fails", "findings": "greeting.txt missing"},
            {"verdict": "needs work"},
            {"verdict": "fail", "summary": "criterion 1 not met"},
            {"verdict": "reject", "findings": []},
            {"verdict": "approve"},
            {"findings": []},
        )
        for shape in shapes:
            self.host.calls.clear()
            self.host.script("critic:A", Ret(shape), Ret(shape))
            dag = make_dag(node("A"), node("B", deps=["A"]))
            with self.subTest(shape=shape):
                await self.run_dag(dag)
                a = self.saved_node("A")
                self.assertEqual(a["status"], "blocked")
                self.assertTrue(a["blocked_reason"].startswith("critic failed: "), a["blocked_reason"])
                self.assertEqual(len(self.host.spawns("critic:A")), 2)  # one retry, as for any unusable answer
                self.assertEqual(self.saved_node("B")["status"], "skipped")
                self.assertTrue(any("A: critic output is unusable" in m for m in self.host.logs))

    async def test_revise_without_findings_is_a_revise_that_retries_the_worker(self):
        self.host.script("critic:A", Ret({"verdict": "revise", "summary": "criterion 1 fails: file missing", "findings": []}), Ret(approve()))
        dag = make_dag(node("A"))
        await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertEqual((a["status"], a["attempts"]), ("done", 2))
        self.assertIn("criterion 1 fails: file missing", self.host.prompts("A")[1])

    async def test_a_critic_that_recovers_on_the_retry_is_used(self):
        broken_critics = (
            Ret(None),
            Fail("Subagent exited without calling yield tool after 3 reminders."),
            Raise(ValueError("Expecting value")),
            FailSpawn("boom"),
        )
        for broken in broken_critics:
            self.host.calls.clear()
            self.host.spawn_errors.clear()
            self.host.script("critic:A", broken, Ret(approve()))
            dag = make_dag(node("A"))
            await self.run_dag(dag)
            a = self.saved_node("A")
            self.assertEqual((a["status"], a["attempts"]), ("done", 1), broken)
            # two critic runs, except that a spawn failure never produced a job
            self.assertEqual(len(self.host.spawns("critic:A")) + len(self.host.spawn_errors), 2)
            self.assertEqual(len(self.host.spawns("A")), 1)

    async def test_critic_that_never_works_names_the_failure(self):
        self.host.script("critic:A", Fail("no yield"), Fail("still no yield"))
        await self.run_dag(make_dag(node("A")))
        self.assertEqual(self.saved_node("A")["blocked_reason"], "critic failed: RuntimeError: still no yield")
        self.host.calls.clear()
        self.host.script("critic:A", FailSpawn("Unknown agent 'critic'"), FailSpawn("Unknown agent 'critic'"))
        await self.run_dag(make_dag(node("A")))
        self.assertEqual(self.saved_node("A")["blocked_reason"], "critic failed: RuntimeError: Unknown agent 'critic'")

    async def test_critic_failure_after_a_revise_keeps_the_second_report(self):
        self.host.script("A", Ret(worker_ok(summary="first")), Ret(worker_ok(summary="second")))
        self.host.script("critic:A", Ret(revise(finding("major", "again"))), Ret(None), Ret(None))
        await self.run_dag(make_dag(node("A")))
        a = self.saved_node("A")
        self.assertEqual((a["status"], a["attempts"]), ("blocked", 2))
        self.assertEqual(a["result"]["summary"], "second")
        self.assertTrue(a["blocked_reason"].startswith("critic failed"))


class WorkerFailure(RunCase):
    async def test_a_failed_worker_is_retried_with_the_failure_as_feedback(self):
        self.host.script("A", Fail("provider error 529"), Ret(worker_ok()))
        dag = make_dag(node("A"), node("B", deps=["A"]))
        await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertEqual((a["status"], a["attempts"]), ("done", 2))
        self.assertEqual(len(self.host.spawns("critic:A")), 1)  # no critic for a run that never produced a report
        self.assertIn("worker failed: RuntimeError: provider error 529", self.host.prompts("A")[1])
        self.assertEqual(self.saved_node("B")["status"], "done")

    async def test_all_attempts_failing_blocks_without_calling_the_critic(self):
        self.host.script("A", Fail("boom one"), Fail("boom two"))
        dag = make_dag(node("A"), node("B", deps=["A"]), node("C"))
        await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertEqual((a["status"], a["attempts"]), ("blocked", 2))
        self.assertIn("worker failed: RuntimeError: boom two", a["blocked_reason"])
        self.assertEqual(self.host.spawns("critic:A"), [])
        self.assertEqual(self.statuses(dag), {"A": "blocked", "B": "skipped", "C": "done"})

    async def test_value_and_timeout_errors_count_as_failed_attempts(self):
        for exc in (ValueError("Expecting value: line 1"), TimeoutError("still running")):
            self.host.calls.clear()
            self.host.script("A", Raise(exc), Ret(worker_ok()))
            dag = make_dag(node("A"))
            await self.run_dag(dag)
            self.assertEqual((dag["nodes"][0]["status"], dag["nodes"][0]["attempts"]), ("done", 2), repr(exc))
            self.assertIn(f"worker failed: {type(exc).__name__}", self.host.prompts("A")[1])

    async def test_a_criterion_number_that_int_cannot_read_does_not_crash_the_node(self):
        # "²".isdigit() is True but int("²") raises: the report could not be read and the node was blocked
        report = {**worker_ok(), "evidence": [{"criterion": "²", "command": "check A", "observed": "ok", "passed": True}]}
        self.host.script("A", Ret(report))
        await self.run_dag(make_dag(node("A")))
        a = self.saved_node("A")
        self.assertEqual((a["status"], a["attempts"]), ("done", 1))
        self.assertEqual(a["result"]["evidence"][0]["criterion"], "²")  # not a criterion number: kept as it came

    async def test_an_unexpected_error_blocks_the_node_and_the_run_still_finishes(self):
        self.host.script("A", Raise(OSError("bridge went away")))
        dag = make_dag(node("A"), node("B", deps=["A"]), node("C"))
        await self.run_dag(dag)
        self.assertEqual(self.statuses(dag), {"A": "blocked", "B": "skipped", "C": "done"})
        self.assertEqual(self.saved_node("A")["blocked_reason"], "OSError: bridge went away")
        self.assertEqual(self.saved_node("B")["blocked_reason"], "dependency A not done")

    async def test_a_worker_that_reports_failed_is_a_failed_attempt_without_a_critic(self):
        # audit adversarial-review m4: the worker said it did not finish, so there is nothing to approve
        failed = {**worker_ok(), "status": "failed", "summary": "could not finish", "notes": "no write tool"}
        self.host.script("A", Ret(failed), Ret(worker_ok()))
        dag = make_dag(node("A"), node("B", deps=["A"]))
        await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertEqual((a["status"], a["attempts"]), ("done", 2))
        self.assertEqual(len(self.host.spawns("critic:A")), 1)  # only the second attempt was reviewed
        self.assertIn("- [major] worker reported failure: could not finish no write tool", self.host.prompts("A")[1])
        self.assertEqual(a["result"]["status"], "done")
        self.assertEqual(self.saved_node("B")["status"], "done")
        self.assertTrue(any("A worker reported failure (attempt 1/2)" in m for m in self.host.logs), self.host.logs)

    async def test_every_attempt_reporting_failed_blocks_the_node_and_keeps_the_report(self):
        failed = {**worker_ok(), "status": "failed", "summary": "gave up", "notes": "read-only"}
        self.host.script("A", Ret(failed), Ret(failed))
        dag = make_dag(node("A"), node("B", deps=["A"]))
        await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertEqual((a["status"], a["attempts"]), ("blocked", 2))
        self.assertEqual(a["blocked_reason"], "not approved after 2 attempts: [major] worker reported failure: gave up read-only (fix: complete the task and every acceptance criterion; return status done only when they all pass)")
        self.assertEqual(self.host.critic_calls(), [])
        self.assertEqual(a["result"]["status"], "failed")  # the report stays in the state for the user
        self.assertNotIn("evidence", a)  # nothing for dependents to build on
        self.assertEqual(self.saved_node("B")["status"], "skipped")

    async def test_a_failed_report_is_failed_even_with_passing_evidence(self):
        failed = {**worker_ok(n=3), "status": " FAILED ", "summary": "evidence looks fine but I am not done"}
        self.host.script("A", Ret(failed), Ret(worker_ok()))
        dag = make_dag(node("A"))
        await self.run_dag(dag)
        self.assertEqual(len(self.host.spawns("A")), 2)
        self.assertEqual(len(self.host.spawns("critic:A")), 1)

    async def test_a_failed_report_resumes_with_the_reason_as_feedback(self):
        failed = {**worker_ok(), "status": "failed", "summary": "gave up"}
        self.host.script("A", Ret(failed), Ret(failed))
        dag = make_dag(node("A"))
        await self.run_dag(dag)
        doc = self.saved()
        doc["retry_blocked"] = True
        with open(self.state, "w", encoding="utf-8") as f:
            json.dump(doc, f)
        await self.run_dag(self.ns["load_dag"](self.state))
        self.assertIn("worker reported failure: gave up", self.host.prompts("A")[2])


class PlanDefects(RunCase):
    async def test_a_plan_defect_blocks_immediately(self):
        defect = finding("blocker", "criterion 1 is unsatisfiable as written", "rewrite the criterion", target="plan")
        self.host.script("critic:A", Ret(revise(defect)))
        dag = make_dag(node("A"), node("B", deps=["A"]))
        await self.run_dag(dag, max_attempts=5)
        a = self.saved_node("A")
        self.assertEqual((a["status"], a["attempts"]), ("blocked", 1))
        self.assertTrue(a["blocked_reason"].startswith("plan defect:"), a["blocked_reason"])
        self.assertIn("criterion 1 is unsatisfiable as written", a["blocked_reason"])
        self.assertEqual(len(self.host.spawns("A")), 1)
        self.assertEqual(len(self.host.spawns("critic:A")), 1)
        self.assertEqual(self.saved_node("B")["status"], "skipped")

    async def test_one_plan_finding_among_work_findings_still_blocks(self):
        self.host.script("critic:A", Ret(revise(finding("major", "fix the code", target="work"), finding("major", "criterion is wrong", target="plan"))))
        await self.run_dag(make_dag(node("A")), max_attempts=3)
        a = self.saved_node("A")
        self.assertTrue(a["blocked_reason"].startswith("plan defect:"))
        self.assertNotIn("fix the code", a["blocked_reason"])
        self.assertEqual(a["attempts"], 1)

    async def test_a_minor_plan_note_does_not_block(self):
        self.host.script("critic:A", Ret(approve(findings=[finding("minor", "wording could be clearer", target="plan")])))
        dag = make_dag(node("A"))
        await self.run_dag(dag)
        self.assertEqual(dag["nodes"][0]["status"], "done")

    async def test_work_findings_alone_are_retried(self):
        self.host.script("critic:A", Ret(revise(finding("blocker", "wrong", target="work"))), Ret(approve()))
        dag = make_dag(node("A"))
        await self.run_dag(dag)
        self.assertEqual(dag["nodes"][0]["attempts"], 2)

    async def test_plan_findings_are_not_fed_to_a_retry_worker(self):
        self.host.script("critic:A", Ret(revise(finding("major", "do better", target="work"), finding("minor", "plan nit", target="plan"))), Ret(approve()))
        await self.run_dag(make_dag(node("A")))
        self.assertIn("do better", self.host.prompts("A")[1])
        self.assertNotIn("plan nit", self.host.prompts("A")[1])


class Cancellation(RunCase):
    async def start(self, dag, **kw):
        kw.setdefault("state_path", self.state)
        kw.setdefault("max_concurrency", 4)
        return asyncio.ensure_future(self.ns["run_dag"](dag, **kw))

    async def test_cancel_cancels_live_handles_and_resets_statuses(self):
        self.host.script("A", Hang())
        self.host.script("B", Hang())
        dag = make_dag(node("A"), node("B"), node("C", deps=["A", "B"]))
        task = await self.start(dag)
        await self.until(lambda: len(self.host.calls) == 2)
        self.assertEqual(self.saved_node("A")["status"], "running")
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(set(self.host.cancelled), {"A", "B"})
        self.assertEqual(self.statuses(self.saved()), {"A": "pending", "B": "pending", "C": "pending"})
        self.assertEqual(self.statuses(dag), {"A": "pending", "B": "pending", "C": "pending"})
        await self.until(lambda: self.host.running == 0)  # no executor thread is left waiting

    async def test_cancel_during_review_resets_to_pending_and_cancels_the_critic(self):
        self.host.script("critic:A", Hang())
        dag = make_dag(node("A"), node("B", deps=["A"]))
        task = await self.start(dag)
        await self.until(lambda: len(self.host.spawns("critic:A")) == 1)
        self.assertEqual(self.saved_node("A")["status"], "review")
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(set(self.host.cancelled), {"criticA"})
        self.assertEqual(self.statuses(self.saved()), {"A": "pending", "B": "pending"})

    async def test_cancel_keeps_finished_work(self):
        self.host.script("B", Hang())
        dag = make_dag(node("A"), node("B"))
        task = await self.start(dag)
        await self.until(lambda: self.saved_node("A")["status"] == "done")
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.statuses(self.saved()), {"A": "done", "B": "pending"})

    async def test_a_resume_after_cancel_does_not_redo_finished_nodes(self):
        self.host.script("B", Hang())
        dag = make_dag(node("A"), node("B"))
        task = await self.start(dag)
        await self.until(lambda: self.saved_node("A")["status"] == "done")
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        spawned_before = len(self.host.spawns("A"))
        state_path, resumed_dag, resumed = self.ns["prepare_dag"](self.state, state_dir=os.path.dirname(self.state))
        self.assertTrue(resumed)
        await self.run_dag(resumed_dag)
        self.assertEqual(len(self.host.spawns("A")), spawned_before)
        self.assertEqual(self.statuses(resumed_dag), {"A": "done", "B": "done"})
        self.assertEqual(self.saved_node("B")["attempts"], 2)  # the interrupted attempt still counts

    async def test_an_aborted_wait_is_a_cancellation_not_a_node_failure(self):
        for message in ("Operation aborted", "eval cell was interrupted"):
            self.host.calls.clear()
            self.host.script("A", Fail(message))
            dag = make_dag(node("A"), node("B", deps=["A"]))
            with self.assertRaises(asyncio.CancelledError):
                await self.run_dag(dag)
            self.assertEqual(self.statuses(self.saved()), {"A": "pending", "B": "pending"}, message)
            self.assertNotIn("blocked_reason", self.saved_node("A"))
            self.assertEqual(len(self.host.spawns("A")), 1)  # no second attempt after an abort

    async def test_an_unexpected_error_in_one_node_stops_the_other_nodes_too(self):
        # e.g. the state file cannot be written: the error escapes one node task, and gather does not cancel the others
        self.host.script("A", Hang())
        real = self.ns["_persist"]

        def disk_full_once_both_have_started(obj, path):
            if len(self.host.calls) >= 2:
                raise OSError("disk full")
            real(obj, path)

        self.ns["_persist"] = disk_full_once_both_have_started
        with self.assertRaises(OSError):
            await self.run_dag(make_dag(node("A"), node("B")))
        self.ns["_persist"] = real
        await asyncio.sleep(0.2)  # a task that was not stopped treats A's cancelled job as a failed attempt and respawns it
        self.assertEqual(len(self.host.spawns("A")), 1)
        self.assertIn("A", self.host.cancelled)

    async def test_a_save_that_fails_right_after_a_spawn_cancels_the_new_job(self):
        # the job is not tracked until its wait starts, so without this it would keep running unwatched
        self.host.script("A", Hang())
        real = self.ns["_persist"]

        def disk_full_once_a_job_started(obj, path):
            if self.host.calls:
                raise OSError("disk full")
            real(obj, path)

        self.ns["_persist"] = disk_full_once_a_job_started
        self.addCleanup(self.ns.__setitem__, "_persist", real)
        with self.assertRaises(OSError):
            await self.run_dag(make_dag(node("A")))
        self.assertEqual(len(self.host.spawns("A")), 1)
        self.assertIn("A", self.host.cancelled)


class RunState(RunCase):
    """The shared run object: whole-run teardown must not depend on each task getting to clean up."""

    def run_object(self, dag):
        return self.ns["_Run"](dag, self.state, 2, False, 4000, 2, "off")

    def probe(self, fail=False):
        seen = []

        class Handle:
            id = "probe"

            def cancel(inner):
                seen.append(inner)
                if fail:
                    raise ConnectionError("bridge rejected the call")
                return True

        return Handle(), seen

    async def test_unwind_cancels_every_handle_and_survives_failures(self):
        run = self.run_object(make_dag(node("A")))
        self.addCleanup(run.pool.shutdown)
        good, seen_good = self.probe()
        bad, seen_bad = self.probe(fail=True)
        also_good, seen_also = self.probe()
        run.live.update({good, bad, also_good})
        run.unwind()
        self.assertEqual((len(seen_good), len(seen_bad), len(seen_also)), (1, 1, 1))
        self.assertEqual(run.live, set())

    async def test_unwind_resets_in_flight_nodes_cancels_jobs_and_saves(self):
        dag = make_dag(node("A", status="running"), node("B", status="review"), node("C", status="done"), node("D", status="blocked"), node("E"))
        run = self.run_object(dag)
        self.addCleanup(run.pool.shutdown)
        handle, seen = self.probe()
        run.live.add(handle)
        run.unwind("because", "E")
        self.assertEqual(len(seen), 1)
        self.assertEqual(self.statuses(dag), {"A": "pending", "B": "pending", "C": "done", "D": "blocked", "E": "pending"})
        saved = self.saved()
        self.assertEqual(self.statuses(saved), self.statuses(dag))
        # audit adversarial-review R2-07: only the node that could not start carries the reason; the ones that
        # were merely in flight are told the run was aborted, not handed another node's error
        errors = {n["id"]: n.get("last_error") for n in saved["nodes"] if n.get("last_error")}
        self.assertEqual(errors, {"A": "interrupted: run aborted at E", "B": "interrupted: run aborted at E", "E": "because"})

    async def test_unwind_still_cancels_when_the_state_cannot_be_saved(self):
        run = self.run_object(make_dag(node("A", status="running")))
        self.addCleanup(run.pool.shutdown)
        handle, seen = self.probe()
        run.live.add(handle)
        with mock.patch.object(run, "persist", side_effect=OSError("read-only file system")):
            run.unwind()  # must not raise: the cancellation is what matters
        self.assertEqual(len(seen), 1)
        self.assertTrue(any("could not save state" in m for m in self.host.logs))

    async def test_wait_tracks_a_handle_only_while_it_is_being_waited_for(self):
        run = self.run_object(make_dag(node("A")))
        self.addCleanup(run.pool.shutdown)
        during = []

        class Handle:
            id = "probe"

            def wait(inner, timeout=None):
                during.append(inner in run.live)
                return "done"

            def cancel(inner):
                return False

        self.assertEqual(await run.wait(Handle()), "done")
        self.assertEqual(during, [True])
        self.assertEqual(run.live, set())

    async def test_the_wait_pool_is_shut_down_when_the_run_ends(self):
        import concurrent.futures

        made = []

        class Spy(concurrent.futures.ThreadPoolExecutor):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.shut_down = False
                if kwargs.get("thread_name_prefix") == "dag-wait":
                    made.append(self)

            def shutdown(self, *args, **kwargs):
                self.shut_down = True
                super().shutdown(*args, **kwargs)

        with mock.patch.object(concurrent.futures, "ThreadPoolExecutor", Spy):
            await self.run_dag(make_dag(node("A")))
        self.assertEqual(len(made), 1)
        self.assertTrue(made[0].shut_down)


class StatePath(RunCase):
    async def test_a_relative_state_path_survives_a_chdir_during_the_run(self):
        elsewhere = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, elsewhere, True)
        home = os.getcwd()
        self.addCleanup(os.chdir, home)
        os.chdir(self.tmp)

        def wander(call):
            os.chdir(elsewhere)  # a cell elsewhere in the kernel changes directory mid-run
            return Ret(worker_ok())

        self.host.script("A", wander)
        dag = make_dag(node("A"), node("B", deps=["A"]))
        await self.ns["run_dag"](dag, state_path="state.json", max_concurrency=2)
        self.assertTrue(os.path.exists(os.path.join(self.tmp, "state.json")))
        self.assertFalse(os.path.exists(os.path.join(elsewhere, "state.json")))
        with open(os.path.join(self.tmp, "state.json"), encoding="utf-8") as f:
            self.assertEqual({n["id"]: n["status"] for n in json.load(f)["nodes"]}, {"A": "done", "B": "done"})


class SpawnFailures(RunCase):
    async def test_spawn_failure_aborts_without_blocking_anything(self):
        self.host.script("A", Hang())
        self.host.script("B", FailSpawn("Unknown agent 'nope'"))
        dag = make_dag(node("A"), node("B"), node("C", deps=["A"]))
        with self.assertRaisesRegex(RuntimeError, r"^dag aborted: .*B.*Unknown agent 'nope'"):
            await self.run_dag(dag)
        states = self.statuses(self.saved())
        self.assertEqual(states, {"A": "pending", "B": "pending", "C": "pending"})
        self.assertNotIn("blocked", states.values())
        self.assertIn("Unknown agent 'nope'", self.saved_node("B")["last_error"])
        # audit adversarial-review R2-07: A was running when B could not start; it must not blame B's error on itself
        self.assertEqual(self.saved_node("A")["last_error"], "interrupted: run aborted at B")
        self.assertNotIn("last_error", self.saved_node("C"))
        self.assertEqual(set(self.host.cancelled), {"A"})
        self.assertEqual(self.host.critic_calls(), [])
        await self.until(lambda: self.host.running == 0)

    async def test_the_aborted_state_resumes_cleanly(self):
        self.host.script("B", FailSpawn("Unknown agent 'nope'"))
        dag = make_dag(node("A"), node("B"))
        with self.assertRaises(RuntimeError):
            await self.run_dag(dag)
        state_path, again, resumed = self.ns["prepare_dag"](self.state, state_dir=os.path.dirname(self.state))
        self.assertIs(resumed, True)  # every node is pending, but the run has a history (audit real-omp-e2e:V1-03)
        self.assertIn("Unknown agent 'nope'", self.ns["summarize"](again))  # and the table says why it stopped
        await self.run_dag(again)  # the host is fixed now: scripts are used up
        self.assertEqual(set(self.statuses(again).values()), {"done"})
        self.assertNotIn("last_error", self.saved_node("B"))

    async def test_a_stale_last_error_is_cleared_when_the_node_runs_again(self):
        self.host.script("B", FailSpawn("Unknown agent 'nope'"))
        dag = make_dag(node("A"), node("B"))
        with self.assertRaises(RuntimeError):
            await self.run_dag(dag)
        self.assertIn("Unknown agent", self.saved_node("B")["last_error"])
        self.host.scripts.clear()
        self.host.script("B", Fail("first"), Fail("second"))  # this time it runs, and then blocks
        _, again, _ = self.ns["prepare_dag"](self.state, state_dir=os.path.dirname(self.state))
        await self.run_dag(again)
        b = self.saved_node("B")
        self.assertEqual(b["status"], "blocked")
        self.assertNotIn("last_error", b)

    async def test_plan_mode_style_failure_on_every_spawn(self):
        self.host.reject_kwarg = ("isolated", "Subagent isolation, apply, and merge controls are unavailable in plan mode.")
        dag = make_dag(node("A"), node("B"))
        with self.assertRaisesRegex(RuntimeError, "dag aborted: .*plan mode"):
            await self.run_dag(dag, isolated=True)
        self.assertEqual(set(self.statuses(self.saved()).values()), {"pending"})
        self.assertEqual(self.host.calls, [])

    async def test_a_failed_second_attempt_spawn_aborts_too(self):
        self.host.script("critic:A", Ret(revise(finding("major", "again"))))
        self.host.script("A", None, FailSpawn("bridge down"))
        dag = make_dag(node("A"))
        with self.assertRaisesRegex(RuntimeError, "dag aborted: .*bridge down"):
            await self.run_dag(dag)
        a = self.saved_node("A")
        self.assertEqual(a["status"], "pending")
        self.assertEqual(a["attempts"], 1)  # the second attempt never started
        self.assertIn("bridge down", a["last_error"])

    async def test_job_limit_spawns_are_retried_after_a_pause(self):
        self.ns["JOB_LIMIT_DELAY"] = 5.0
        self.host.script("A", FailSpawn(JOB_LIMIT), FailSpawn(JOB_LIMIT), FailSpawn(JOB_LIMIT))
        sleep = mock.AsyncMock()
        with mock.patch.object(asyncio, "sleep", sleep):
            dag = make_dag(node("A"))
            await self.run_dag(dag)
        self.assertEqual(sleep.await_args_list, [mock.call(5.0)] * 3)
        self.assertEqual(dag["nodes"][0]["status"], "done")
        self.assertEqual(len(self.host.spawns("A")), 1)
        self.assertEqual(len(self.host.spawn_errors), 3)
        self.assertEqual(dag["nodes"][0]["attempts"], 1)
        self.assertTrue(any("job limit" in m for m in self.host.logs))

    async def test_job_limit_defaults(self):
        pristine = fp.make_namespace(fp.FakeHost())
        self.assertEqual(pristine["JOB_LIMIT_RETRIES"], 3)
        self.assertEqual(pristine["JOB_LIMIT_DELAY"], 5.0)

    async def test_job_limit_retries_run_out(self):
        self.host.script("A", *[FailSpawn(JOB_LIMIT)] * 4)
        dag = make_dag(node("A"))
        with self.assertRaisesRegex(RuntimeError, "dag aborted: .*Background job limit reached"):
            await self.run_dag(dag)
        self.assertEqual(len(self.host.spawn_errors), 4)  # the first try plus three retries
        self.assertEqual(self.saved_node("A")["status"], "pending")

    async def test_critic_spawns_retry_on_the_job_limit_too(self):
        self.host.script("critic:A", FailSpawn(JOB_LIMIT), None)
        dag = make_dag(node("A"))
        await self.run_dag(dag)
        self.assertEqual(dag["nodes"][0]["status"], "done")
        self.assertEqual(len(self.host.spawn_errors), 1)

    async def test_other_spawn_errors_are_not_retried(self):
        self.host.script("A", FailSpawn("Unknown agent 'x'"), None)
        with self.assertRaises(RuntimeError):
            await self.run_dag(make_dag(node("A")))
        self.assertEqual(len(self.host.spawn_errors), 1)
        self.assertEqual(len(self.host.spawns("A")), 0)


class Resume(RunCase):
    async def blocked_run(self, dag=None, **kw):
        """A run where A's critic keeps asking for changes, so A ends blocked."""
        verdict = revise(finding("major", "finding-alpha: output is wrong", "do alpha"))
        self.host.script("critic:A", Ret(verdict), Ret(verdict))
        dag = dag or make_dag(node("A"), node("B", deps=["A"]), node("C"))
        await self.run_dag(dag, **kw)
        return dag

    def reload(self):
        return self.ns["load_dag"](self.state)

    def flag_retry(self, value=True):
        doc = self.saved()
        doc["retry_blocked"] = value
        with open(self.state, "w", encoding="utf-8") as f:
            json.dump(doc, f)

    async def test_blocked_nodes_stay_blocked_without_retry_blocked(self):
        await self.blocked_run()
        calls = len(self.host.calls)
        dag = self.reload()
        await self.run_dag(dag)
        self.assertEqual(len(self.host.calls), calls)
        self.assertEqual(self.statuses(dag), {"A": "blocked", "B": "skipped", "C": "done"})

    async def test_retry_blocked_is_one_shot(self):
        await self.blocked_run()
        self.flag_retry()
        self.host.script("critic:A", Ret(revise(finding("major", "still bad"))), Ret(revise(finding("major", "still bad"))))
        dag = self.reload()
        self.assertIs(dag["retry_blocked"], True)
        await self.run_dag(dag)
        self.assertNotIn("retry_blocked", self.saved())  # consumed
        self.assertNotIn("retry_blocked", dag)
        self.assertEqual(self.saved_node("A")["attempts"], 4)  # cumulative across runs
        self.assertEqual(self.saved_node("A")["status"], "blocked")
        # audit adversarial-review R2-08: the reason counts what the node has used, not this run's budget of 2
        self.assertTrue(self.saved_node("A")["blocked_reason"].startswith("not approved after 4 attempts: "), self.saved_node("A")["blocked_reason"])
        calls = len(self.host.calls)
        await self.run_dag(self.reload())  # a third run must not retry again
        self.assertEqual(len(self.host.calls), calls)
        self.assertEqual(self.saved_node("A")["status"], "blocked")

    async def test_retry_blocked_can_finish_the_job(self):
        await self.blocked_run()
        self.flag_retry()
        dag = self.reload()
        await self.run_dag(dag)  # scripts exhausted: default critic approves
        self.assertEqual(self.statuses(dag), {"A": "done", "B": "done", "C": "done"})
        self.assertNotIn("blocked_reason", self.saved_node("A"))
        self.assertEqual(self.saved_node("A")["attempts"], 3)

    async def test_retry_blocked_false_or_odd_values_retry_nothing(self):
        await self.blocked_run()
        for value in (False, "false", "no", "yes", 0, "0", 2, None, []):
            self.flag_retry(value)
            calls = len(self.host.calls)
            dag = self.reload()
            await self.run_dag(dag)
            self.assertEqual(len(self.host.calls), calls, value)
            self.assertNotIn("retry_blocked", self.saved(), value)

    async def test_retry_blocked_is_read_the_way_a_hand_edit_spells_it(self):
        # audit adversarial-review R2-09: "true" or 1 typed into the state file was consumed and silently ignored
        for value in (True, "true", "TRUE", " true ", 1, "1"):
            await self.blocked_run()
            self.flag_retry(value)
            dag = self.reload()
            await self.run_dag(dag)
            self.assertEqual(self.statuses(dag), {"A": "done", "B": "done", "C": "done"}, repr(value))
            self.assertNotIn("retry_blocked", self.saved(), repr(value))  # and it is still one-shot

    async def test_a_damaged_attempts_count_starts_over_and_a_good_one_carries_on(self):
        # audit tests-prelude:V1-T3: True is not "1 attempt" (bool is an int subclass), and junk is not a count
        for stored, expected in ((True, 1), (False, 1), ("x", 1), ("²", 1), (None, 1), (2.5, 1), ("3", 4), (2, 3), (2.0, 3)):
            dag = make_dag(node("A", attempts=stored))
            await self.run_dag(dag)
            self.assertEqual(self.saved_node("A")["attempts"], expected, repr(stored))

    async def test_a_retry_still_sees_the_findings_that_blocked_it(self):
        await self.blocked_run()
        self.flag_retry()
        await self.run_dag(self.reload())
        first_retry_prompt = self.host.prompts("A")[2]
        self.assertIn("# Prior critic findings (must fix)", first_retry_prompt)
        self.assertIn("finding-alpha: output is wrong - fix: do alpha", first_retry_prompt)

    async def test_prior_findings_of_an_interrupted_node_are_kept(self):
        dag = make_dag(node("A", status="review", attempts=1, worker_ids=["old"], verdict=revise(finding("major", "finding-beta", "do beta"))))
        await self.run_dag(dag)
        self.assertIn("finding-beta - fix: do beta", self.host.prompts("A")[0])
        self.assertEqual(self.saved_node("A")["attempts"], 2)
        self.assertEqual(self.saved_node("A")["worker_ids"], ["old", "A"])

    async def test_skipped_nodes_are_re_evaluated_on_resume(self):
        await self.blocked_run()
        doc = self.saved()
        self.assertEqual(self.statuses(doc)["B"], "skipped")
        doc["nodes"][0]["status"] = "pending"  # the user fixed A by hand; no retry_blocked involved
        with open(self.state, "w", encoding="utf-8") as f:
            json.dump(doc, f)
        dag = self.reload()
        await self.run_dag(dag)
        self.assertEqual(self.statuses(dag), {"A": "done", "B": "done", "C": "done"})
        self.assertNotIn("blocked_reason", self.saved_node("B"))
        self.assertEqual(len(self.host.spawns("B")), 1)

    async def test_skipped_nodes_stay_skipped_while_their_dependency_is_blocked(self):
        await self.blocked_run()
        calls = len(self.host.calls)
        dag = self.reload()
        await self.run_dag(dag)
        b = self.saved_node("B")
        self.assertEqual((b["status"], b["blocked_reason"]), ("skipped", "dependency A not done"))
        self.assertEqual(len(self.host.calls), calls)

    async def test_interrupted_nodes_run_again_from_pending(self):
        dag = make_dag(node("A", status="running", attempts=1, worker_ids=["x"]), node("B", status="review", attempts=1, critic_ids=["y"]), node("C", status="done", evidence="kept"))
        await self.run_dag(dag)
        self.assertEqual(set(self.statuses(dag).values()), {"done"})
        self.assertEqual(len(self.host.spawns("A")), 1)
        self.assertEqual(len(self.host.spawns("B")), 1)
        self.assertEqual(self.host.spawns("C"), [])
        self.assertTrue(any("A was interrupted" in m for m in self.host.logs))

    async def test_done_nodes_are_not_rerun_and_feed_their_evidence_forward(self):
        dag = make_dag(node("A", status="done", evidence="stored proof", attempts=1), node("B", deps=["A"]))
        await self.run_dag(dag)
        self.assertEqual(self.host.spawns("A"), [])
        self.assertIn("## A\nstored proof", self.host.prompts("B")[0])

    async def test_a_resumed_run_persists_the_reset_before_any_agent_starts(self):
        dag = make_dag(node("A", status="running"))
        snapshots = []
        real = self.ns["_persist"]

        def spy(d, path):
            snapshots.append(d["nodes"][0]["status"])
            real(d, path)

        self.ns["_persist"] = spy
        await self.run_dag(dag)
        self.assertEqual(snapshots[0], "pending")

    async def test_skipped_is_derived_state_and_is_reset_before_anything_runs(self):
        dag = make_dag(
            node("A", status="blocked", blocked_reason="boom"),
            node("B", deps=["A"], status="skipped", blocked_reason="dependency A not done"),
            node("C", deps=["B"], status="skipped", blocked_reason="dependency B not done"),
        )
        snapshots = []
        real = self.ns["_persist"]

        def spy(d, path):
            snapshots.append({n["id"]: (n["status"], n.get("blocked_reason")) for n in d["nodes"]})
            real(d, path)

        self.ns["_persist"] = spy
        await self.run_dag(dag)
        self.assertEqual(snapshots[0], {"A": ("blocked", "boom"), "B": ("pending", None), "C": ("pending", None)})
        self.assertEqual(self.statuses(dag), {"A": "blocked", "B": "skipped", "C": "skipped"})  # re-derived
        self.assertEqual(self.saved_node("C")["blocked_reason"], "dependency B not done")

    async def test_nodes_without_a_status_start_pending(self):
        n = node("A")
        del n["status"]
        dag = make_dag(n)
        await self.run_dag(dag)
        self.assertEqual(dag["nodes"][0]["status"], "done")

    async def test_interrupted_review_and_running_nodes_are_pending_on_disk_before_any_agent_starts(self):
        dag = make_dag(
            node("A", status="review", attempts=1, worker_ids=["A"], critic_ids=["criticA"]),
            node("B", status="running", attempts=1, worker_ids=["B"]),
        )
        first_sight = {}

        def peek(call):
            first_sight[call.label] = self.saved_node(call.label)["status"]
            return Ret(worker_ok())

        self.host.script("A", peek)
        self.host.script("B", peek)
        await self.run_dag(dag)
        self.assertEqual(first_sight, {"A": "pending", "B": "pending"})
        self.assertEqual(self.statuses(dag), {"A": "done", "B": "done"})


class PrdRoundTrip(RunCase):
    """prepare_dag -> run_dag -> sync_prd on a ralplan-shaped PRD, then Resume and Restart (audit X-01)."""

    def write_prd(self):
        self.prd = os.path.join(self.tmp, "prd.json")
        doc = {
            "goal": "Ship the thing",
            "source_spec": ".omp/pipeline/specs/ship-the-thing.md",
            "approved": True,
            "created": "2026-09-30T00:00:00Z",
            "open_findings": [],
            "stories": [
                node("US-001"),
                node("US-002", deps=["US-001"]),
                node("US-003"),
            ],
        }
        with open(self.prd, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)
        self.state_dir = os.path.join(self.tmp, "dag")
        return doc

    def prd_json(self):
        with open(self.prd, encoding="utf-8") as f:
            return json.load(f)

    async def test_status_mirror_and_restart(self):
        original = self.write_prd()
        state_path, dag, resumed = self.ns["prepare_dag"](self.prd, state_dir=self.state_dir)
        self.assertEqual((os.path.basename(state_path), resumed), ("ship-the-thing.json", False))
        bad = revise(finding("major", "wrong"))
        self.host.script("critic:US-001", Ret(bad), Ret(bad))
        await self.run_dag(dag, state_path=state_path)
        self.assertEqual(self.statuses(dag), {"US-001": "blocked", "US-002": "skipped", "US-003": "done"})

        self.assertEqual(self.ns["sync_prd"](dag, self.prd), 3)
        mirrored = self.prd_json()
        self.assertEqual([s["status"] for s in mirrored["stories"]], ["blocked", "skipped", "done"])
        self.assertNotIn("nodes", mirrored)
        for key in ("goal", "source_spec", "approved", "created", "open_findings"):
            self.assertEqual(mirrored[key], original[key])
        for before, after in zip(original["stories"], mirrored["stories"]):
            self.assertEqual({k: v for k, v in after.items() if k != "status"}, {k: v for k, v in before.items() if k != "status"})

        # a bare re-invocation resumes the saved state instead of re-copying the PRD over it
        before = os.path.getsize(state_path)
        again_path, again, resumed = self.ns["prepare_dag"](self.prd, state_dir=self.state_dir)
        self.assertEqual((again_path, resumed), (state_path, True))
        self.assertEqual(self.statuses(again), {"US-001": "blocked", "US-002": "skipped", "US-003": "done"})
        self.assertEqual(os.path.getsize(state_path), before)

        # Restart copies from the PRD, whose mirrored statuses must not leak into the new run
        fresh = self.ns["init_dag"](self.prd, state_path)
        self.assertEqual(set(self.statuses(fresh).values()), {"pending"})
        self.assertNotIn("evidence", fresh["nodes"][2])
        self.host.calls.clear()
        await self.run_dag(fresh, state_path=state_path)
        self.assertEqual(self.statuses(fresh), {"US-001": "done", "US-002": "done", "US-003": "done"})
        self.assertEqual(len(self.host.spawns("US-003")), 1)  # re-run for real, with its own evidence

    async def test_a_replanned_prd_never_runs_the_old_state(self):
        # audit adversarial-review B2: an aborted run leaves every node pending, then ralplan rewrites the PRD
        self.write_prd()
        state_path, dag, resumed = self.ns["prepare_dag"](self.prd, state_dir=self.state_dir)
        self.state = state_path  # saved() and saved_node() read the state file the run uses
        self.host.script("US-001", FailSpawn("Subagent isolation, apply, and merge controls are unavailable in plan mode."))
        with self.assertRaisesRegex(RuntimeError, "dag aborted"):
            await self.run_dag(dag, state_path=state_path)
        self.assertEqual(self.saved_node("US-001")["status"], "pending")  # an aborted spawn blocks nothing
        self.assertNotIn("blocked", self.statuses(self.saved()).values())

        replanned = self.prd_json()
        replanned["approved"] = False
        replanned["stories"][0]["task"] = "NEW TASK: edit a.txt"
        replanned.pop("approved_at", None)
        with open(self.prd, "w", encoding="utf-8") as f:
            json.dump(replanned, f)
        with self.assertRaisesRegex(ValueError, "not approved"):
            self.ns["prepare_dag"](self.prd, state_dir=self.state_dir)  # the unapproved plan is not bypassed
        self.ns["approve_file"](self.prd)
        with self.assertRaises(self.ns["StaleState"]) as caught:
            self.ns["prepare_dag"](self.prd, state_dir=self.state_dir)
        self.assertEqual(self.saved_node("US-001")["task"], "Do US-001")  # the old run was left alone

        self.host.calls.clear()
        fresh = self.ns["init_dag"](caught.exception.source, caught.exception.state_path)  # Restart
        await self.run_dag(fresh, state_path=state_path)
        self.assertEqual(set(self.statuses(fresh).values()), {"done"})
        prompts = "\n".join(c.prompt for c in self.host.worker_calls())
        self.assertIn("NEW TASK: edit a.txt", prompts)
        self.assertNotIn("Do US-001", prompts)  # nothing of the old plan ran

    async def test_finishing_the_old_run_after_a_replan_never_marks_the_new_stories(self):
        # audit adversarial-review R2-03: StaleState, Cancel, then `/skill:dag <state path>` and the completion cell.
        # The old run's terminal statuses describe stories that no longer exist; the new PRD must stay untouched.
        self.write_prd()
        state_path, dag, _ = self.ns["prepare_dag"](self.prd, state_dir=self.state_dir)
        self.state = state_path
        replanned = self.prd_json()
        replanned["stories"][0]["task"] = "Something entirely different"
        with open(self.prd, "w", encoding="utf-8") as f:
            json.dump(replanned, f)
        with self.assertRaisesRegex(self.ns["StaleState"], "changed: US-001"):
            self.ns["prepare_dag"](self.prd, state_dir=self.state_dir)

        old_path, old, resumed = self.ns["prepare_dag"](state_path, state_dir=self.state_dir)  # the state path, as is
        self.assertEqual((old_path, resumed, old["source"]), (state_path, False, self.prd))
        await self.run_dag(old, state_path=old_path)
        self.assertEqual(self.statuses(old), {"US-001": "done", "US-002": "done", "US-003": "done"})
        self.assertIn("Do US-001", self.host.prompts("US-001")[0])  # the old task is what ran

        with open(self.prd, "rb") as f:
            before = f.read()
        self.assertEqual(self.ns["sync_prd"](old, old["source"]), 0)
        with open(self.prd, "rb") as f:
            self.assertEqual(f.read(), before)
        self.assertEqual({s["status"] for s in self.prd_json()["stories"]}, {"pending"})
        self.assertTrue(
            any("no longer holds the plan" in m and "changed: US-001" in m for m in self.host.logs), self.host.logs
        )

        # a Restart copies the new plan, and then the mirror works again
        fresh = self.ns["init_dag"](self.prd, state_path)
        await self.run_dag(fresh, state_path=state_path)
        self.assertEqual(self.ns["sync_prd"](fresh, self.prd), 3)
        self.assertEqual({s["status"] for s in self.prd_json()["stories"]}, {"done"})

    async def test_an_unapproved_prd_never_reaches_the_runner(self):
        doc = self.write_prd()
        doc["approved"] = False
        with open(self.prd, "w", encoding="utf-8") as f:
            json.dump(doc, f)
        with self.assertRaisesRegex(ValueError, "not approved"):
            self.ns["prepare_dag"](self.prd, state_dir=self.state_dir)
        self.ns["approve_file"](self.prd)
        state_path, dag, resumed = self.ns["prepare_dag"](self.prd, state_dir=self.state_dir)
        await self.run_dag(dag, state_path=state_path)
        self.assertEqual(set(self.statuses(dag).values()), {"done"})
        self.assertIn("stories", self.prd_json())  # approving did not turn the PRD into a DAG file


class ScreenModeFailsOpen(RunCase):
    async def test_a_broken_typesafe_mode_means_off(self):
        def boom(dag=None):
            raise RuntimeError("config unreadable")

        self.ns["typesafe_mode"] = boom
        dag = make_dag(node("A"))
        await self.run_dag(dag)
        self.assertEqual(dag["nodes"][0]["status"], "done")
        self.assertEqual(self.host.judge_batches, [])

    async def test_a_runner_loaded_without_judgments_means_off(self):
        ns = fp.make_namespace(self.host, judgments=False)
        self.assertNotIn("typesafe_mode", ns)
        dag = make_dag(node("A"))
        await ns["run_dag"](dag, state_path=self.state, max_concurrency=2)
        self.assertEqual(dag["nodes"][0]["status"], "done")


class Concurrency(RunCase):
    def slow(self, ids):
        for nid in ids:
            self.host.script(nid, Ret(worker_ok(), delay=0.03))
            self.host.script(f"critic:{nid}", Ret(approve(), delay=0.02))

    async def test_semaphore_bound_is_respected(self):
        ids = [f"N{i}" for i in range(8)]
        for limit in (1, 2, 3):
            self.host.peak = 0
            self.host.calls.clear()
            self.slow(ids)
            dag = make_dag(*[node(i) for i in ids])
            await self.run_dag(dag, max_concurrency=limit)
            self.assertEqual(set(self.statuses(dag).values()), {"done"})
            self.assertEqual(self.host.peak, limit, f"limit {limit}")  # reached, never exceeded

    async def test_workers_and_critics_share_the_bound(self):
        ids = [f"N{i}" for i in range(6)]
        self.slow(ids)
        await self.run_dag(make_dag(*[node(i) for i in ids]), max_concurrency=2)
        self.assertLessEqual(self.host.peak, 2)

    async def test_independent_nodes_do_run_in_parallel(self):
        ids = [f"N{i}" for i in range(6)]
        self.slow(ids)
        await self.run_dag(make_dag(*[node(i) for i in ids]), max_concurrency=6)
        self.assertGreaterEqual(self.host.peak, 4)

    async def test_waiting_on_dependencies_does_not_hold_a_slot(self):
        # limit 1: the dependent waits for A without occupying the only slot, so A can run.
        self.host.script("A", Ret(worker_ok(), delay=0.02))
        dag = make_dag(node("B", deps=["A"]), node("A"))  # B is first in the list
        await self.run_dag(dag, max_concurrency=1)
        self.assertEqual(set(self.statuses(dag).values()), {"done"})
        self.assertEqual(self.host.peak, 1)

    async def test_default_bound_comes_from_omp_config(self):
        asked = []

        def fake_detect(default=4):
            asked.append(default)
            return 1

        self.ns["detect_max_concurrency"] = fake_detect
        ids = [f"N{i}" for i in range(4)]
        self.slow(ids)
        await self.run_dag(make_dag(*[node(i) for i in ids]), max_concurrency=None)
        self.assertEqual(len(asked), 1)
        self.assertEqual(self.host.peak, 1)

    async def test_an_explicit_bound_does_not_ask_omp(self):
        self.ns["detect_max_concurrency"] = lambda default=4: self.fail("asked omp")
        await self.run_dag(make_dag(node("A")), max_concurrency=2)

    async def test_dependency_order_is_respected_under_a_tight_bound(self):
        dag = make_dag(node("A"), node("B", deps=["A"]), node("C", deps=["B"]), node("D", deps=["A"]))
        await self.run_dag(dag, max_concurrency=1)
        order = [c.label for c in self.host.calls if c.agent != "critic"]
        self.assertLess(order.index("A"), order.index("B"))
        self.assertLess(order.index("B"), order.index("C"))
        self.assertLess(order.index("A"), order.index("D"))

    async def test_a_negative_bound_still_runs_one_at_a_time(self):
        for bad in (-1, -5):
            self.host.peak = 0
            self.host.calls.clear()
            dag = make_dag(node("A"), node("B"))
            await self.run_dag(dag, max_concurrency=bad)
            self.assertEqual(set(self.statuses(dag).values()), {"done"}, bad)
            self.assertEqual(self.host.peak, 1, bad)

    async def test_every_slot_has_its_own_live_wait(self):
        # the host cancels a job on Esc only while a wait for it is live, so a bound of 3 needs 3 concurrent waits
        waiting, peak, lock = [0], [0], threading.Lock()
        real_wait = fp.FakeHandle.wait

        def counting_wait(handle, timeout=None):
            with lock:
                waiting[0] += 1
                peak[0] = max(peak[0], waiting[0])
            try:
                return real_wait(handle, timeout)
            finally:
                with lock:
                    waiting[0] -= 1

        ids = [f"N{i}" for i in range(6)]
        self.slow(ids)
        with mock.patch.object(fp.FakeHandle, "wait", counting_wait):
            await self.run_dag(make_dag(*[node(i) for i in ids]), max_concurrency=3)
        self.assertEqual(peak[0], 3)


class DependencySkips(RunCase):
    async def test_dependents_of_a_blocked_node_are_skipped_transitively(self):
        self.host.script("A", Fail("x"), Fail("y"))
        dag = make_dag(node("A"), node("B", deps=["A"]), node("C", deps=["B"]), node("D"))
        await self.run_dag(dag)
        self.assertEqual(self.statuses(dag), {"A": "blocked", "B": "skipped", "C": "skipped", "D": "done"})
        self.assertEqual(self.saved_node("B")["blocked_reason"], "dependency A not done")
        self.assertEqual(self.saved_node("C")["blocked_reason"], "dependency B not done")
        self.assertEqual(self.host.spawns("B") + self.host.spawns("C"), [])
        self.assertEqual(self.ns["summarize"](dag).split("\n")[0], "INCOMPLETE: 1 blocked, 2 skipped, 0 pending (of 4)")

    async def test_a_node_waits_for_all_of_its_dependencies(self):
        self.host.script("A", Ret(worker_ok(), delay=0.05))
        dag = make_dag(node("A"), node("B"), node("C", deps=["A", "B"]))
        await self.run_dag(dag)
        order = [c.label for c in self.host.calls]
        self.assertLess(order.index("critic:A"), order.index("C"))
        self.assertLess(order.index("critic:B"), order.index("C"))

    async def test_one_blocked_dependency_skips_a_node_even_if_the_other_is_done(self):
        self.host.script("A", Fail("x"), Fail("y"))
        dag = make_dag(node("A"), node("B"), node("C", deps=["A", "B"]))
        await self.run_dag(dag)
        self.assertEqual(self.saved_node("C")["blocked_reason"], "dependency A not done")

    async def test_every_bad_dependency_is_named_in_the_reason_in_order(self):
        self.host.script("A", Fail("x"), Fail("y"))
        self.host.script("B", Fail("x"), Fail("y"))
        dag = make_dag(node("A"), node("B"), node("C", deps=["B", "A"]))
        await self.run_dag(dag)
        self.assertEqual(self.saved_node("C")["blocked_reason"], "dependency A, B not done")


class WaitHandle(unittest.IsolatedAsyncioTestCase):
    """_wait_handle: a thread-pool wait that carries the caller's context and cancels the job."""

    async def asyncSetUp(self):
        self.host = fp.FakeHost()
        self.addCleanup(self.host.close)
        self.ns = fp.make_namespace(self.host)

    def handle(self, behaviour):
        return self.host.agent("p", label="x") if behaviour is None else self._spawn(behaviour)

    def _spawn(self, behaviour):
        self.host.script("x", behaviour)
        return self.host.agent("p", label="x")

    async def test_returns_the_result(self):
        h = self._spawn(Ret({"v": 1}))
        self.assertEqual(await self.ns["_wait_handle"](h), {"v": 1})

    async def test_returns_the_result_through_a_dedicated_pool(self):
        import concurrent.futures

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.addCleanup(pool.shutdown)
        self.assertEqual(await self.ns["_wait_handle"](self._spawn(Ret("ok")), pool), "ok")

    async def test_errors_from_the_job_propagate(self):
        with self.assertRaisesRegex(RuntimeError, "job exploded"):
            await self.ns["_wait_handle"](self._spawn(Fail("job exploded")))
        with self.assertRaises(ValueError):
            await self.ns["_wait_handle"](self._spawn(Raise(ValueError("not json"))))

    async def test_the_callers_context_reaches_the_waiting_thread(self):
        # `await handle` loses context variables (run_in_executor does not copy them); the kernel's
        # run id lives in one, so the wait must go through a copied context.
        run_id = contextvars.ContextVar("run_id", default=None)
        run_id.set("run-1")
        seen = []

        class Probe:
            id = "probe"

            def wait(self, timeout=None):
                seen.append(run_id.get())
                return "done"

            def cancel(self):
                return False

        self.assertEqual(await self.ns["_wait_handle"](Probe()), "done")
        import concurrent.futures

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.addCleanup(pool.shutdown)
        self.assertEqual(await self.ns["_wait_handle"](Probe(), pool), "done")
        self.assertEqual(seen, ["run-1", "run-1"])

    async def test_cancelling_the_wait_cancels_the_job(self):
        h = self._spawn(Hang())
        task = asyncio.ensure_future(self.ns["_wait_handle"](h))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.host.cancelled, [h.id])

    async def test_a_failing_cancel_is_ignored(self):
        started = threading.Event()
        release = threading.Event()

        class Stubborn:
            id = "stubborn"

            def wait(self, timeout=None):
                started.set()
                release.wait(5)
                raise RuntimeError("Cancelled by user")

            def cancel(self):
                release.set()
                raise ConnectionError("bridge down")

        task = asyncio.ensure_future(self.ns["_wait_handle"](Stubborn()))
        await asyncio.sleep(0.05)
        self.assertTrue(started.is_set())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_an_aborted_bridge_call_becomes_a_cancellation(self):
        for message in ("Operation aborted", "eval cell was interrupted"):
            with self.assertRaises(asyncio.CancelledError):
                await self.ns["_wait_handle"](self._spawn(Fail(message)))

    async def test_an_ordinary_cancelled_job_is_still_a_runtime_error(self):
        with self.assertRaisesRegex(RuntimeError, "Cancelled by user"):
            await self.ns["_wait_handle"](self._spawn(Fail("Cancelled by user")))


if __name__ == "__main__":
    unittest.main()
