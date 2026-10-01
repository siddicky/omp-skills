"""Plan files and environment helpers: slugify, stamp, approval, state paths, init/prepare, sync_prd,
atomic writes, and the omp/git probes."""

import datetime
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fake_prelude as fp  # noqa: E402
from fake_prelude import make_dag, node  # noqa: E402

HOST = fp.FakeHost()
NS = fp.make_namespace(HOST)


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def write_json(path, doc):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)


class TempDirCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def path(self, *parts):
        return os.path.join(self.tmp, *parts)


class Slugify(unittest.TestCase):
    slugify = staticmethod(NS["slugify"])

    def test_basics(self):
        self.assertEqual(self.slugify("Hello, World!"), "hello-world")
        self.assertEqual(self.slugify("  --Leading and trailing--  "), "leading-and-trailing")
        self.assertEqual(self.slugify("Version 2.0 (beta)"), "version-2-0-beta")
        self.assertEqual(self.slugify(123), "123")

    def test_ascii_folding(self):
        self.assertEqual(self.slugify("Café Münster"), "cafe-munster")
        self.assertEqual(self.slugify("naïve façade"), "naive-facade")

    def test_empty_results_are_untitled(self):
        for text in ("", "   ", "!!!", "为管理后台添加单点登录", "Добавить тёмную тему"):
            self.assertEqual(self.slugify(text), "untitled", text)

    def test_cut_at_a_word_boundary(self):
        goal = "Implement user authentication with OAuth and SAML for the admin console"
        # the first 40 characters end in "-oauth"; the cut backs off to the last "-" inside them
        self.assertEqual(self.slugify(goal), "implement-user-authentication-with")
        slug = self.slugify("word " * 20)
        self.assertEqual(slug, "-".join(["word"] * 8))
        self.assertLessEqual(len(slug), 40)
        # here the 40th character is a dash itself, so the cut must not leave it dangling
        self.assertEqual(self.slugify("alpha beta gamma delta epsilon zeta eta theta"), "alpha-beta-gamma-delta-epsilon-zeta-eta")

    def test_boundary_only_counts_past_half_the_length(self):
        self.assertEqual(self.slugify("a" * 30 + " " + "b" * 30), "a" * 30)
        short_head = self.slugify("a" * 10 + " " + "b" * 60)
        self.assertEqual(short_head, "a" * 10 + "-" + "b" * 29)  # hard cut: the boundary is too early

    def test_length_limits(self):
        self.assertEqual(self.slugify("a" * 40), "a" * 40)
        self.assertEqual(self.slugify("a" * 41), "a" * 40)
        self.assertEqual(self.slugify("a" * 200), "a" * 40)
        self.assertEqual(self.slugify("hello world foo", max_len=8), "hello")
        self.assertEqual(self.slugify("hello world foo", max_len=11), "hello-world")
        self.assertEqual(self.slugify("hello world foo", max_len=12), "hello-world")  # "hello-world-" loses its dash

    def test_never_ends_with_a_hyphen_and_is_idempotent(self):
        for text in ("a-" * 30, "x" * 39 + " y", "Mixed CASE with -- dashes -- everywhere and more words here"):
            slug = self.slugify(text)
            self.assertRegex(slug, r"^[a-z0-9]+(-[a-z0-9]+)*$")
            self.assertEqual(self.slugify(slug), slug)

    def test_a_tiny_max_len_never_leaves_a_hyphen_at_either_end(self):
        self.assertEqual(self.slugify("a-b", max_len=2), "a")
        for text in ("a-b-c", "ab-cd", "-x-y-z-", "x y z"):
            for max_len in range(1, 8):
                out = self.slugify(text, max_len=max_len)
                self.assertTrue(out, (text, max_len))
                self.assertLessEqual(len(out), max_len, (text, max_len))
                self.assertEqual(out, out.strip("-"), (text, max_len))


class Stamp(unittest.TestCase):
    def test_format_and_clock(self):
        value = NS["stamp"]()
        self.assertRegex(value, r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
        parsed = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
        self.assertLess(abs((datetime.datetime.now(datetime.timezone.utc) - parsed).total_seconds()), 5)


class Approval(TempDirCase):
    def test_is_approved_is_strict(self):
        is_approved = NS["is_approved"]
        self.assertIs(is_approved({"approved": True}), True)
        for bad in ({}, {"approved": False}, {"approved": "true"}, {"approved": 1}, {"approved": None}, None, [], "x"):
            self.assertIs(is_approved(bad), False, bad)

    def test_approve_file_keeps_the_prd_shape(self):
        prd = {"goal": "g", "approved": False, "stories": [node("US-001")], "extra": {"k": [1, 2]}}
        write_json(self.path("prd.json"), prd)
        out = NS["approve_file"](self.path("prd.json"))
        on_disk = read_json(self.path("prd.json"))
        self.assertEqual(out, on_disk)
        self.assertIs(on_disk["approved"], True)
        self.assertRegex(on_disk["approved_at"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
        self.assertIn("stories", on_disk)  # not renamed to nodes
        self.assertNotIn("nodes", on_disk)
        self.assertEqual(on_disk["stories"], prd["stories"])
        self.assertEqual(on_disk["extra"], prd["extra"])
        self.assertEqual(os.listdir(self.tmp), ["prd.json"])  # no temp file left behind

    def test_approve_file_rejects_bad_input(self):
        with open(self.path("bad.json"), "w") as f:
            f.write("{")
        with self.assertRaisesRegex(ValueError, "bad.json"):
            NS["approve_file"](self.path("bad.json"))
        write_json(self.path("list.json"), [1])
        with self.assertRaises(ValueError):
            NS["approve_file"](self.path("list.json"))


class AtomicWrites(TempDirCase):
    def test_persist_creates_parents_and_leaves_no_temp_file(self):
        target = self.path("a", "b", "state.json")
        NS["_persist"]({"x": [1, "é"]}, target)
        self.assertEqual(read_json(target), {"x": [1, "é"]})
        self.assertEqual(os.listdir(os.path.dirname(target)), ["state.json"])

    def test_a_failed_replace_leaves_the_old_file_intact(self):
        target = self.path("state.json")
        write_json(target, {"v": 1})
        with mock.patch("os.replace", side_effect=OSError("disk gone")):
            with self.assertRaises(OSError):
                NS["_persist"]({"v": 2}, target)
        self.assertEqual(read_json(target), {"v": 1})

    def test_non_ascii_text_stays_readable_in_the_file(self):
        target = self.path("state.json")
        NS["_persist"]({"x": "café ✓"}, target)
        self.assertIn("café ✓", read_bytes(target).decode("utf-8"))

    def test_a_lone_surrogate_is_escaped_instead_of_crashing(self):
        target = self.path("state.json")
        NS["_persist"]({"observed": "cut emoji \ud83d here", "ok": "café"}, target)
        self.assertEqual(read_json(target), {"observed": "cut emoji \ud83d here", "ok": "café"})
        self.assertEqual(os.listdir(self.tmp), ["state.json"])

    def test_relative_path_in_cwd(self):
        old = os.getcwd()
        os.chdir(self.tmp)
        self.addCleanup(os.chdir, old)
        NS["_persist"]({"v": 1}, "state.json")
        self.assertEqual(read_json(self.path("state.json")), {"v": 1})

    def test_a_leftover_temp_file_from_a_crashed_write_is_not_appended_to(self):
        target = self.path("state.json")
        with open(target + ".tmp", "w", encoding="utf-8") as f:
            f.write('{"half": "writt')  # what a killed process leaves behind
        NS["_persist"]({"v": 2}, target)
        self.assertEqual(read_json(target), {"v": 2})
        self.assertEqual(os.listdir(self.tmp), ["state.json"])

    def test_the_file_is_indented_and_ends_with_a_newline_because_people_edit_it(self):
        target = self.path("state.json")
        NS["_persist"]({"nodes": [{"id": "A"}]}, target)
        text = read_bytes(target).decode("utf-8")
        self.assertTrue(text.endswith("}\n"))
        self.assertIn('\n  "nodes": [\n', text)


class StatePath(TempDirCase):
    def test_a_file_already_in_the_state_dir_is_its_own_state(self):
        state_dir = self.path("dag")
        src = os.path.join(state_dir, "mine.json")
        self.assertEqual(NS["dag_state_path"](src, {"goal": "g"}, state_dir), src)

    def test_relative_spelling_of_the_same_dir(self):
        old = os.getcwd()
        os.chdir(self.tmp)
        self.addCleanup(os.chdir, old)
        os.makedirs("dag")
        self.assertEqual(NS["dag_state_path"]("./dag/x.json", {}, "dag"), "./dag/x.json")

    def test_a_sibling_directory_sharing_the_name_prefix_is_not_the_state_dir(self):
        # a plain startswith would treat dag2/ and dag-old/ as "inside dag/" and resume or overwrite the wrong file
        state_dir = self.path("dag")
        for sibling in ("dag2", "dag-old", "dagger", "dag.bak"):
            src = os.path.join(self.path(sibling), "x.json")
            got = NS["dag_state_path"](src, {"goal": "Ship it"}, state_dir)
            self.assertEqual(got, os.path.join(state_dir, "ship-it.json"), sibling)

    def test_a_parent_or_a_file_named_like_the_state_dir_is_not_inside_it(self):
        state_dir = self.path("dag")
        for outside in (self.tmp, self.path("dag.json"), os.path.join(self.tmp, "..", os.path.basename(self.tmp) + "x", "dag", "a.json")):
            got = NS["dag_state_path"](outside, {"goal": "Ship it"}, state_dir)
            self.assertEqual(got, os.path.join(state_dir, "ship-it.json"), outside)

    def test_slug_comes_from_the_source_spec_basename(self):
        doc = {"source_spec": ".omp/pipeline/specs/Add SSO Login.md", "goal": "ignored"}
        got = NS["dag_state_path"](self.path("prd.json"), doc, self.path("dag"))
        self.assertEqual(got, self.path("dag", "add-sso-login.json"))

    def test_goal_when_there_is_no_source_spec(self):
        for spec in (None, "", "   "):
            doc = {"source_spec": spec, "goal": "Produce hello.txt now"}
            got = NS["dag_state_path"](self.path("prd.json"), doc, self.path("dag"))
            self.assertEqual(got, self.path("dag", "produce-hello-txt-now.json"), spec)

    def test_file_name_when_there_is_neither(self):
        got = NS["dag_state_path"](self.path("my plan.json"), {}, self.path("dag"))
        self.assertEqual(got, self.path("dag", NS["slugify"]("my plan.json") + ".json"))
        self.assertEqual(NS["dag_state_path"](self.path("p.json"), None, self.path("dag")), self.path("dag", "p-json.json"))

    def test_default_state_dir(self):
        got = NS["dag_state_path"](self.path("prd.json"), {"goal": "Do a thing"})
        self.assertEqual(got, ".omp/pipeline/dag/do-a-thing.json")

    def test_empty_slug_falls_back_to_untitled(self):
        doc = {"source_spec": "specs/为管理.md"}
        self.assertEqual(NS["dag_state_path"](self.path("p.json"), doc, self.path("d")), self.path("d", "untitled.json"))

    def test_a_symlinked_spelling_of_the_state_dir_is_still_inside_it(self):
        real = self.path("real")
        os.makedirs(os.path.join(real, "dag"))
        os.symlink(real, self.path("link"))
        src = os.path.join(self.path("link"), "dag", "mine.json")
        self.assertEqual(NS["dag_state_path"](src, {"goal": "g"}, os.path.join(real, "dag")), src)

    def test_a_source_spec_written_as_a_directory_path_still_names_the_state(self):
        doc = {"source_spec": ".omp/pipeline/specs/add-sso/", "goal": "ignored"}
        got = NS["dag_state_path"](self.path("prd.json"), doc, self.path("dag"))
        self.assertEqual(got, self.path("dag", "add-sso.json"))

    def test_prepare_dag_and_dag_state_path_share_one_default_state_dir(self):
        old = os.getcwd()
        os.chdir(self.tmp)
        self.addCleanup(os.chdir, old)
        state = os.path.join(".omp", "pipeline", "dag", "mine.json")
        os.makedirs(os.path.dirname(state))
        write_json(state, approved_prd())
        state_path, _dag, _resumed = NS["prepare_dag"](state)
        self.assertEqual(state_path, state)  # inside the default state dir: it is the saved run itself


def approved_prd(**extra):
    doc = {
        "goal": "Ship it",
        "source_spec": ".omp/pipeline/specs/ship-it.md",
        "approved": True,
        "approved_at": "2026-09-30T00:00:00Z",
        "isolated": True,
        "retry_blocked": True,
        "stories": [
            node("US-001", status="done", attempts=2, worker_ids=["a"], critic_ids=["b"], verdict={"verdict": "approve"},
                 verdict_overridden=True, evidence="old", result={"x": 1}, screen={"y": 2}, blocked_reason="r", last_error="e"),
            node("US-002", deps=["US-001"], status="blocked", blocked_reason="no", agent="sonic"),
        ],
    }
    doc.update(extra)
    return doc


class InitDag(TempDirCase):
    def test_fresh_copy_strips_run_state(self):
        src, dst = self.path("prd.json"), self.path("dag", "ship-it.json")
        write_json(src, approved_prd())
        before = read_bytes(src)
        dag = NS["init_dag"](src, dst)
        self.assertEqual(read_json(dst), dag)
        self.assertEqual(read_bytes(src), before)  # the source is never touched
        for n in dag["nodes"]:
            self.assertEqual(n["status"], "pending")
            for key in ("attempts", "worker_ids", "critic_ids", "verdict", "verdict_overridden", "evidence",
                        "result", "screen", "blocked_reason", "last_error"):
                self.assertNotIn(key, n)
        self.assertNotIn("isolated", dag)
        self.assertNotIn("retry_blocked", dag)
        self.assertEqual(dag["source"], src)
        self.assertRegex(dag["started"], r"^\d{4}-\d{2}-\d{2}T")
        self.assertNotIn("stories", dag)
        # everything else survives
        self.assertEqual(dag["goal"], "Ship it")
        self.assertEqual(dag["approved_at"], "2026-09-30T00:00:00Z")
        self.assertEqual(dag["source_spec"], ".omp/pipeline/specs/ship-it.md")
        self.assertEqual(dag["nodes"][1]["agent"], "sonic")
        self.assertEqual(dag["nodes"][1]["depends_on"], ["US-001"])
        self.assertEqual(os.listdir(os.path.dirname(dst)), ["ship-it.json"])

    def test_refuses_an_unapproved_source_and_writes_nothing(self):
        for approved in (False, None, "true"):
            src, dst = self.path("prd.json"), self.path("dag", "x.json")
            write_json(src, approved_prd(approved=approved))
            with self.assertRaisesRegex(ValueError, "not approved"):
                NS["init_dag"](src, dst)
            self.assertFalse(os.path.exists(dst))

    def test_missing_approved_key_is_unapproved(self):
        src = self.path("prd.json")
        doc = approved_prd()
        del doc["approved"]
        write_json(src, doc)
        with self.assertRaises(ValueError):
            NS["init_dag"](src, self.path("out.json"))

    def test_overwrites_the_destination(self):
        src, dst = self.path("prd.json"), self.path("out.json")
        write_json(src, approved_prd())
        write_json(dst, {"stale": True})
        NS["init_dag"](src, dst)
        self.assertNotIn("stale", read_json(dst))

    def test_restart_in_place(self):
        path = self.path("dag", "run.json")
        write_json(path, make_dag(node("A", status="done", attempts=3, evidence="x"), approved=True))
        dag = NS["init_dag"](path, path)
        self.assertEqual(dag["nodes"][0]["status"], "pending")
        self.assertNotIn("attempts", read_json(path)["nodes"][0])

    def test_nodes_key_source(self):
        src = self.path("plan.json")
        write_json(src, make_dag(node("A"), node("B", deps=["A"])))
        self.assertEqual([n["id"] for n in NS["init_dag"](src, self.path("o.json"))["nodes"]], ["A", "B"])

    def test_nodes_that_are_not_a_list_are_refused_and_nothing_is_written(self):
        src, dst = self.path("plan.json"), self.path("o.json")
        write_json(src, {"goal": "g", "approved": True, "nodes": "x"})
        with self.assertRaisesRegex(ValueError, "nodes must be a list"):
            NS["init_dag"](src, dst)
        self.assertFalse(os.path.exists(dst))

    def test_entries_that_are_not_nodes_are_copied_as_they_are(self):
        src = self.path("plan.json")
        write_json(src, {"goal": "g", "approved": True, "nodes": [5, "x", node("A", status="done", attempts=2)]})
        dag = NS["init_dag"](src, self.path("o.json"))
        self.assertEqual(dag["nodes"][:2], [5, "x"])
        self.assertEqual((dag["nodes"][2]["status"], "attempts" in dag["nodes"][2]), ("pending", False))


class PrepareDag(TempDirCase):
    def setUp(self):
        super().setUp()
        self.src = self.path("prd.json")
        self.state_dir = self.path("dag")
        write_json(self.src, approved_prd())
        self.state = os.path.join(self.state_dir, "ship-it.json")

    def prepare(self, src=None):
        return NS["prepare_dag"](src or self.src, state_dir=self.state_dir)

    def test_first_call_creates_the_state(self):
        state_path, dag, resumed = self.prepare()
        self.assertEqual(state_path, self.state)
        self.assertIs(resumed, False)
        self.assertEqual(read_json(self.state), dag)
        self.assertEqual([n["status"] for n in dag["nodes"]], ["pending", "pending"])

    def test_existing_state_is_resumed_and_never_overwritten(self):
        self.prepare()
        progressed = read_json(self.state)
        progressed["nodes"][0].update(status="done", attempts=1, evidence="proof")
        write_json(self.state, progressed)
        before = read_bytes(self.state)
        state_path, dag, resumed = self.prepare()
        self.assertIs(resumed, True)
        self.assertEqual(state_path, self.state)
        self.assertEqual(dag["nodes"][0]["evidence"], "proof")
        self.assertEqual(read_bytes(self.state), before)

    def replan(self, **changes):
        """What ralplan does: rewrite the PRD (same source_spec, so the same state file) and approve it."""
        prd = approved_prd()
        prd["stories"][0].update(changes)
        write_json(self.src, prd)

    def test_a_changed_source_is_reported_never_silently_resumed_or_overwritten(self):
        self.prepare()
        marked = read_json(self.state)
        marked["marker"] = "keep me"
        write_json(self.state, marked)
        write_json(self.src, approved_prd(goal="Changed goal"))
        before = read_bytes(self.state)
        with self.assertRaises(NS["StaleState"]) as caught:
            self.prepare()
        self.assertEqual(read_bytes(self.state), before)  # the saved run is left alone
        self.assertEqual((caught.exception.state_path, caught.exception.source), (self.state, self.src))
        self.assertIn("goal changed", str(caught.exception))
        self.assertIn(self.state, str(caught.exception))
        self.assertIn("left untouched", str(caught.exception))

    def test_stale_state_is_a_value_error(self):
        # the dag skill's generic "report a ValueError and stop" handling stays safe for it
        self.assertTrue(issubclass(NS["StaleState"], ValueError))

    def test_a_replanned_prd_is_stale_even_when_every_node_is_pending(self):
        # audit adversarial-review B2: pending everywhere is the normal state after an early abort
        self.prepare()
        self.replan(task="NEW TASK: edit a.txt")
        with self.assertRaisesRegex(NS["StaleState"], r"changed: US-001"):
            self.prepare()
        self.assertEqual(read_json(self.state)["nodes"][0]["task"], "Do US-001")

    def test_every_plan_field_makes_the_state_stale(self):
        changes = {
            "id": "US-009",
            "title": "another title",
            "task": "another task",
            "acceptance_criteria": ["`false` exits 1"],
            "depends_on": ["US-002"],
            "files": ["elsewhere.txt"],
            "agent": "sonic",
        }
        for field, value in changes.items():
            self.prepare()
            self.replan(**{field: value})
            with self.assertRaises(NS["StaleState"], msg=field):
                self.prepare()
            os.remove(self.state)

    def test_added_and_removed_stories_are_named_in_the_message(self):
        self.prepare()
        prd = approved_prd()
        prd["stories"][1]["id"] = "US-777"
        prd["stories"].append(node("US-888"))
        write_json(self.src, prd)
        with self.assertRaises(NS["StaleState"]) as caught:
            self.prepare()
        message = str(caught.exception)
        self.assertIn("added: US-777, US-888", message)
        self.assertIn("removed: US-002", message)

    def test_a_long_list_of_changes_is_shortened(self):
        self.prepare()
        prd = approved_prd()
        prd["stories"] = [node(f"NEW-{i}") for i in range(8)]
        write_json(self.src, prd)
        with self.assertRaisesRegex(NS["StaleState"], r"added: NEW-0, NEW-1, NEW-2, NEW-3, NEW-4 and 3 more"):
            self.prepare()

    def test_an_unapproved_source_is_refused_even_when_a_saved_run_exists(self):
        self.prepare()
        write_json(self.src, approved_prd(approved=False, goal="Changed goal"))
        with self.assertRaisesRegex(ValueError, "not approved") as caught:
            self.prepare()
        self.assertNotIsInstance(caught.exception, NS["StaleState"])  # approval is checked first
        self.assertEqual(read_json(self.state)["goal"], "Ship it")

    def test_mirrored_statuses_and_a_new_approval_stamp_do_not_make_the_state_stale(self):
        # sync_prd rewrites story statuses and approve_file adds a timestamp; neither changes the plan
        self.prepare()
        progressed = read_json(self.state)
        progressed["nodes"][0].update(status="done", attempts=1, evidence="proof")
        write_json(self.state, progressed)
        prd = read_json(self.src)
        for story in prd["stories"]:
            story["status"] = "blocked"
        prd["approved_at"] = "2026-10-01T00:00:00Z"
        prd["open_findings"] = ["something else"]
        prd["typesafe"] = True
        write_json(self.src, prd)
        state_path, dag, resumed = self.prepare()
        self.assertEqual((state_path, resumed), (self.state, True))
        self.assertEqual(dag["nodes"][0]["evidence"], "proof")

    def test_resuming_by_the_state_path_never_compares_with_a_source(self):
        state_path, _, _ = self.prepare()
        write_json(self.src, approved_prd(goal="Changed goal"))
        again_path, again, _ = self.prepare(src=state_path)
        self.assertEqual((again_path, again["goal"]), (state_path, "Ship it"))  # the explicit way to finish the old run

    def test_restart_after_a_stale_report_copies_the_new_plan(self):
        self.prepare()
        self.replan(task="NEW TASK")
        with self.assertRaises(NS["StaleState"]) as caught:
            self.prepare()
        fresh = NS["init_dag"](caught.exception.source, caught.exception.state_path)
        self.assertEqual(fresh["nodes"][0]["task"], "NEW TASK")
        _, dag, resumed = self.prepare()  # consistent again: no longer stale
        self.assertEqual((dag["nodes"][0]["task"], resumed), ("NEW TASK", False))

    def test_a_source_in_a_prefix_sibling_of_the_state_dir_is_copied_not_taken_as_state(self):
        # audit tests-prelude:V1-T2: <state_dir>2/ is not inside <state_dir>/
        src = os.path.join(self.path("dag2"), "prd.json")
        write_json(src, approved_prd())
        before = read_bytes(src)
        state_path, dag, resumed = self.prepare(src=src)
        self.assertEqual(state_path, self.state)
        self.assertEqual(read_bytes(src), before)  # the source stays a source
        self.assertEqual((os.listdir(self.state_dir), resumed), (["ship-it.json"], False))

    def test_resumed_means_any_node_not_pending(self):
        for status, expected in (("running", True), ("review", True), ("blocked", True), ("skipped", True), ("pending", False)):
            self.prepare()
            state = read_json(self.state)
            state["nodes"][1]["status"] = status
            write_json(self.state, state)
            self.assertIs(self.prepare()[2], expected, status)
            os.remove(self.state)

    def test_resumed_also_means_any_run_history(self):
        # audit real-omp-e2e:V1-03: an Esc or a spawn failure returns every node to pending but keeps its history
        for key, value in (
            ("attempts", 1), ("worker_ids", ["a"]), ("critic_ids", ["c"]), ("verdict", {"verdict": "revise"}),
            ("verdict_overridden", True), ("evidence", "e"), ("result", {}), ("screen", {}),
            ("blocked_reason", "r"), ("last_error", "plan mode"),
        ):
            self.prepare()
            state = read_json(self.state)
            state["nodes"][0][key] = value
            write_json(self.state, state)
            self.assertEqual(state["nodes"][0]["status"], "pending")
            self.assertIs(self.prepare()[2], True, key)
            os.remove(self.state)

    def test_a_fresh_copy_has_no_history(self):
        for _ in range(2):
            self.assertIs(self.prepare()[2], False)  # the second call resumes the untouched copy

    def test_source_inside_the_state_dir_is_the_state(self):
        state_path, dag, resumed = self.prepare()
        again_path, again, again_resumed = self.prepare(src=state_path)
        self.assertEqual(again_path, state_path)
        self.assertEqual(again, dag)
        self.assertIs(again_resumed, False)
        self.assertEqual(os.listdir(self.state_dir), ["ship-it.json"])

    def test_unapproved_source_without_state_is_refused(self):
        write_json(self.src, approved_prd(approved=False))
        with self.assertRaises(ValueError):
            self.prepare()
        self.assertFalse(os.path.exists(self.state_dir))

    def test_missing_source(self):
        with self.assertRaises(OSError):
            self.prepare(src=self.path("nope.json"))


def prd_for(dag, **extra):
    """The PRD a run was copied from: the dag's nodes as stories, every one still pending."""
    return {
        "goal": dag["goal"],
        "approved": True,
        "stories": [{**n, "status": "pending"} if isinstance(n, dict) else n for n in dag["nodes"]],
        **extra,
    }


class SyncPrd(TempDirCase):
    def setUp(self):
        super().setUp()
        self.prd = self.path("prd.json")
        HOST.logs.clear()

    def test_mirrors_terminal_statuses_only(self):
        dag = make_dag(
            node("A", status="done"), node("B", status="blocked"), node("C", status="skipped"),
            node("D", status="running"), node("E", status="pending"),
        )
        prd = prd_for(dag, open_findings=[1])
        write_json(self.prd, prd)
        self.assertEqual(NS["sync_prd"](dag, self.prd), 3)
        out = read_json(self.prd)
        self.assertEqual([s["status"] for s in out["stories"]], ["done", "blocked", "skipped", "pending", "pending"])
        self.assertEqual(out["stories"][0]["task"], "Do A")
        self.assertEqual({k: v for k, v in out.items() if k != "stories"}, {k: v for k, v in prd.items() if k != "stories"})
        self.assertEqual(os.listdir(self.tmp), ["prd.json"])
        self.assertEqual(HOST.logs, [])

    def test_second_call_changes_nothing(self):
        dag = make_dag(node("A", status="done"))
        write_json(self.prd, prd_for(dag))
        self.assertEqual(NS["sync_prd"](dag, self.prd), 1)
        before = read_bytes(self.prd)
        self.assertEqual(NS["sync_prd"](dag, self.prd), 0)
        self.assertEqual(read_bytes(self.prd), before)
        self.assertEqual(HOST.logs, [])  # "nothing left to change" is not "refused"

    def test_story_without_status_gets_one(self):
        dag = make_dag(node("A", status="blocked"))
        prd = prd_for(dag)
        del prd["stories"][0]["status"]
        write_json(self.prd, prd)
        self.assertEqual(NS["sync_prd"](dag, self.prd), 1)
        self.assertEqual(read_json(self.prd)["stories"][0]["status"], "blocked")

    def test_odd_entries_are_ignored(self):
        # a story list that is not clean still matches the run copied from it: init_dag keeps the junk too
        dag = make_dag(node("A", status="done"), "junk", {"id": 5}, {"id": ["A"]}, node("B", status="done"))
        write_json(self.prd, prd_for(dag))
        junk = read_json(self.prd)["stories"][1:4]
        self.assertEqual(NS["sync_prd"](dag, self.prd), 2)
        self.assertEqual(read_json(self.prd)["stories"][1:4], junk)

    def test_requires_a_stories_list(self):
        write_json(self.prd, {"nodes": []})
        with self.assertRaisesRegex(ValueError, "stories"):
            NS["sync_prd"](make_dag(node("A", status="done")), self.prd)

    # audit adversarial-review R2-03: a re-planned PRD is not the plan that ran

    def test_a_replanned_prd_is_left_alone(self):
        # StaleState -> Cancel -> /skill:dag <state path>: the finished old run must not mark the new stories
        old = make_dag(node("US-001", status="done"), node("US-002", ["US-001"], status="done"))
        prd = prd_for(old)
        prd["stories"][0]["task"] = "Something entirely different"
        write_json(self.prd, prd)
        before = read_bytes(self.prd)
        self.assertEqual(NS["sync_prd"](old, self.prd), 0)
        self.assertEqual(read_bytes(self.prd), before)
        self.assertEqual([s["status"] for s in read_json(self.prd)["stories"]], ["pending", "pending"])
        self.assertEqual(len(HOST.logs), 1)
        self.assertIn(self.prd, HOST.logs[0])
        self.assertIn("no longer holds the plan this run was started from", HOST.logs[0])
        self.assertIn("changed: US-001", HOST.logs[0])
        self.assertIn("not updated", HOST.logs[0])

    def test_any_change_to_what_runs_is_a_replan(self):
        def retitle(prd):
            prd["stories"][0]["title"] = "Another title"

        def reword_criteria(prd):
            prd["stories"][0]["acceptance_criteria"] = ["`other` exits 0"]

        def new_dependency(prd):
            prd["stories"][0]["depends_on"] = ["US-002"]

        def new_files(prd):
            prd["stories"][0]["files"] = ["elsewhere.txt"]

        def new_agent(prd):
            prd["stories"][0]["agent"] = "designer"

        def new_goal(prd):
            prd["goal"] = "A different goal"

        def added(prd):
            prd["stories"].append(node("US-003"))

        def removed(prd):
            prd["stories"].pop()

        def reordered(prd):
            prd["stories"].reverse()

        for change in (retitle, reword_criteria, new_dependency, new_files, new_agent, new_goal, added, removed, reordered):
            old = make_dag(node("US-001", status="done"), node("US-002", status="done"))
            prd = prd_for(old)
            change(prd)
            write_json(self.prd, prd)
            before = read_bytes(self.prd)
            self.assertEqual(NS["sync_prd"](old, self.prd), 0, change.__name__)
            self.assertEqual(read_bytes(self.prd), before, change.__name__)

    def test_a_partly_changed_prd_gets_nothing(self):
        # US-002 is unchanged, but it ran against the old US-001: all or nothing
        old = make_dag(node("US-001", status="blocked"), node("US-002", ["US-001"], status="skipped"))
        prd = prd_for(old)
        prd["stories"][0]["task"] = "Rewritten"
        write_json(self.prd, prd)
        self.assertEqual(NS["sync_prd"](old, self.prd), 0)
        self.assertEqual([s["status"] for s in read_json(self.prd)["stories"]], ["pending", "pending"])

    def test_only_the_plan_decides_not_statuses_stamps_or_other_keys(self):
        # sync_prd rewrites story statuses, approve_file adds a stamp, and a run adds history to its nodes
        dag = make_dag(
            node("A", status="done", attempts=2, evidence="proof", result={"summary": "ok"}, verdict={"verdict": "approve"}),
            node("B", status="blocked", blocked_reason="why", last_error="boom"),
            typesafe=True, started="2026-10-01T00:00:00Z", source="prd.json",
        )
        prd = prd_for(dag, approved_at="2026-10-01T00:00:00Z", open_findings=["x"], source_spec=None)
        prd["stories"][0]["priority"] = 1  # a key the dag never copies
        prd["stories"][1]["status"] = "done"
        write_json(self.prd, prd)
        self.assertEqual(NS["sync_prd"](dag, self.prd), 2)
        self.assertEqual([s["status"] for s in read_json(self.prd)["stories"]], ["done", "blocked"])
        self.assertEqual(HOST.logs, [])

    def test_a_missing_goal_on_both_sides_is_the_same_plan(self):
        dag = make_dag(node("A", status="done"))
        del dag["goal"]
        prd = prd_for(make_dag(node("A")))
        del prd["goal"]
        write_json(self.prd, prd)
        self.assertEqual(NS["sync_prd"](dag, self.prd), 1)

    def test_a_prd_with_nothing_to_change_is_not_rewritten(self):
        dag = make_dag(node("A", status="pending"))
        with open(self.prd, "w", encoding="utf-8") as f:
            json.dump(prd_for(dag), f)  # compact and without a trailing newline: not the format sync_prd writes
        before = read_bytes(self.prd)
        self.assertEqual(NS["sync_prd"](dag, self.prd), 0)
        self.assertEqual(read_bytes(self.prd), before)

    def test_only_the_status_of_a_story_changes(self):
        dag = make_dag(node("A", status="blocked"), node("B", status="done"))
        prd = prd_for(dag)
        prd["stories"][0].update(blocked_reason="kept", notes=["n"], priority=3)
        prd["stories"][1].update(last_error="kept too", attempts=2)
        write_json(self.prd, prd)
        self.assertEqual(NS["sync_prd"](dag, self.prd), 2)
        out = read_json(self.prd)
        for before, after in zip(prd["stories"], out["stories"]):
            self.assertEqual({k: v for k, v in after.items() if k != "status"}, {k: v for k, v in before.items() if k != "status"})


class OmpProbes(TempDirCase):
    """detect_isolation / detect_max_concurrency against stub `omp` and `git` executables."""

    def setUp(self):
        super().setUp()
        self.bin = self.path("bin")
        os.makedirs(self.bin)
        self.stubs = self.path("stubs")
        os.makedirs(self.stubs)
        self.script("omp", '#!/bin/sh\nf="$OMP_STUBS/omp-$3"\nif [ -f "$f" ]; then cat "$f"; exit 0; fi\necho "Unknown setting: $3"\nexit 1\n')
        self.script("git", '#!/bin/sh\nprintf "%s\\n" "$GIT_STUB_OUT"\nexit "${GIT_STUB_EXIT:-0}"\n')
        patcher = mock.patch.dict(os.environ, {"PATH": self.bin + os.pathsep + "/usr/bin:/bin", "OMP_STUBS": self.stubs, "GIT_STUB_OUT": "true"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def script(self, name, body):
        path = os.path.join(self.bin, name)
        with open(path, "w") as f:
            f.write(body)
        os.chmod(path, 0o755)

    def setting(self, key, value, **extra):
        with open(os.path.join(self.stubs, f"omp-{key}"), "w") as f:
            json.dump({"key": key, "value": value, "type": "x", "description": "d", **extra}, f)

    def test_isolation_needs_the_setting_and_a_git_work_tree(self):
        self.setting("task.isolation.enabled", True)
        ok, why = NS["detect_isolation"]()
        self.assertIs(ok, True)
        self.assertIn("git work tree", why)

    def test_isolation_off(self):
        self.setting("task.isolation.enabled", False)
        ok, why = NS["detect_isolation"]()
        self.assertEqual((ok, why), (False, "task.isolation.enabled is off"))

    def test_isolation_on_but_not_a_git_work_tree(self):
        self.setting("task.isolation.enabled", True)
        for out, code in (("false", "0"), ("", "128"), ("fatal: not a git repository", "128")):
            with mock.patch.dict(os.environ, {"GIT_STUB_OUT": out, "GIT_STUB_EXIT": code}):
                ok, why = NS["detect_isolation"]()
            self.assertIs(ok, False, (out, code))
            self.assertIn("not inside a git work tree", why)

    def test_isolation_value_must_be_a_real_true(self):
        for value in ("true", 1, None, "yes"):
            self.setting("task.isolation.enabled", value)
            self.assertIs(NS["detect_isolation"]()[0], False, value)

    def test_isolation_never_raises(self):
        # omp missing
        with mock.patch.dict(os.environ, {"PATH": self.path("empty")}):
            ok, why = NS["detect_isolation"]()
        self.assertIs(ok, False)
        self.assertIn("omp is not on PATH", why)
        # unknown setting (the stub has no file for it), then garbage output
        self.assertIs(NS["detect_isolation"]()[0], False)
        stub = os.path.join(self.stubs, "omp-task.isolation.enabled")
        for garbage in ("not json at all", "[1, 2]", '{"no": "value"}'):
            with open(stub, "w") as f:
                f.write(garbage)
            self.assertIs(NS["detect_isolation"]()[0], False, garbage)

    def test_isolation_when_git_itself_cannot_run(self):
        # omp answers and git is missing or hangs: still (False, reason), never an exception
        self.setting("task.isolation.enabled", True)
        real = subprocess.run

        def run(cmd, *args, **kwargs):
            if cmd[0] == "git":
                raise FileNotFoundError("git")
            return real(cmd, *args, **kwargs)

        with mock.patch("subprocess.run", side_effect=run):
            self.assertEqual(NS["detect_isolation"](), (False, "FileNotFoundError: git"))

    def test_isolation_survives_subprocess_errors(self):
        with mock.patch("subprocess.run", side_effect=OSError("boom")):
            ok, why = NS["detect_isolation"]()
        self.assertIs(ok, False)
        self.assertIsInstance(why, str)

    def test_max_concurrency_reads_and_caps(self):
        # 32 is omp's own default, which `omp config get` reports when the key is unset (audit adversarial-review
        # R2-04): an unset key therefore means 16, and 4 only when omp cannot be asked
        for value, expected in ((12, 12), (1, 1), (16, 16), (17, 16), (32, 16), (100, 16), (8.0, 8), ("6", 6)):
            self.setting("task.maxConcurrency", value)
            self.assertEqual(NS["detect_max_concurrency"](), expected, value)

    def test_max_concurrency_falls_back_to_the_default(self):
        for value in (0, -3, 2.5, "abc", "", True, None, [4], {"n": 4}):
            self.setting("task.maxConcurrency", value)
            self.assertEqual(NS["detect_max_concurrency"](), 4, repr(value))
            self.assertEqual(NS["detect_max_concurrency"](default=7), 7, repr(value))

    def test_max_concurrency_without_omp_or_setting(self):
        self.assertEqual(NS["detect_max_concurrency"](), 4)  # stub says "Unknown setting"
        with mock.patch.dict(os.environ, {"PATH": self.path("empty")}):
            self.assertEqual(NS["detect_max_concurrency"](default=5), 5)
        with mock.patch("subprocess.run", side_effect=RuntimeError("boom")):
            self.assertEqual(NS["detect_max_concurrency"](), 4)

    def test_omp_is_asked_for_json(self):
        self.script("omp", '#!/bin/sh\necho "$@" > "$OMP_STUBS/argv"\necho \'{"value": 3}\'\n')
        self.assertEqual(NS["detect_max_concurrency"](), 3)
        with open(os.path.join(self.stubs, "argv")) as f:
            self.assertEqual(f.read().strip(), "config get task.maxConcurrency --json")


if __name__ == "__main__":
    unittest.main()
