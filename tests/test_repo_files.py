"""Repo files that must stay consistent with the runner: the critic's output schema, the example
fixture, and .gitignore (checked with real `git check-ignore`)."""

import contextlib
import io
import json
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import unicodedata
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fake_prelude as fp  # noqa: E402

NS = fp.make_namespace(fp.FakeHost())


def _scalar(text):
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "\"'":
        return text[1:-1]
    if text in ("true", "false"):
        return text == "true"
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    return text


def _value(text):
    text = text.strip()
    if text.startswith("["):
        if not text.endswith("]"):
            raise ValueError(f"unsupported flow sequence: {text}")
        inner = text[1:-1].strip()
        return [_scalar(p) for p in inner.split(",")] if inner else []
    if text.startswith("{") or text[:1] in "|>&*!":
        raise ValueError(f"unsupported YAML value: {text}")
    return _scalar(text)


def parse_yaml(block):
    """The small YAML subset used in agent frontmatter: nested mappings, flow sequences, scalars,
    and "- item" sequences. Anything else raises, so a format change cannot pass silently."""
    lines = [l.rstrip() for l in block.splitlines() if l.strip() and not l.lstrip().startswith("#")]
    pos = 0

    def indent_of(line):
        return len(line) - len(line.lstrip(" "))

    def parse_block(indent):
        nonlocal pos
        if lines[pos].lstrip().startswith("- "):
            items = []
            while pos < len(lines) and indent_of(lines[pos]) == indent and lines[pos].lstrip().startswith("- "):
                items.append(_value(lines[pos].lstrip()[2:]))
                pos += 1
            return items
        out = {}
        while pos < len(lines) and indent_of(lines[pos]) >= indent:
            line = lines[pos]
            if indent_of(line) != indent:
                raise ValueError(f"unexpected indent: {line!r}")
            key, sep, rest = line.strip().partition(":")
            if not sep:
                raise ValueError(f"not a mapping line: {line!r}")
            pos += 1
            if rest.strip():
                out[key] = _value(rest)
            elif pos < len(lines) and indent_of(lines[pos]) > indent:
                out[key] = parse_block(indent_of(lines[pos]))
            else:
                out[key] = None
        return out

    return parse_block(indent_of(lines[0]))


def frontmatter(path):
    with open(path, encoding="utf-8") as f:
        text = f.read()
    m = re.match(r"---\n(.*?)\n---\n", text, re.S)
    if not m:
        raise ValueError(f"{path}: no frontmatter")
    return parse_yaml(m.group(1))


class MiniYaml(unittest.TestCase):
    def test_the_subset_parser(self):
        doc = parse_yaml("a: 1\nb:\n  c: [x, y]\n  d:\n    - p\n    - q\n  e: \"quoted\"\nf: true\ng: text: with colon")
        self.assertEqual(doc, {"a": 1, "b": {"c": ["x", "y"], "d": ["p", "q"], "e": "quoted"}, "f": True, "g": "text: with colon"})

    def test_unsupported_syntax_raises(self):
        for bad in ("a: |\n  text", "a: {x: 1}", "a: &anchor 1"):
            with self.assertRaises(ValueError):
                parse_yaml(bad)


# Globals defined by the omp 18.4.5 eval prelude (extracted from the binary). runner.py and
# judgments.py are exec'd into that namespace, so they must not redefine any of them.
PRELUDE_NAMES = (
    "AgentHandle", "CompletionHandle", "JudgmentBatch", "JudgmentItem", "WorkPool",
    "agent", "budget", "completion", "display", "env", "judge", "judge_batch", "log", "output",
    "phase", "read", "tool", "wait", "workpool", "write",
    "_BRIDGE_OPENER", "_Budget", "_EvalTool", "_HANDLE_UNSET", "_Handle", "_OMP_CALL_IDENTITY",
    "_OMP_CALL_OCCURRENCES", "_OMP_INTERNAL_URL_RE", "_PRESENTABLE_REPRS", "_TOOL_NAME_RE",
    "_ToolCallable", "_ToolProxy", "__omp_prelude_loaded__", "__omp_reset_call_occurrences__",
    "__omp_tools__", "__omp_with_call_site__", "_annotation_schema", "_apply_query",
    "_attach_judge_batch", "_bridge_call", "_check_questions", "_emit_status", "_handle_value",
    "_judge_batch_from", "_omp_display", "_omp_prelude", "_omp_url_roots", "_read_line_selector",
    "_read_tool_text", "_resolve_omp_path", "_should_delegate_read", "_surface_bridged_tool_images",
    "_tool_proxy_from_env", "_tool_schema",
)  # fmt: skip


class KernelNamespace(unittest.TestCase):
    def exec_into(self, ns):
        for name in ("runner.py", "judgments.py"):
            exec(fp._compiled(name), ns)

    def test_exec_leaves_every_prelude_name_alone(self):
        sentinels = {name: object() for name in PRELUDE_NAMES}
        ns = {"__name__": "kernel", **sentinels}
        self.exec_into(ns)
        for name, sentinel in sentinels.items():
            self.assertIs(ns[name], sentinel, f"{name} was redefined")

    def test_the_public_api_is_defined(self):
        ns = {"__name__": "kernel"}
        self.exec_into(ns)
        for name in (
            "STATUSES", "WORKER_SCHEMA", "CRITIC_SCHEMA", "load_dag", "validate_dag", "paths_overlap",
            "detect_isolation", "detect_max_concurrency", "slugify", "stamp", "is_approved", "approve_file",
            "dag_state_path", "init_dag", "prepare_dag", "StaleState", "run_dag", "summarize", "sync_prd",
            "TYPESAFE_ENV", "CRITERIA_QUESTIONS", "SCREEN_QUESTIONS", "LINT_WEAK_BELOW", "SCREEN_CONTRADICT_CONF",
            "typesafe_mode", "typesafe_available", "redact", "lint_criteria", "screen_evidence", "screen_verdict",
        ):
            self.assertIn(name, ns)
        self.assertTrue(asyncio_iscoroutinefunction(ns["run_dag"]))
        self.assertTrue(asyncio_iscoroutinefunction(ns["lint_criteria"]))
        self.assertTrue(asyncio_iscoroutinefunction(ns["screen_evidence"]))

    def test_running_cell_1_again_is_harmless(self):
        host = fp.FakeHost()
        ns = fp.make_namespace(host)
        first, kernel_agent, kernel_log = ns["slugify"], ns["agent"], ns["log"]
        self.exec_into(ns)  # "re-run Cell 1" after a NameError
        self.exec_into(ns)
        self.assertEqual(ns["slugify"]("Hello World"), "hello-world")
        self.assertEqual(ns["validate_dag"](fp.make_dag(fp.node("A"))), [])
        self.assertIsNot(ns["slugify"], first)  # a fresh definition replaced the old one
        self.assertIs(ns["agent"], kernel_agent)  # and the kernel's own helpers are untouched
        self.assertIs(ns["log"], kernel_log)

    def test_no_kernel_state_is_needed_at_import_time(self):
        # exec'ing into a bare namespace (no agent/log/phase) must work: they are only needed when called
        ns = {"__name__": "bare"}
        self.exec_into(ns)
        self.assertEqual(ns["paths_overlap"]("a/b.ts", "a/c.ts"), False)


def asyncio_iscoroutinefunction(fn):
    import inspect

    return inspect.iscoroutinefunction(fn)


class CriticSchema(unittest.TestCase):
    def test_critic_md_output_is_identical_to_critic_schema(self):
        meta = frontmatter(os.path.join(fp.REPO, "agents", "critic.md"))
        self.assertEqual(meta["output"], NS["CRITIC_SCHEMA"])

    def test_schema_has_the_target_field_for_plan_defects(self):
        finding = NS["CRITIC_SCHEMA"]["properties"]["findings"]["items"]
        self.assertEqual(finding["properties"]["target"], {"type": "string", "enum": ["work", "plan"]})
        self.assertNotIn("target", finding["required"])  # optional: the runner defaults to work

    def test_critic_frontmatter_still_loads_for_omp(self):
        meta = frontmatter(os.path.join(fp.REPO, "agents", "critic.md"))
        self.assertEqual(meta["name"], "critic")
        for tool in ("read", "grep", "glob", "bash"):
            self.assertIn(tool, meta["tools"])

    def test_worker_schema_shape(self):
        schema = NS["WORKER_SCHEMA"]
        self.assertEqual(schema["required"], ["status", "files_changed", "evidence"])
        self.assertEqual(schema["properties"]["status"]["enum"], ["done", "failed"])
        evidence = schema["properties"]["evidence"]["items"]
        self.assertEqual(evidence["required"], ["criterion", "command", "observed", "passed"])
        self.assertEqual(evidence["properties"]["criterion"], {"type": "integer"})
        self.assertEqual(evidence["properties"]["passed"], {"type": "boolean"})
        self.assertEqual(schema["properties"]["files_changed"], {"type": "array", "items": {"type": "string"}})
        self.assertEqual(schema["properties"]["notes"], {"type": "string"})

    def test_status_vocabulary(self):
        self.assertEqual(NS["STATUSES"], ("pending", "running", "review", "done", "blocked", "skipped"))

    def test_schemas_are_plain_json(self):
        for name in ("WORKER_SCHEMA", "CRITIC_SCHEMA", "UNPARSEABLE_VERDICT"):
            json.dumps(NS[name])

    def test_worker_results_from_the_schema_example_are_accepted_as_is(self):
        result = fp.worker_ok(files=["a"], n=2)
        self.assertEqual(NS["_coerce_worker"](result), result)


class ExampleFixture(unittest.TestCase):
    def setUp(self):
        self.dag = NS["load_dag"](os.path.join(fp.DAG_DIR, "example.json"))

    def test_valid_and_approved(self):
        self.assertEqual(NS["validate_dag"](self.dag), [])
        self.assertTrue(NS["is_approved"](self.dag))

    def test_the_verify_node_is_read_only(self):
        nodes = {n["id"]: n for n in self.dag["nodes"]}
        self.assertEqual(nodes["N-004"]["files"], [])
        self.assertEqual(nodes["N-004"]["depends_on"], ["N-003"])
        for nid in ("N-001", "N-002", "N-003"):
            self.assertTrue(nodes[nid]["files"])

    def test_the_two_roots_can_run_together(self):
        roots = [n for n in self.dag["nodes"] if not n["depends_on"]]
        self.assertEqual(len(roots), 2)

    def test_criteria_avoid_commands_an_interceptor_blocks(self):
        for n in self.dag["nodes"]:
            for c in n["acceptance_criteria"]:
                for blocked in ("`cat ", "`head ", "`tail ", "`find "):
                    self.assertNotIn(blocked, c, c)


def read_text(*parts):
    with open(os.path.join(fp.REPO, *parts), encoding="utf-8") as f:
        return f.read()


class TypesafeSwitchDocs(unittest.TestCase):
    """audit real-omp-e2e:V1-01: omp's eval kernel runs with an allowlisted environment, so the docs must
    not promise that an exported OMP_SKILLS_TYPESAFE works. They name the switches that do."""

    def test_the_docs_name_the_working_switches_and_the_caveat(self):
        for path in (("README.md",), ("skills", "dag", "SKILL.md")):
            text = " ".join(read_text(*path).split())
            self.assertIn('env("OMP_SKILLS_TYPESAFE", "shadow")', text, path)
            self.assertIn('top-level `"typesafe"` key', text, path)
            self.assertIn("before launching omp does", text, path)  # "... does not work" / "... does nothing"
            self.assertIn("allowlisted environment", text, path)
            self.assertNotIn("Enable with `OMP_SKILLS_TYPESAFE=1`", text, path)
            self.assertNotIn("Enable with the environment variable", text, path)

    def test_the_kernel_side_comment_says_the_same(self):
        text = " ".join(read_text("skills", "dag", "judgments.py").split())
        self.assertIn("allowlisted environment", text)
        self.assertIn('env("OMP_SKILLS_TYPESAFE", "shadow")', text)

    def test_cell_2_prints_the_mode_so_a_silent_off_is_visible(self):
        self.assertIn('print("typesafe:", mode)', read_text("skills", "dag", "SKILL.md"))


class DagSkillDocs(unittest.TestCase):
    def test_stale_state_handling_is_documented_with_the_runner_api(self):
        text = read_text("skills", "dag", "SKILL.md")
        for needle in ("except StaleState as e:", "stale.state_path", "stale.source", "init_dag(stale.source, state_path)"):
            self.assertIn(needle, text)
        self.assertTrue(issubclass(NS["StaleState"], ValueError))

    def test_resumed_is_described_as_history_not_only_status(self):
        text = " ".join(read_text("skills", "dag", "SKILL.md").split())
        self.assertIn("carries any history", text)

    def test_the_free_text_error_path_never_deletes_the_draft_or_skips_the_gate(self):
        # audit adversarial-review m1: after Run the draft is approved, so a fix needs the gate again, and for
        # a free-text DAG the state file IS the draft
        body = read_text("skills", "dag", "SKILL.md")
        section = body.split("If `errs` is non-empty:\n", 1)[1].split("\n\nLint findings", 1)[0]
        free_text, prd_sourced = (" ".join(part.split()) for part in section.split("\n- PRD-sourced", 1))
        self.assertIn('set `"approved": false` in it, and repeat the whole Edit / Run / Cancel gate', free_text)
        self.assertIn("Never delete the draft", free_text)
        self.assertNotIn("os.remove", free_text)
        self.assertIn("os.remove(state_path)", prd_sourced)  # only the copy of a PRD or explicit file may go

    def test_the_interrupt_is_explained_and_attempts_count_interrupted_ones(self):
        text = " ".join(read_text("skills", "dag", "SKILL.md").split())
        self.assertIn("shows a `CancelledError`/`KeyboardInterrupt` traceback; that is expected, the state was saved", text)
        self.assertIn("the attempt count includes interrupted attempts", text)

    def test_failure_semantics_are_documented(self):
        text = " ".join(read_text("skills", "dag", "SKILL.md").split())
        self.assertIn("returns `status: failed`", text)
        self.assertIn("without a critic review", text)
        self.assertIn("never approves a node", text)

    def test_plan_mode_is_a_precondition_the_runner_cannot_check(self):
        # audit real-omp-e2e:V1-02 and V2-01: with isolation off omp accepts the spawns and every worker fails, so
        # the skill must be the guard, and both documents must say what plan mode really does in each setting
        text = " ".join(read_text("skills", "dag", "SKILL.md").split())
        self.assertIn("no worker can create or edit a file", text)
        self.assertIn("The runner cannot detect plan mode", text)
        self.assertIn("With isolation on omp rejects the spawn", text)
        self.assertIn("so a node that has to change one ends `blocked` (dependents `skipped`)", text)
        self.assertNotIn("for example plan mode or a job limit", text)  # plan mode aborts the run only with isolation on
        readme = " ".join(read_text("README.md").split())
        self.assertIn("With isolation on, omp rejects the `agent()` spawn", readme)
        self.assertIn("every node that has to change one ends up `blocked` with its dependents `skipped`", readme)
        self.assertNotIn("rejects the isolation arguments", readme)

    def test_the_readme_gives_the_real_concurrency_default(self):
        # audit adversarial-review R2-04: `omp config get` reports omp's own default of 32 for an unset key
        readme = " ".join(read_text("README.md").split())
        self.assertIn("omp's default is 32, so unset means 16", readme)
        self.assertNotIn("4 when unset", readme)

    def test_every_approval_gate_counts_only_an_exact_answer(self):
        # audit adversarial-review R2-12: approving authorizes unattended shell commands, so typed text is no consent
        for skill, label in (("dag", "Run"), ("ralplan", "Approve"), ("deep-interview", "Approve")):
            text = " ".join(read_text("skills", skill, "SKILL.md").split())
            self.assertIn(f"only when the answer is exactly `{label}` and not `timedOut`", text, skill)
            self.assertIn('typed through "Other"', text, skill)
        dag = " ".join(read_text("skills", "dag", "SKILL.md").split())
        self.assertIn("treat typed text as an Edit request", dag)

    def run_completion_cell(self, replan):
        """Execute the PRD mirror cell of "Completion report" exactly as written in SKILL.md."""
        section = read_text("skills", "dag", "SKILL.md").split("## Completion report", 1)[1]
        cell = section.split("```python\n", 1)[1].split("\n```", 1)[0]
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        prd = os.path.join(tmp, "prd.json")
        doc = {"goal": "g", "approved": True, "stories": [fp.node("US-001"), fp.node("US-002", deps=["US-001"])]}
        with open(prd, "w", encoding="utf-8") as f:
            json.dump(doc, f)
        ns = fp.make_namespace(fp.FakeHost())
        ns.update(os=os, json=json)  # Cell 1 imports these
        _, dag, _ = ns["prepare_dag"](prd, state_dir=os.path.join(tmp, "dag"))
        for n in dag["nodes"]:
            n["status"] = "done"
        if replan:  # ralplan rewrote the PRD while the old run was still going
            doc["stories"][0]["task"] = "Something entirely different"
            with open(prd, "w", encoding="utf-8") as f:
                json.dump(doc, f)
        ns["dag"] = dag
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            exec(cell, ns)
        with open(prd, encoding="utf-8") as f:
            return out.getvalue(), [s.get("status") for s in json.load(f)["stories"]]

    def test_the_completion_cell_mirrors_a_matching_prd(self):
        out, statuses = self.run_completion_cell(replan=False)
        self.assertEqual(out, "prd statuses updated: 2 | not mirrored: none\n")
        self.assertEqual(statuses, ["done", "done"])

    def test_the_completion_cell_says_when_a_replanned_prd_was_left_alone(self):
        # audit adversarial-review R2-03
        out, statuses = self.run_completion_cell(replan=True)
        self.assertEqual(out, "prd statuses updated: 0 | not mirrored: ['US-001', 'US-002']\n")
        self.assertEqual(statuses, ["pending", "pending"])  # the stories of the new plan never ran

    def test_the_docs_explain_the_refusal_and_the_redaction_limits(self):
        text = " ".join(read_text("skills", "dag", "SKILL.md").split())
        self.assertIn("copies statuses only while the PRD still holds the plan this run was started from", text)
        self.assertIn("never copy them by hand", text)
        self.assertIn("Case is ignored, as on a default macOS volume", text)
        self.assertIn("a bare 40-character hex string, passes through", text)
        readme = " ".join(read_text("README.md").split())
        self.assertIn("its statuses are then not copied into the re-planned PRD", readme)
        self.assertIn("Case is ignored, as on a default macOS volume", readme)
        self.assertIn("a bare 40-character hex string, still passes", readme)


class CriticScopeRule(unittest.TestCase):
    """audit adversarial-review m2: agents/critic.md and the review prompt must give one scope rule."""

    def test_critic_md_defers_to_the_two_lists_in_the_prompt(self):
        text = " ".join(read_text("agents", "critic.md").split())
        self.assertNotIn("only for a path that no node owns", text)
        self.assertIn("outside both the files the worker may edit and the paths the prompt lists as owned by other nodes", text)
        self.assertIn("a node that runs after this one", text)

    def test_the_prompt_gives_the_same_rule_and_leaves_descendants_out_of_the_list(self):
        prompt = NS["_critic_prompt"](
            fp.make_dag(fp.node("A", files=["a.txt"]), fp.node("B", deps=["A"], files=["b.txt"]), fp.node("C", files=["c.txt"])),
            fp.node("A", files=["a.txt"]),
            fp.worker_ok(),
        )
        self.assertIn("An edit outside both lists is a blocker.", prompt)
        listed = prompt.split("# Paths owned by other nodes\n")[1].split("# Instructions")[0]
        self.assertIn("- c.txt", listed)  # may run alongside A
        self.assertNotIn("b.txt", listed)  # runs after A, so a change there is A's own

    def test_the_verdict_contract_in_critic_md_matches_the_runner(self):
        text = " ".join(read_text("agents", "critic.md").split())
        self.assertIn("exactly `approve` or `revise`", text)
        self.assertIn("never read as an approval", text)

    def test_bash_is_for_criteria_commands_and_read_only_git_only(self):
        # audit adversarial-review R2-11: a test run writes caches and snapshots into a tree other workers are editing
        text = " ".join(read_text("agents", "critic.md").split())
        self.assertNotIn("to run existing tests", text)
        self.assertIn("only to run the commands the acceptance criteria name and for read-only git inspection", text)
        self.assertIn("run a test only when a criterion names it", text)


def skill_slug(goal):
    """skills/deep-interview/SKILL.md, steps 1-5 of "State", followed literally."""
    text = unicodedata.normalize("NFKD", goal).encode("ascii", "ignore").decode("ascii")  # 1
    text = re.sub(r"[^a-z0-9]+", "-", text.lower())  # 2
    text = text.strip("-")  # 3
    if len(text) > 40:  # 4
        piece = text[:40]
        number = piece.rfind("-") + 1  # the first character is number 1; 0 when there is no "-"
        if number >= 22:
            piece = piece[: number - 1]
        text = piece.rstrip("-")
    return text or "untitled"  # 5


class DeepInterviewSlug(unittest.TestCase):
    """audit adversarial-review m5: the hand-computed slug must equal slugify (spec: same rules)."""

    def test_the_skill_states_the_22nd_character_rule(self):
        text = " ".join(read_text("skills", "deep-interview", "SKILL.md").split())
        self.assertIn("22nd character or later (the first character is number 1)", text)
        self.assertNotIn("after position 20", text)

    def test_the_boundary_cases(self):
        slugify = NS["slugify"]
        for head in range(17, 25):  # the last hyphen lands on characters 18 .. 25
            goal = "a" * head + " " + "b" * 25
            self.assertEqual(skill_slug(goal), slugify(goal), (head, skill_slug(goal)))
        self.assertEqual(skill_slug("a" * 20 + " " + "b" * 25), "a" * 20 + "-" + "b" * 19)  # hyphen is character 21: kept
        self.assertEqual(skill_slug("a" * 21 + " " + "b" * 25), "a" * 21)  # hyphen is character 22: cut

    def test_random_goals(self):
        slugify, rng = NS["slugify"], random.Random(20260930)
        alphabet = ["a", "b", "x", "Z", "9", " ", " ", "-", "_", "é", "!", "ß", "  "]
        for _ in range(3000):
            goal = "".join(rng.choice(alphabet) * rng.randint(1, 6) for _ in range(rng.randint(0, 25)))
            self.assertEqual(skill_slug(goal), slugify(goal), repr(goal))


class GitIgnore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        shutil.copy(os.path.join(fp.REPO, ".gitignore"), os.path.join(self.tmp, ".gitignore"))
        subprocess.run(["git", "init", "-q", self.tmp], check=True, capture_output=True)

    def ignored(self, rel):
        path = os.path.join(self.tmp, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write("x")
        proc = subprocess.run(["git", "check-ignore", "-q", rel], cwd=self.tmp, capture_output=True)
        return proc.returncode == 0

    def test_generated_files_are_ignored(self):
        for rel in (
            "node_modules/pkg/index.js",
            "skills/dag/__pycache__/runner.cpython-314.pyc",
            "tests/__pycache__/x.pyc",
            ".omp/pipeline/dag/state.json.tmp",
            "prd.json.tmp",
            ".omm/notes.md",
            ".omc/state/hud.json",
            ".omc/plans/plan.md",
            ".omc/research/report.md",
        ):
            self.assertTrue(self.ignored(rel), f"{rel} should be ignored")

    def test_project_skills_under_omc_stay_committable(self):
        self.assertFalse(self.ignored(".omc/skills/my-skill/SKILL.md"))

    def test_real_sources_are_not_ignored(self):
        for rel in ("skills/dag/runner.py", "skills/dag/judgments.py", "tests/test_run.py", "tests/fake_prelude.py", "agents/critic.md", "README.md", "skills/dag/example.json"):
            self.assertFalse(self.ignored(rel), f"{rel} must not be ignored")


if __name__ == "__main__":
    unittest.main()
