---
name: dag
description: Execute a dependency DAG of subagent tasks in parallel with a critic gate on every node - a node runs as soon as its dependencies are approved, dependents of a blocked node are skipped, state resumes from .omp/pipeline/dag/<slug>.json. Use ONLY when explicitly invoked (/skill:dag) with an approved PRD from /skill:ralplan or an explicit task list. Do NOT use for a single sequential task.
---

# Dag

Execute a dependency DAG of subagent tasks: independent nodes run in parallel,
every node's output passes a `critic` gate before its dependents start, and
state persists so a run can be aborted and resumed.

Explicit invocation only, with an approved PRD from `/skill:ralplan` or an
explicit task list the user gives you. Not for a single sequential task.

## Preconditions

1. `eval` (Python) must be in the tool inventory. If absent, stop with
   "dag requires the eval tool (Python)".
2. Check `task.isolation.enabled` by reading `~/.omp/agent/config.yml` and
   `.omp/config.yml` (if present) with `read`. Set `isolated = True` iff it is
   `true` in either. Otherwise say in the pre-run summary: "isolation off —
   concurrent nodes are kept safe only by disjoint `files` ownership".

## Building the DAG

Source, in order of preference:

1. A `User:` arg path to a JSON DAG file — use exactly that file as both
   input and state.
2. `.omp/pipeline/prd.json` with `"approved": true` — copy it to
   `.omp/pipeline/dag/<slug>.json` (slug = kebab-case of `goal`, ≤ 40 chars;
   `bash mkdir -p .omp/pipeline/dag` first) and run from that copy. Do not
   mutate `prd.json` during the run.
3. A free-text task list in `User:` — derive nodes yourself (ids `N-001`…),
   write `.omp/pipeline/dag/<slug>.json`, then show the node table plus the
   dependency edges and ask with `ask` (`Run` / `Edit` / `Cancel`,
   recommended `Run`) before executing. This gate is required because the
   split took judgment.

If `prd.json` exists but `"approved"` is false, stop: "run `/skill:ralplan`
and approve first". Do not improvise a DAG from an unapproved PRD.

## Run

Three separate Python `eval` cells, in order:

Cell 1 — load the runner (substitute the skill directory printed by the
invocation message):

```python
SKILL_DIR = "<skill directory>"
exec(open(f"{SKILL_DIR}/runner.py").read(), globals())
```

Cell 2 — validate; never run with errors:

```python
dag = load_dag("<dag path>")
errs = validate_dag(dag)
display(errs)
```

If `errs` is non-empty: for an ad-hoc DAG, fix the file and re-validate; for
a PRD-sourced DAG, report the errors to the user (they mean the PRD's
`depends_on`/`files` are inconsistent) instead of editing ralplan's output
unilaterally.

Cell 3 — run to completion (`timeout: 0`):

```python
dag = await run_dag(dag, state_path="<dag path>", isolated=<bool>, max_attempts=2)
print(summarize(dag))
```

While cell 3 runs, node transitions surface through `log()`; the user can
watch every worker and critic in Agent Hub (`Alt+A`). Do not spawn additional
agents from outside the cell during the run.

## Cancel and resume

Ctrl-C aborts the cell and cancels in-flight handles. Re-invoking
`/skill:dag <same path>` resumes: nodes already `done` are not re-run.
Top-level `"retry_blocked": true` in the DAG file re-attempts previously
blocked nodes on resume.

Run one dag execution from one session. The runner persists its in-memory
state after every transition; a second concurrent run sharing the same state
file will overwrite the first run's progress.

## Completion report

Print `summarize(dag)`. List `blocked` and `skipped` nodes first with their
`blocked_reason`, and link each node's critic transcript as
`agent://<critic id>`. For PRD-sourced runs, mirror the final per-node
statuses back into `prd.json` `stories[].status` (`done` / `blocked` /
`skipped`). A run with any `blocked` or `skipped` node is incomplete — report
it as such; do not bury a blocked node under a headline "done". If new scope
surfaced during the run, suggest `/skill:ralplan` for it; never fold it into
this run.

Dag is the terminal stage of the pipeline. After the report, stop.
