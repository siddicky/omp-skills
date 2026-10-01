---
name: dag
description: Execute a dependency DAG of subagent tasks in parallel with a critic gate on every node - a node runs as soon as its dependencies are approved, dependents of a blocked node are skipped, state resumes from .omp/pipeline/dag/<slug>.json. Use ONLY when explicitly invoked (/skill:dag) with an approved PRD from /skill:ralplan or an explicit task list. Do NOT use for a single sequential task.
hide: true
---

# Dag

Runs a dependency DAG of subagent tasks: independent nodes in parallel, each
passing a `critic` before its dependents start; state persists so a run can be
aborted and resumed.

Explicit invocation only (`/skill:dag`), with an approved PRD from
`/skill:ralplan`, an approved DAG file, or an explicit task list from the user.
Not for a single sequential task. If this body was loaded without a user message
invoking `/skill:dag`, stop and ask the user to run it.

## Preconditions

1. `eval` (Python) must be in the tool inventory, else stop with "dag requires
   the eval tool (Python)".
2. Not in plan mode. If the system prompt says plan mode is active, stop and
   tell the user: "dag cannot run in plan mode (subagents are read-only). Exit
   plan mode and re-run `/skill:dag <args>`." The runner cannot detect plan
   mode, so this check is yours. With isolation on omp rejects the spawn and the run aborts; with
   isolation off no worker can create or edit a file, so a node that has to
   change one ends `blocked` (dependents `skipped`) and a worker that claims work
   it could not do may be wrongly approved.
3. Isolation comes from `detect_isolation()` in Cell 1, never from reading config
   files yourself: `task.isolation.enabled` on and the working directory in a git
   work tree. It reads the setting with `omp config get`, that is from config.yml
   (global or project); a per-run `omp --config <overlay>` is not visible to it, so
   such a run proceeds without isolation and with the overlap check on (fails safe).

An `ask` answer with `timedOut: true` is never consent, anywhere in this skill:
re-ask once, then stop.

## Source and state

The first match wins: (1) the `User:` arg is an existing `.json` file: the
source; (2) any other non-empty arg: a free-text task list; (3) no arg: the
source is `.omp/pipeline/prd.json`, which must contain `"approved": true`, else
stop: "run `/skill:ralplan` and approve first" (never improvise a DAG from an
unapproved PRD).

Never run a source in place: `prepare_dag` copies it to
`.omp/pipeline/dag/<slug>.json` (the state file). A path already inside that
directory is itself the state file and resumes as is. A saved run resumes only
while the source still holds the plan it was copied from, else `prepare_dag`
raises `StaleState`.

## Node fields

`id`, `title`, `task`, `acceptance_criteria`, `depends_on`, `files`, optional
`agent` (default `task`) and `status` (`pending`, `running`, `review`, `done`,
`blocked`, `skipped`; `skipped` is derived and re-evaluated every run).

- `files` is required: every path or glob the node may edit. `files: []` is a
  read-only node: it must not create, modify, or delete anything.
- With isolation off, `validate_dag` rejects overlapping `files` between nodes
  with no dependency path (with isolation on the check is skipped, but patches
  can still conflict). A name with no wildcard and no dot (`src/auth`) is the path
  and everything below it; a dotted name (`package.json`, `packages/ui.kit`) is a
  file unless another node's pattern spells out something inside it, so write
  `dir/**` (or `dir/`) for a directory. Case is ignored, as on a default macOS
  volume (`README.md` overlaps `readme.md`, `[!a]` takes `A`).

## Run

On a `NameError` for a runner function, re-run Cell 1 (cells share one kernel).

Cell 1 - load the runner and judgments helper (substitute the skill directory
printed by the invocation message):

```python
import json, os
SKILL_DIR = "<skill directory>"
for _name in ("runner.py", "judgments.py"):
    exec(open(f"{SKILL_DIR}/{_name}", encoding="utf-8").read(), globals())
isolated, why = detect_isolation()
print(f"isolated={isolated}: {why}")
```

Say the result in the pre-run summary. `isolated=True`: workers run in git
worktrees and a worker's patch is applied to the checkout before its critic
reviews it, so a revised or blocked node's edits are not rolled back (check
`git status` in the report). `isolated=False`: "isolation off - concurrent nodes
are kept safe only by disjoint `files` ownership", plus the reason.

### Build the DAG

Source 1 or 3 (explicit file or `.omp/pipeline/prd.json`):

```python
stale = None
try:
    state_path, dag, resumed = prepare_dag("<source path>")
except StaleState as e:
    stale = e
    print(e)
else:
    print("state file:", state_path)  # resume later with: /skill:dag <state_path>
    if resumed:
        print(summarize(dag))
```

`prepare_dag` never overwrites an existing state file and refuses a source that is
no longer approved, even when a saved run exists. On `ValueError` or `OSError`
(not approved, missing, unreadable, malformed), stop and report the message
verbatim; for an unapproved explicit file add that a hand-written DAG must be
reviewed and given `"approved": true`.

If `stale` is set, the saved run in `stale.state_path` was copied from an older
plan in `stale.source` (the message says what changed). Never run it under the
new plan. Show the message and ask one `ask` question `Restart` / `Cancel`, no
`recommended`:

- `Restart`: `state_path = stale.state_path; dag = init_dag(stale.source, state_path); resumed = False`
  (discards the saved progress), then print the state file as above.
- `Cancel`: stop. The saved run is untouched; `/skill:dag <stale.state_path>`
  finishes it instead (a state file path resumes as is, without comparing it to
  the source).

`resumed` is true when any node is past `pending` or the saved run carries any
history (an attempt, a verdict, a recorded error), so an early-aborted run with
every node `pending` still gets this question. When true, show the `summarize`
table (it shows the attempts and the last error; the attempt count includes
interrupted attempts) and ask one `ask`
question `Resume` / `Restart` / `Cancel` with `recommended: 0`:

- `Resume`: continue with `dag` as loaded. If any node is `blocked`, ask a second
  question `Retry blocked nodes` / `Keep blocked`; on retry set
  `dag["retry_blocked"] = True` before Cell 3.
- `Restart`: `dag = init_dag(dag.get("source") or state_path, state_path)`.
- `Cancel`: stop.

Source 2 (free text): derive the nodes (ids `N-001`...; each has `task`,
`acceptance_criteria`, `depends_on`, `files`, `agent`, `status: "pending"`). Each
criterion is a command plus an explicit pass condition that works on BSD and GNU
userland and avoids `cat`/`head`/`tail`/`find` (for example
`test "$(wc -c < f.txt | tr -d ' ')" = 11` exits 0). Write the draft with Python,
never `approved: true`:

```python
goal = "<one sentence goal>"
path = f".omp/pipeline/dag/{slugify(goal)}.json"
n = 2
while os.path.exists(path):
    path = f".omp/pipeline/dag/{slugify(goal)}-{n}.json"
    n += 1
draft = {"goal": goal, "created": stamp(), "approved": False, "nodes": [...]}
os.makedirs(os.path.dirname(path), exist_ok=True)
with open(path, "w", encoding="utf-8") as f:
    json.dump(draft, f, indent=2)
print(validate_dag(load_dag(path), isolated=isolated))
```

Fix any validation error before showing the draft. Then show every node with its
dependency edges, `files`, task, and every acceptance criterion verbatim (workers
and the critic run these commands), and ask one `ask` question `Edit` / `Run` /
`Cancel` with no `recommended`; the gate is required because the split took
judgment. Approval authorizes unattended shell commands run with the user's
privileges: call `approve_file` only when the answer is exactly `Run` and not
`timedOut`. Free text typed through "Other", silence, a topic change, or your own
confidence is never approval; treat typed text as an Edit request.

- `Edit`: ask in prose what to change and end the turn; then rewrite the file,
  re-run the validation line, show it all again, and re-ask.
- `Run`: `approve_file(path)`, then `state_path, dag, resumed = prepare_dag(path)`.
- `Cancel`: stop; the draft stays on disk with `"approved": false`.

Cell 2 - validate; never run with errors:

```python
errs = validate_dag(dag, isolated=isolated)
display(errs)
mode = typesafe_mode(dag)
print("typesafe:", mode)  # off unless enabled, see "Optional: TypeSafe checks"
if not errs and mode["lint"]:
    lint = await lint_criteria(dag)
    print("lint:", lint["status"], lint.get("reason") or "")
    for w in lint["weak"]:
        print(f'{w["node_id"]}#{w["index"]}: {w["why"]}: {w["criterion"]}')
```

If `errs` is non-empty:

- Free-text DAG: fix the draft file, set `"approved": false` in it, and repeat
  the whole Edit / Run / Cancel gate (every node, task, and criterion again,
  `approve_file` only on Run) before Cell 3: the user approved the text they saw,
  not your fix. Never delete the draft: for a free-text DAG `state_path` is the
  draft itself.
- PRD-sourced or explicit-file DAG: report the errors (the PRD's
  `depends_on`/`files` are inconsistent) and stop without editing ralplan's
  output. If `resumed` is false, delete the fresh copy first with
  `os.remove(state_path)` so a corrected source is re-copied next time.

Lint findings are advisory: show weak criteria and let the user decide whether to
fix them first; they never block the run.

Cell 3 - run to completion (`timeout: 0`). First tell the user the state path and
that Esc cancels:

```python
dag = await run_dag(dag, state_path=state_path, isolated=isolated, max_attempts=2)
print(summarize(dag))
```

The user can watch workers and critics in Agent Hub (`Alt+A`). Do not spawn other
agents during the run. If eval reports the cell was backgrounded before it
finished, call `wait` until it completes before reporting.

`run_dag` raises `ValueError` for an unapproved or invalid DAG (nothing was
spawned) and `RuntimeError("dag aborted: ...")` when a worker cannot be spawned
(unknown agent, a job limit that does not clear);
the state file is then intact with in-flight nodes `pending`. Show the message,
tell the user to fix the cause, and resume. A critic that cannot be spawned does
not abort the run: it is tried once more, then its node is blocked
(`critic failed: ...`); once the cause is fixed, resume and choose `Retry blocked
nodes`.

A worker that raises or returns `status: failed` fails that attempt: it is
retried (up to `max_attempts`) without a critic review, and its reason reaches
the next attempt as a finding. A critic `blocker` or `major` finding means
`revise`, whatever the verdict word says. A critic answer with neither such a
finding nor a usable `approve` or `revise` (another word, no `verdict`, `approve`
without a findings list, not a JSON object) is retried once, then the node is
blocked (`critic failed: ...`); a malformed answer never approves a node.

## Cancel and resume

Esc aborts the cell: in-flight workers and critics are cancelled and their nodes
return to `pending`. The cell then shows a `CancelledError`/`KeyboardInterrupt`
traceback; that is expected, the state was saved. `/skill:dag <state path>` (the
path printed before the run) resumes: `done` nodes are not re-run, interrupted
nodes start over, `skipped` nodes are re-derived.

A `blocked` node stays blocked until the user chooses `Retry blocked nodes` (or
sets top-level `"retry_blocked": true` in the state file); the flag is one-shot
and a retried node sees its previous critic findings. Run one dag execution from
one session: a second concurrent run on the same state file overwrites the first
run's progress.

## Completion report

Print `summarize(dag)`. List `blocked` and `skipped` nodes first with their
`blocked_reason`, each linked to its last critic's output `agent://<critic id>`
(from `critic_ids`) and transcript `history://<critic id>`. `plan defect:` means
the task or a criterion is wrong: send the user to `/skill:ralplan` rather than
retrying. `worker reported failure:` means the worker said it could not finish
(`summary` and `notes` are in the node's `result`); `critic failed:` means no
usable review came back.

For a PRD-sourced run (the state's `source` file contains `stories`), mirror the
terminal statuses back once at the end:

```python
from pathlib import Path

src = dag.get("source")
prd = json.loads(Path(src).read_text(encoding="utf-8")) if src and os.path.exists(src) else {}
if "stories" in prd:
    changed = sync_prd(dag, src)
    stories = json.loads(Path(src).read_text(encoding="utf-8"))["stories"]
    shown = {s.get("id"): s.get("status") for s in stories if isinstance(s, dict)}
    behind = [n["id"] for n in dag["nodes"] if n["status"] in ("done", "blocked", "skipped") and shown.get(n["id"]) != n["status"]]
    print("prd statuses updated:", changed, "| not mirrored:", behind or "none")
```

`sync_prd` copies statuses only while the PRD still holds the plan this run was
started from (the comparison `prepare_dag` makes). After a re-plan or edit, as
when an old run is finished by its state path after Cancel on `StaleState`, it
writes nothing, returns 0, and logs why; `not mirrored` then lists the finished
nodes. Say in the report that their statuses were not copied because they
describe a plan the PRD no longer holds, and never copy them by hand. A Restart
that runs the new plan mirrors normally.

A run with any `blocked` or `skipped` node is incomplete: report it so, never
under a headline "done". If new scope surfaced, suggest `/skill:ralplan`; never
fold it into this run. Dag is the terminal stage: after the report, stop.

## Optional: TypeSafe checks

Off by default. Two advisory signals from omp's `judge_batch`; they need a
TypeSafe credential (`TYPESAFE_API_KEY` or `/login typesafe`). Never print the key.

- Per DAG: a top-level `"typesafe"` key in the DAG or PRD file (`true`, a word
  such as `"shadow"`, or `{"lint": true, "screen": "off|shadow|enforce"}`). It is
  copied into the state file only when that file is created; for a saved run,
  edit the state file.
- Per session: `env("OMP_SKILLS_TYPESAFE", "shadow")` in an eval cell before
  Cell 2 (`1`/`on`/`shadow` = lint + shadow screen, `enforce` = lint + enforcing
  screen). Exporting the variable before launching omp does nothing: omp starts
  the eval kernel with an allowlisted environment, so TypeSafe stays off silently;
  Cell 2 prints the mode it found. `TYPESAFE_API_KEY` is read by the omp host, so
  exporting it works.
- `lint` flags criteria that name no command or no pass condition. `screen`
  checks after each worker whether its own evidence contradicts a criterion:
  `shadow` only records `node["screen"]`; `enforce` may turn a high-confidence
  contradiction into a revise verdict before the critic, never on a run's last
  attempt.
- TypeSafe never approves anything. Only the critic does.
- Fail open: an unavailable judge, or a fallback to a non-TypeSafe chat model
  (its answers are ignored), reports the check as skipped and the run continues.
- Evidence is redacted by shape (keys, tokens, passwords, cookies, URL
  credentials, `-p`/`--password` arguments), so a secret with none, such as a
  bare 40-character hex string, passes through, and criteria text and evidence
  leave the machine. Keep secrets out of criteria and command output. Run `shadow`
  and compare it with the critic's verdicts before ever using `enforce`.
