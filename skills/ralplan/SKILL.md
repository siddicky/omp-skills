---
name: ralplan
description: Turn an approved spec (or a request the user says to plan directly) into a critic-reviewed PRD at .omp/pipeline/prd.json whose stories carry testable acceptance criteria, depends_on, and file ownership - the input /skill:dag executes. Use ONLY when explicitly invoked. Do NOT use for small direct edits.
hide: true
---

# Ralplan

Convert an approved spec into a `prd.json` of testable, dependency-ordered
stories that a critic reviews before `/skill:dag` may run unattended against it.

Explicit invocation only (`/skill:ralplan`); nothing auto-routes a vague
`/skill:dag` request here. If a request looks underspecified for an unattended
run, say so in prose ("recommend `/skill:deep-interview` first") and let the user
decide. If this body was loaded without a user message invoking
`/skill:ralplan`, stop and ask the user to run it.

Not in plan mode (it blocks writing `.omp/pipeline/prd.json`): if the system
prompt says plan mode is active, stop and tell the user to exit plan mode and
re-run `/skill:ralplan <args>`. An `ask` answer with `timedOut: true` is silence,
never approval: re-ask once, then stop.

## Inputs

The `User:` arg is a spec path when it is a single token ending in `.md`, else
free text.

- Spec path: if the file does not exist, stop and tell the user to run
  `/skill:deep-interview` first. Its first line must start with `<!-- APPROVED`
  (for example `<!-- APPROVED 2026-09-30 -->`); with `<!-- UNAPPROVED DRAFT -->`
  or no marker, stop: "this spec is not approved; approve it via
  `/skill:deep-interview` first". A non-empty `## Open items` section is copied
  into a top-level `"open_questions": [...]` array of the PRD and shown at the
  approval gate.
- Free text: plan directly only when the user's text explicitly says to (for
  example "plan directly: ..."); use the request verbatim as the goal and set
  `source_spec` to `null`. Otherwise stop with "recommend
  `/skill:deep-interview` first" and let the user decide.

## Producing the PRD

Write `.omp/pipeline/prd.json` (create `.omp/pipeline/` if missing), always with
`"approved": false`: approval happens only at the gate. The PRD is a singleton:
if the file exists, tell the user it is replaced (dag state files under
`.omp/pipeline/dag/` are separate and untouched).

```json
{
  "goal": "one sentence",
  "source_spec": ".omp/pipeline/specs/<slug>.md",
  "approved": false,
  "created": "<UTC timestamp, e.g. from `date -u +%Y-%m-%dT%H:%M:%SZ`>",
  "stories": [
    {
      "id": "US-001",
      "title": "short title",
      "task": "complete, self-contained instructions a subagent can execute without reading the spec",
      "acceptance_criteria": [
        "`grep -q 'export function foo' src/foo.ts` exits 0"
      ],
      "depends_on": [],
      "files": ["src/foo.ts"],
      "agent": "task",
      "status": "pending"
    }
  ]
}
```

Rules:

- Story ids are `US-NNN`, sequential.
- `task` must be complete on its own: a subagent given only this text, the goal,
  and upstream results must be able to succeed. It is embedded verbatim in the
  dag worker prompt.
- `acceptance_criteria` are testable by command output or a reproducible check,
  never by opinion ("Code is cleaner" is invalid): name the exact command and its
  pass condition ("exits 0", "prints exactly `X`"). Workers and the critic run
  them, so:
  - Make them policy-neutral: avoid `cat`, `head`, `tail`, `find`, `sed -i`, and
    `echo > file`, which a shell policy may block. Prefer `test`, `cmp`,
    `grep -c`, `od`, `wc` with a redirect, or the project's own test command.
  - Make them portable between BSD (macOS) and GNU userland: no `sed -i`,
    `grep -P`, `readlink -f`, or `date -d`; never compare raw `wc` output as text
    (space-padded on macOS): use `test "$(wc -c < f.txt | tr -d ' ')" = 11` exits 0.
  - Never rely on network access, installs, `sudo`, or paths outside the repo.
- `depends_on` holds story ids only for real data/interface dependencies, never
  for tidiness.
- `files` is required: every path or glob the story may edit, including the
  manifests and lockfiles its task touches (`package.json`, lockfiles,
  `pyproject.toml`, ...). `[]` is a read-only story: it must not modify anything.
  Entries are relative to the repo: no absolute paths, no `..`. Write `dir/**`
  (or `dir/`) for a directory, never a bare name (a bare name with a dot,
  `packages/ui.kit`, reads as a file). Two stories with overlapping `files` MUST
  be linked by a `depends_on` path; the dag runner rejects the PRD otherwise.
- `agent` is any agent visible in `/agents`; default `task`. Read-only or
  mechanical items may use lighter agents if available.
- When the spec has a `## Work units` section that is not `None`, seed the
  stories from it 1:1, splitting a unit only when its criteria genuinely cannot
  share one node.

## Validate before the critic

When `eval` is available, check the PRD mechanically with the dag runner
(substitute this skill's directory from the invocation message, without a
trailing slash):

```python
import os
SKILL_DIR = "<skill directory>"
DAG_DIR = os.path.join(os.path.normpath(SKILL_DIR), "..", "dag")
for _name in ("runner.py", "judgments.py"):
    exec(open(f"{DAG_DIR}/{_name}", encoding="utf-8").read(), globals())
dag = load_dag(".omp/pipeline/prd.json")
errs = validate_dag(dag)
display(errs)
if typesafe_mode(dag)["lint"]:
    lint = await lint_criteria(dag)
    print("lint:", lint["status"], lint.get("reason") or "")
    for w in lint["weak"]:
        print(f'{w["node_id"]}#{w["index"]}: {w["why"]}: {w["criterion"]}')
```

Fix every error and re-run until `errs` is empty; never call the critic with a
PRD that fails validation, and re-run after every later change to the PRD. Lint
findings appear only when the optional TypeSafe checks are on (see `/skill:dag`;
the PRD's top-level `"typesafe"` key or `env("OMP_SKILLS_TYPESAFE", "shadow")` in
a cell turns them on, an exported variable does not) and are advisory: rewrite
weak criteria when you can, carry the rest to the gate. If `eval` is unavailable,
skip this step and say "mechanical validation skipped (no eval)" at the gate.

## Critic review loop

Before presenting the plan, get an adversarial second opinion from the `critic`
agent:

```
task {
  context: "<goal>. Spec: <spec path, or 'none (planned directly from the request)'>.",
  tasks: [{
    name: "PrdCritic",
    agent: "critic",
    task: "Review .omp/pipeline/prd.json against <spec path, or 'the goal in the context line'>. Check: every spec acceptance criterion (or goal requirement) is covered by at least one story; every story criterion is testable by command or inspection; depends_on reflects real dependencies and overlapping files are ordered; no scope beyond the spec. Use node_id = story id.",
    solutionSpace: "Closed review of one file against one spec; return findings only."
  }]
}
```

`task` runs in the background and its result auto-delivers; never poll. Note the
agent id the call reports (`agent://<id>`; it can differ from the requested name).
With nothing else to do, call `wait` (no arguments) until that critic's result is
the one delivered (it may return an unrelated job or message first); skip the
wait if the task call returned the result inline. Read the verdict from
`agent://<id>/verdict` and the findings from `agent://<id>/findings`.

- On `revise`: apply every `blocker` and `major` fix to the PRD (fix, do not just
  note), re-run the validation cell, then dispatch round 2 as `PrdCritic2` with
  the round-1 findings in its task text so it checks the fixes. At most 2 review
  rounds total.
- Collect every `minor` finding from every round into a top-level
  `"notes": [...]` array in the PRD.
- After round 2 still `revise`: write the remaining blocker/major findings into a
  top-level `"open_findings": [...]` array and surface them at the gate. Never
  loop further, never silently drop them.

## Approval gate

In a normal reply, present everything that will be executed:

- For every story: id, title, `depends_on`, `files`, its `task` verbatim, and each
  acceptance criterion verbatim and numbered. Approval authorizes unattended
  shell commands run by workers and the critic with the user's privileges, so the
  user must see them. Call out any task or criterion that reaches outside the repo
  or the story's own files: network fetches, installs, `sudo`, absolute paths,
  `git push`, deletion.
- The critic verdict and round count, `open_findings`, `notes`, `open_questions`,
  mechanical validation status, and any weak-criteria lint.

Then ask one `ask` question `id: approval` with options in this order:
`Request changes` (index 0), `Approve`, `Cancel`. Do not set `recommended`.

- Approve only when the answer is exactly `Approve` and not `timedOut`. Free text
  typed through "Other", silence, a topic change, or your own confidence is never
  approval; treat typed text as a change request.
- `Request changes`: apply them, re-run the validation cell, and re-present,
  listing what changed since the last critic pass. The 2-round critic budget is
  not reset; run another critic pass only if the user asks.
- `Cancel`: leave `"approved": false` and stop.
- `Approve`: run `approve_file(".omp/pipeline/prd.json")` in an eval cell (the
  runner is already loaded); it sets `"approved": true` and `"approved_at"`. If
  `eval` is unavailable, edit the file to set `"approved": true` and an
  `"approved_at"` UTC timestamp.
- Headless (no `ask` tool): leave `"approved": false`, print the summary above,
  and say: "To approve, set `"approved": true` in `.omp/pipeline/prd.json`, then
  run `/skill:dag .omp/pipeline/prd.json`." Then stop.

## Handoff

On approval, say in prose: "PRD approved at `.omp/pipeline/prd.json`. Run
`/skill:dag .omp/pipeline/prd.json` next." Nothing wires this handoff
automatically. Then stop.
