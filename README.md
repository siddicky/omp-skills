# omp-skills

Pipeline skills for [Oh My Pi (omp)](https://omp.sh): a gated path from a
vague request to parallel, critic-gated execution.

```
/skill:deep-interview  → turns a vague request into an approved spec
                         (.omp/pipeline/specs/<slug>.md)
/skill:ralplan         → turns an approved spec into a critic-reviewed PRD
                         (.omp/pipeline/prd.json) whose stories carry
                         depends_on, file ownership, and acceptance criteria
/skill:dag             → executes the PRD (or an explicit task list) as a
                         dependency DAG of parallel subagents, with a critic
                         gate on every node before its dependents start
```

Ported from oh-my-musecode's Tier-0 pipeline (`deep-interview → ralplan →
ralph`), using omp's `ask`, `task`/`wait`, eval `agent()`, and a custom `critic`
agent. All three skills set `hide: true`, so omp does not list them to the model
and nothing auto-fires them: you invoke them with `/skill:<name>`, and each tells
you what to run next, then stops. The full procedure of each is its
`skills/<name>/SKILL.md`; this file only summarises.

## Requirements

- omp 18.4 or newer (developed against 18.4.5; the `hub` tool is gone since
  18.3.0). The skills use `ask`, `task`, `wait`, `read`, `write`, `bash`, and
  `eval` (Python, for `dag` and ralplan's validation step).
- Not in plan mode: it makes subagents read-only and blocks writes to the working
  tree. With isolation on, omp rejects the `agent()` spawn and the run aborts;
  with isolation off no worker can edit a file, so every node that has to change
  one ends up `blocked` with its dependents `skipped`. The runner cannot detect
  plan mode, so each skill checks the system prompt and stops. If
  `plan.defaultOnStartup` is on, leave plan mode before running a skill.
- A persistent Python kernel (the default `python.kernelMode: session`): the dag
  cells share names.

## Install

Append to `~/.omp/agent/config.yml`:

```yaml
extensions:
  - <path-to>/omp-skills
```

`<path-to>` is wherever you cloned this repo. Restart omp, then check that
`/skill:` autocomplete lists `deep-interview`, `ralplan`, `dag` and `/agents`
lists `critic` (tools: `read, grep, glob, bash`). No `index.ts` or `package.json`
entry is needed: omp discovers `skills/` and `agents/` in any `extensions:` root.

A project's `.omp/config.yml` `extensions:` list replaces the user list instead
of merging with it: add this repo there too, or `/skill:dag` disappears and the
other two can resolve to same-named skills from other packs.

Do not copy `agents/critic.md` into `~/.omp/agent/agents/`: a user-level agent
shadows the one here and drifts. Delete an old copy or symlink it to
`agents/critic.md`.

## State

Pipeline state lives under `.omp/pipeline/` in each project (add it to
`.gitignore`; it collides with nothing omp discovers under `.omp/`):

| Path | Written by | Contents |
| --- | --- | --- |
| `.omp/pipeline/specs/<slug>.md` | deep-interview | specs; line 1 is `<!-- APPROVED YYYY-MM-DD -->` or `<!-- UNAPPROVED DRAFT -->` |
| `.omp/pipeline/prd.json` | ralplan; dag mirrors terminal statuses into it | the PRD |
| `.omp/pipeline/dag/<slug>.json` | dag | runnable DAG + per-node status (the state file) |

## The critic agent

`agents/critic.md` is an adversarial reviewer with a structured output schema
(`verdict: approve|revise`, ranked `findings`, each optionally `target:
work|plan`). It has `bash` for acceptance commands and read-only git, but omp does
not enforce that: it is a rule it follows, not a sandbox. Approve requires zero
blocker and zero major findings, and the runner recomputes the verdict from the
findings instead of trusting the critic's word. An answer that is not a clear
`approve`/`revise` plus a findings list never approves a node (omp drops the
output schema after three failed validations and passes the raw data through).
The schema in `agents/critic.md` and `CRITIC_SCHEMA` in `skills/dag/runner.py`
must stay identical.

To route critic reviews through a different model:

```yaml
task:
  agentModelOverrides:
    critic: "openai/gpt-5.4:high"
```

## DAG semantics

Details are in `skills/dag/SKILL.md`.

- A node starts as soon as its `depends_on` nodes are approved, not in waves.
  Concurrent agents are capped by `task.maxConcurrency` (omp's default is 32, so
  unset means 16: the runner never uses more than 16, and uses 4 only when it
  cannot read a positive number, for example omp is not on `PATH` or the setting
  is 0, which omp reads as unlimited).
- Every node passes a `critic`; `revise` feeds the findings into a retry (default
  2 attempts), then the node is `blocked`. A failed worker, or an unusable critic
  answer, is retried and never approves. A blocker or major finding with
  `target: plan` blocks the node at once (`plan defect:`). Dependents of a
  `blocked` or `skipped` node are `skipped`; statuses are `pending`, `running`,
  `review`, `done`, `blocked`, `skipped`.
- `files` is required on every node: the paths or globs it may edit, `[]` for a
  read-only node. Overlapping `files` between nodes with no dependency path are
  rejected unless workers are isolated (`task.isolation.enabled` and a git work
  tree), where omp applies a worker's patch before the critic reviews it: the
  gate stops dependents, not integration. Isolation is read with
  `omp config get`, that is from config.yml (global or project); a per-run
  `omp --config <overlay>` is not visible to `detect_isolation()`, so such a run
  proceeds without isolation and with the overlap check on (fails safe). Case is
  ignored, as on a default macOS volume.
- State is saved atomically after every transition. Esc aborts the run (Ctrl+C
  only clears the editor); `/skill:dag <state path>` resumes it, `done` nodes are
  not re-run, and blocked nodes are retried only when you choose to.
- A source is never run in place: a PRD or explicit file is copied to
  `.omp/pipeline/dag/<slug>.json`, and an existing state file is resumed, never
  overwritten, unless you choose Restart. If the source's plan (goal, ids, titles,
  tasks, criteria, dependencies, files, agents; not statuses or approval stamps)
  changed since, dag says what changed and asks Restart or Cancel; to finish the
  old run pass its state path, and its statuses are then not copied into the
  re-planned PRD. A source that is no longer approved is refused.

`skills/dag/example.json` is a minimal approved four-node fixture (run it in a
throwaway directory: its nodes create `greeting.txt`, `name.txt`, and `hello.txt`).

## Optional: TypeSafe checks

Off by default. Two advisory checks use omp's `judge_batch` (TypeSafe System One):
a lint of criteria that name no command or pass condition, and a screen that
compares a worker's own evidence with each criterion. They need a TypeSafe
credential (`TYPESAFE_API_KEY` or `/login typesafe`); without one omp falls back
to chat models and the pack ignores those answers.

Enable per DAG with a top-level `"typesafe"` key in the DAG or PRD file (`true`, a
word such as `"shadow"`, or `{"lint": true, "screen": "off|shadow|enforce"}`), or
for the session with `env("OMP_SKILLS_TYPESAFE", "shadow")` (or `"enforce"`) in
an eval cell. A variable exported before launching omp does not work: omp starts
the eval kernel with an allowlisted environment, so TypeSafe stays off silently
(Cell 2 prints the mode it found). `TYPESAFE_API_KEY` is read by the omp host, so
exporting it does work. The checks fail open, redact secrets from the evidence
they send (by shape, so a secret with none, such as a bare 40-character hex string, still
passes), and never
approve anything: only the critic does. Run `shadow` first; criteria text and
evidence leave the machine.

## Tests

```
uv run python -m unittest discover -s tests -v
```
