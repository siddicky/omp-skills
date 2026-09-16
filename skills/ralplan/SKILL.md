---
name: ralplan
description: Turn an approved spec (or a request specific enough to skip the interview) into a critic-reviewed PRD at .omp/pipeline/prd.json whose stories carry testable acceptance criteria, depends_on, and file ownership - the input /skill:dag executes. Use ONLY when explicitly invoked. Do NOT use for small direct edits.
---

# Ralplan

Convert an approved spec (or a request specific enough to skip straight to
planning) into a `prd.json` of testable, dependency-ordered stories, reviewed
by a critic before `/skill:dag` is allowed to run unattended against it.

Explicit invocation only. Nothing auto-routes a vague `/skill:dag` request
into this skill. If a request looks underspecified for an unattended dag run,
say so in prose — "recommend `/skill:deep-interview` first" — and let the
user decide.

## Inputs

The `User:` arg is a spec path (preferred) or free text to plan directly. If
the path does not exist on disk, stop and tell the user to run
`/skill:deep-interview` first — unless their text explicitly says to plan
directly.

## Producing the PRD

Write `.omp/pipeline/prd.json` (create `.omp/pipeline/` if missing) shaped as:

```json
{
  "goal": "one sentence",
  "source_spec": ".omp/pipeline/specs/<slug>.md",
  "approved": false,
  "created": "<ISO timestamp>",
  "stories": [
    {
      "id": "US-001",
      "title": "short title",
      "task": "complete, self-contained instructions a subagent can execute without reading the spec",
      "acceptance_criteria": [
        "testable statement naming the exact command or observation that proves it"
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
- `task` must be complete on its own: a subagent executing only this text, the
  goal, and upstream results must be able to succeed. It is embedded verbatim
  in the dag worker prompt.
- `acceptance_criteria` must be testable by inspection, command output, or
  reproducible check — not by opinion. "Code is cleaner" is not a valid
  criterion; "`bun test packages/x --filter foo` exits 0" is. Name the command
  or the observation in the criterion itself. `dag` iterates against these
  with a critic, so vague criteria become the run's failure mode later.
- `depends_on` holds story ids only for real data/interface dependencies —
  never for tidiness.
- `files` lists every path the story may edit (globs allowed). Two stories
  with overlapping `files` MUST be linked by a `depends_on` path between them;
  the dag runner rejects the PRD otherwise.
- `agent` is any agent visible in `/agents`; default `task`. Read-only or
  mechanical items may use lighter agents if available.
- When the spec has a `## Work units` section, seed the stories from it 1:1,
  splitting a unit into multiple stories only when its criteria genuinely
  cannot share one node.

## Critic review loop

Before presenting the plan, get an adversarial second opinion from the
`critic` agent:

```
task {
  context: "<goal>. Spec: <spec path or 'none'>.",
  tasks: [{
    name: "PrdCritic",
    agent: "critic",
    task: "Review .omp/pipeline/prd.json against <spec path>. Check: every spec acceptance criterion is covered by at least one story; every story criterion is testable by command or inspection; depends_on reflects real dependencies and overlapping files are ordered; no scope beyond the spec. Use node_id = story id."
  }]
}
```

Wait for it with `hub { op: "wait", ids: ["PrdCritic"] }`, re-issuing until
the job settles. Read the verdict from `agent://PrdCritic` (structured output;
fall back to `agent://PrdCritic?q=.verdict`).

- On `revise`: apply every `blocker` and `major` fix to the PRD — fix, do not
  just note — then re-dispatch with a fresh name (`PrdCritic2`, `PrdCritic3`).
  At most 2 review rounds total.
- After round 2 still `revise`: write the remaining findings into a top-level
  `"open_findings": [...]` array in the PRD and surface them at the approval
  gate. Never loop further, never silently drop them.

## Approval gate

Present a per-story summary (id, title, depends_on, files, criteria count),
the critic verdict and round count, and any open findings. Then ask with one
`ask` question `id: approval`, options `Approve` / `Request changes` /
`Cancel`, recommended `Approve`. Do not let `dag` start from an unapproved
PRD. On `Request changes`, revise and re-present. On `Cancel`, leave
`"approved": false` and stop. On `Approve`, set `"approved": true` in the file.

## Handoff

On approval, say in prose: "PRD approved at `.omp/pipeline/prd.json`. Run
`/skill:dag` next." There is no mechanism that wires this handoff
automatically — it exists because this body says it. Then stop.
