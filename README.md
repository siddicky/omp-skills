# omp-skills

Pipeline skills for [Oh My Pi (omp)](https://ohmypai.dev): a gated path from a
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
ralph`), replacing muse primitives with omp ones: `ask` for every user gate,
`task`/`eval agent()` for subagents, and a custom `critic` task agent instead
of an external CLI reviewer.

All three skills are **explicit-invocation only** — nothing auto-fires them.
Sequencing between stages is by prose: each skill tells you what to run next
and then stops.

## Install

Append to `~/.omp/agent/config.yml` (create the key if absent):

```yaml
extensions:
  - /Users/siddicky/Projects/github/omp-skills
```

Restart omp. Then verify:

- `/skill:` autocomplete lists `deep-interview`, `ralplan`, `dag`
- `/agents` lists `critic` (tools: `read, grep, glob, bash`)

If `critic` does not appear in `/agents` but the skills do, copy
`agents/critic.md` to `~/.omp/agent/agents/critic.md` — project/user agent
dirs always load.

## State

Pipeline state lives under `.omp/pipeline/` in each project:

| Path | Written by | Contents |
| --- | --- | --- |
| `.omp/pipeline/specs/<slug>.md` | deep-interview | approved specs |
| `.omp/pipeline/prd.json` | ralplan | the PRD |
| `.omp/pipeline/dag/<slug>.json` | dag | runnable DAG + per-node status |

`.omp/` is also where omp discovers project agents, extensions, and skills;
the `pipeline/` subdirectory collides with none of those. Recommend adding
`.omp/pipeline/` to each project's `.gitignore`.

## The critic agent

`agents/critic.md` defines a read-only adversarial reviewer with a structured
output schema (`verdict: approve|revise`, ranked `findings`). `ralplan` uses
it to review the PRD; `dag` uses it to gate every node. Approve requires zero
blocker and zero major findings.

To route critic reviews through a different model, map the agent:

```yaml
task:
  agentModelOverrides:
    critic: "openai/gpt-5.4:high"
```

## DAG semantics

- A node starts as soon as all of its `depends_on` nodes are approved — not
  in lockstep waves.
- Every node's output passes a `critic` review; `revise` feeds the findings
  back into a retry (default 2 attempts), then the node is `blocked`.
- Dependents of a `blocked` or `skipped` node are `skipped`, never run on a
  stale premise.
- Without `task.isolation.enabled`, concurrent nodes are kept safe by
  disjoint `files` ownership; the runner rejects a DAG where two
  non-dependent nodes own the same file pattern.
- State persists to the DAG file after every transition: Ctrl-C aborts the
  run, re-invoking `/skill:dag <same path>` resumes (done nodes are not
  re-run). Top-level `"retry_blocked": true` re-attempts blocked nodes.

See `skills/dag/example.json` for a minimal four-node fixture.
