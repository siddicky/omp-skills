---
name: deep-interview
description: Socratic interview that turns a vague request into a decision-complete spec at .omp/pipeline/specs/<slug>.md, gated by a measured ambiguity score. Use ONLY when the user explicitly invokes /skill:deep-interview or asks to interview/spec a fuzzy idea before planning. Do NOT use for ordinary implement/fix requests or as an automatic first step on large work.
---

# Deep Interview

Convert an ambiguous request into a spec that `ralplan` can plan from, by
asking targeted questions and tracking how much ambiguity remains after each
round.

This skill only runs when explicitly invoked (`/skill:deep-interview`, or a
direct user ask). Nothing auto-selects it because a task looks complex or
vague. If you are reading this body, something explicitly asked for it.

## Invocation and inputs

The `User:` args are the initial request. If args are empty, get the request
first with one `ask` question offering the option `I'll type it` (recommended)
— the runtime adds `Other (type your own)`, which is where the user enters the
request. Do not proceed without a request.

## State

Specs live at `.omp/pipeline/specs/<slug>.md`. The slug is the kebab-case
form of the goal, at most 40 characters. Create the directory with
`bash mkdir -p .omp/pipeline/specs` if missing. Never write under
`.omp/agents`, `.omp/extensions`, or `.omp/skills` — those are omp-discovered
paths, not scratch space.

## Ambiguity model

Track an explicit ambiguity score (0-100%) across rounds, not a fixed question
budget:

1. Before round 1, enumerate the entities implied by the request (nouns like
   "user", "state directory", "critic", "acceptance criteria") and mark each
   Unknown, Assumed, or Locked. Assign each entity a load-bearing weight of
   1-3 (a naming detail is 1; a topology or interface decision is 3).
2. Each round, ask the smallest set of questions (at most 3) that would lock
   the most Unknown/Assumed entities. Prefer questions that collapse several
   unknowns at once over exhaustive coverage.
3. After each answer, recompute ambiguity as the weighted fraction of
   entities still Unknown or Assumed:

   `ambiguity % = 100 × Σ weight(entity still Unknown or Assumed) / Σ weight(all entities)`

4. Stop asking and move to drafting when ambiguity falls below the threshold
   (default 10%; honor a `threshold=<n>` argument from `User:` args if given
   — there is no omp setting for this). Say which threshold you used.
5. If ambiguity has not dropped by at least 5 points for two consecutive
   rounds, stop the interview and report the stall rather than looping —
   hand the user a partial spec with the stuck questions listed as open
   items instead of asking indefinitely.

Keep the entity table (entity, status, weight) in the working context and
update it after every answer.

## Asking

Use the `ask` tool when it is in the tool inventory: one `ask` call per round,
at most 3 questions, each with 2-4 concrete mutually exclusive options plus a
`recommended` default. If `ask` is not available (headless session), print the
round's questions in prose and end the turn — the user's next message is the
answer. Record every answer in the entity table before the next round.

## Codebase grounding

Before round 1, if the request names a repo, codebase, or area, spawn exactly
one read-only scout to establish current facts:

```
task { context: <goal + entities>, tasks: [{ name: "InterviewScout", agent: "scout",
  task: "Enumerate the current state of <entities> in this codebase: what exists, where, with what interfaces. Facts only, no recommendations." }] }
```

Fold its findings into the entity table as Locked. Do not spawn any other
agent types from this skill.

## Challenge round

Optionally run at most one challenge round when the ontology looks too
comfortable — and only when ambiguity is already below 25% and no
weight-3 entity is Unknown:

- Contrarian: "what would make this the wrong approach entirely?"
- Simplifier: "what would the smallest version that still satisfies the goal
  look like?"

Note which mode ran in the spec header. Do not run more than one unless the
user asks.

## Producing the spec

Write the interview's output to `.omp/pipeline/specs/<slug>.md` with:

- Header line: `challenge: <none|contrarian|simplifier>`, `threshold: <n>%`,
  `final ambiguity: <n>%`.
- One-sentence goal.
- Fact base: what was established (from the codebase, from the user, from
  probing tools) versus assumed.
- Locked decisions, each with the round it was settled in and why.
- Any stated-but-unconfirmed assumptions, flagged explicitly.
- Acceptance criteria the eventual implementation must satisfy.
- Open items (only when the interview stalled — the stuck questions).
- A required `## Work units` section: a bullet list of candidate independent
  units of work, each naming the files it touches. This is what `ralplan`
  turns into stories with `depends_on` and `files`. Write `None` rather than
  padding the section.

Do not silently invent scope to fill a section. If a section has nothing real
to say, write "None".

## Explicit approval gate

Present the complete spec in a normal reply, then ask with one `ask` question
`id: approval`, options `Approve` / `Request changes` / `Cancel`,
recommended `Approve`. Do not treat silence, a topic change, or your own
confidence as approval. On `Request changes`, revise the same spec file and
re-present it. On `Cancel`, prepend `<!-- UNAPPROVED DRAFT -->` as the first
line of the file and stop. On `Approve`, prepend `<!-- APPROVED <ISO date> -->`
as the first line.

## Handoff

This skill does not chain into the next stage automatically. Once the spec is
approved, say in plain prose: "Spec approved and saved to
`.omp/pipeline/specs/<slug>.md`. Run `/skill:ralplan .omp/pipeline/specs/<slug>.md`
next to turn this into a plan." Then stop. Do not start planning or
implementation from inside this skill.
