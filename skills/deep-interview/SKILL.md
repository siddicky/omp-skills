---
name: deep-interview
description: Socratic interview that turns a vague request into a decision-complete spec at .omp/pipeline/specs/<slug>.md, gated by a measured ambiguity score. Use ONLY when the user explicitly invokes /skill:deep-interview or asks to interview/spec a fuzzy idea before planning. Do NOT use for ordinary implement/fix requests or as an automatic first step on large work.
hide: true
---

# Deep Interview

Convert an ambiguous request into a spec that `ralplan` can plan from, by asking
targeted questions and tracking how much ambiguity remains after each round.

Runs only when explicitly invoked (`/skill:deep-interview`, or a direct user
ask); nothing auto-selects it because a task looks complex or vague. If this body
was loaded without such a user message, stop and ask the user to run
`/skill:deep-interview`.

Not in plan mode (it blocks writing under `.omp/`): if the system prompt says plan
mode is active, stop and tell the user to exit plan mode and re-run
`/skill:deep-interview <request>`.

## Inputs

The `User:` args are the initial request. A leading `threshold=<n>` token (1-50)
sets the stop threshold; strip it from the request. If the request is empty,
reply in prose asking for it and end the turn: do not call `ask`, do not offer a
placeholder option, do not proceed.

## State

Specs live at `.omp/pipeline/specs/<slug>.md`. Compute the slug the same way
every time:

1. Take the goal (one sentence) as ASCII: fold accents (NFKD) and drop what does
   not fold.
2. Lowercase it and replace each run of characters outside `a-z0-9` with `-`.
3. Strip leading and trailing `-`.
4. If longer than 40 characters, keep the first 40. If the last `-` in that piece
   is its 22nd character or later (the first character is number 1), cut the
   piece at that `-`; a `-` that is the 21st character or earlier is left alone.
   Strip a trailing `-`.
5. If nothing is left, use `untitled`.

If that file already exists and was not written by this interview, it belongs to
something else: use `<slug>-2`, then `<slug>-3`, and so on. Never overwrite
another spec. Create the directory with `bash mkdir -p .omp/pipeline/specs` if
missing. Never write under `.omp/agents`, `.omp/extensions`, or `.omp/skills`:
those are omp-discovered paths, not scratch space.

## Ambiguity model

Track an explicit ambiguity score (0-100%) across rounds, not a fixed question
budget:

1. Before round 1, enumerate the entities implied by the request (nouns like
   "user", "state directory", "critic", "acceptance criteria") and mark each
   Unknown, Assumed, or Locked. Give each a load-bearing weight of 1-3 (a naming
   detail is 1; a topology or interface decision is 3). If you cannot name a
   single entity, ambiguity is 100%: ask what the request is about. Never score
   an empty table as 0%.
2. Each round, ask the smallest set of questions (at most 3) that would lock the
   most Unknown/Assumed entities; prefer questions that collapse several unknowns
   at once.
3. After each answer, recompute:

   `ambiguity % = 100 × Σ weight(entity still Unknown or Assumed) / Σ weight(all entities)`

4. Check the threshold first. Stop asking and draft when ambiguity is below it
   (default 10%, or the `threshold=<n>` argument; there is no omp setting for
   this). Say which threshold you used.
5. Only if the threshold check failed, and only from round 3 on: if the last two
   rounds each dropped ambiguity by less than 5 points and ambiguity is still at
   least threshold + 5, stop and report the stall instead of looping. Hand the
   user a partial spec with the stuck questions listed as open items.

Keep the entity table (entity, status, weight) in the working context and update
it after every answer, before the next round. A new entity gets a row; an answer
that reopens a decision moves its entity back to Unknown.

## Asking

Use the `ask` tool when it is in the tool inventory: one call per round, at most
3 questions, each with 2-4 concrete mutually exclusive options plus a
`recommended` default (a 0-based option index). Without `ask` (headless), print
the round's questions in prose and end the turn; the user's next message is the
answer. An `ask` answer with `timedOut: true` is not a user answer: do not lock an
entity from it; re-ask or leave the entity Unknown.

## Codebase grounding

Before round 1, if the request names a repo, codebase, or area, spawn exactly one
read-only scout to establish current facts:

```
task { context: <goal + entities>, tasks: [{ name: "InterviewScout", agent: "scout",
  task: "Enumerate the current state of <entities> in this codebase: what exists, where, with what interfaces. Facts only, no recommendations.",
  solutionSpace: "Read-only survey of the current code; no design choices." }] }
```

`task` runs in the background and its result auto-delivers; never poll. Call
`wait` (no arguments) until the scout's result arrives; if it returns another job
or message first, call it again. If the output is truncated, read `agent://<id>`
using the id the task call reported (it can differ from the requested name). Do
not spawn any other agent types from this skill.

Fold its findings into the entity table. Facts describe what exists, not what the
user wants: an entity is Locked only when its current state is observed and the
user's intent for it is settled; otherwise it stays Unknown or Assumed.

## Challenge round

Optionally, once the threshold check passes and before drafting, run at most one
challenge round when the ontology looks too comfortable, and only when no
weight-3 entity is Unknown:

- Contrarian: "what would make this the wrong approach entirely?"
- Simplifier: "what would the smallest version that still satisfies the goal look
  like?"

Note which mode ran in the spec header. If its answer pushes ambiguity back above
the threshold, keep asking regular rounds until it is below again, then draft.

## Producing the spec

Write `.omp/pipeline/specs/<slug>.md`. Line 1 is `<!-- UNAPPROVED DRAFT -->`
until the approval gate changes it. Then:

- Header line: `challenge: <none|contrarian|simplifier>`, `threshold: <n>%`,
  `final ambiguity: <n>%`.
- One-sentence goal.
- Fact base: what was established (from the codebase, from the user, from probing
  tools) versus assumed.
- Locked decisions, each with the round it was settled in, why, and a verbatim
  quote of the user's own words, for example
  `- Use SQLite (round 2, "just keep it in sqlite for now"): why ...`. A decision
  with no user quote is not locked: list it under assumptions.
- Stated-but-unconfirmed assumptions, flagged explicitly.
- A required `## Acceptance criteria` section: bullets of statements the
  implementation must satisfy, each checkable by a command or a concrete
  observation.
- Open items (only when the interview stalled: the stuck questions).
- A required `## Work units` section: bullets of candidate independent units of
  work, each naming the files it touches (`ralplan` turns these into stories with
  `depends_on` and `files`). Write `None` rather than padding.

Do not silently invent scope to fill a section; write "None" when a section has
nothing real to say.

## Explicit approval gate

Present the complete spec in a normal reply, then ask one `ask` question
`id: approval` with options in this order: `Request changes` (index 0),
`Approve`, `Cancel`. Do not set `recommended`.

- Approve only when the answer is exactly `Approve` and not `timedOut`. Silence, a
  topic change, your own confidence, an `ask` timeout, or free text typed through
  "Other" is never approval; treat typed text as a change request.
- `Request changes`: revise the same spec file and re-present it.
- `Cancel`: leave line 1 as `<!-- UNAPPROVED DRAFT -->` and stop.
- `Approve`: replace line 1 with `<!-- APPROVED YYYY-MM-DD -->` (today's UTC date,
  for example from `date -u +%F`).
- Headless (no `ask` tool): leave line 1 as `<!-- UNAPPROVED DRAFT -->`, print the
  spec, and say: "To approve, replace the first line of
  `.omp/pipeline/specs/<slug>.md` with `<!-- APPROVED YYYY-MM-DD -->` (today's
  date), then run `/skill:ralplan .omp/pipeline/specs/<slug>.md`." Then stop.

The two markers are exactly `<!-- APPROVED YYYY-MM-DD -->` and
`<!-- UNAPPROVED DRAFT -->`; `ralplan` reads line 1 to decide whether to plan.

## Handoff

Nothing chains into the next stage automatically. Once the spec is approved, say
in plain prose: "Spec approved and saved to `.omp/pipeline/specs/<slug>.md`. Run
`/skill:ralplan .omp/pipeline/specs/<slug>.md` next to turn this into a plan."
Then stop; do not start planning or implementation from inside this skill.
