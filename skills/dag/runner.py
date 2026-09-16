"""DAG runner for the omp `dag` skill.

Executed inside the omp Python eval kernel via:

    exec(open(f"{SKILL_DIR}/runner.py").read(), globals())

so the prelude helpers `agent`, `read`, `write`, `log`, and `phase` resolve
from globals. Stdlib imports only.
"""

import asyncio
import fnmatch
import json

CRITIC_SCHEMA = {
    "type": "object",
    "required": ["verdict", "findings"],
    "properties": {
        "verdict": {"type": "string", "enum": ["approve", "revise"]},
        "summary": {"type": "string"},
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["severity", "issue", "fix"],
                "properties": {
                    "severity": {"type": "string", "enum": ["blocker", "major", "minor"]},
                    "node_id": {"type": "string"},
                    "issue": {"type": "string"},
                    "fix": {"type": "string"},
                    "evidence": {"type": "string"},
                },
            },
        },
    },
}

UNPARSEABLE_VERDICT = {
    "verdict": "revise",
    "summary": "critic returned unparseable output",
    "findings": [
        {"severity": "major", "issue": "critic returned unparseable output", "fix": "re-run"}
    ],
}

VALID_STATUSES = {"pending", "running", "review", "done", "blocked", "skipped"}


def load_dag(path: str) -> dict:
    """Read a DAG file; accept top-level "nodes" or "stories"; normalize to "nodes"."""
    with open(path) as f:
        dag = json.load(f)
    if "nodes" not in dag and "stories" in dag:
        dag = dict(dag)
        dag["nodes"] = dag.pop("stories")
    return dag


def validate_dag(dag: dict) -> list:
    """Return error strings; [] = valid."""
    errors = []
    nodes = dag.get("nodes") or []
    seen = set()
    by_id = {}
    for n in nodes:
        nid = n.get("id")
        if not nid:
            errors.append("node missing id")
            continue
        if nid in seen:
            errors.append(f"duplicate id {nid}")
            continue
        seen.add(nid)
        by_id[nid] = n
        if not (n.get("task") or "").strip():
            errors.append(f"{nid} has empty task")
        if not n.get("acceptance_criteria"):
            errors.append(f"{nid} has no acceptance_criteria")
    for n in nodes:
        nid = n.get("id")
        for dep in n.get("depends_on") or []:
            if dep not in by_id:
                errors.append(f"{nid} depends on unknown {dep}")
            elif dep == nid:
                errors.append(f"cycle: {nid} -> {nid}")
    # Kahn's algorithm for cycle detection.
    indeg = {nid: 0 for nid in by_id}
    children = {nid: [] for nid in by_id}
    for n in nodes:
        nid = n.get("id")
        if nid not in by_id:
            continue
        for dep in set(n.get("depends_on") or []):
            if dep in by_id:
                indeg[nid] += 1
                children[dep].append(nid)
    queue = [nid for nid, d in indeg.items() if d == 0]
    order = []
    while queue:
        nid = queue.pop()
        order.append(nid)
        for c in children[nid]:
            indeg[c] -= 1
            if indeg[c] == 0:
                queue.append(c)
    if len(order) != len(by_id):
        remaining = [nid for nid in by_id if nid not in set(order)]
        # Walk one cycle to present it readably.
        cyc = [remaining[0]]
        cur = remaining[0]
        while True:
            nxt = None
            for dep in by_id[cur].get("depends_on") or []:
                if dep in by_id and dep not in set(order):
                    nxt = dep
                    break
            if nxt is None or nxt in cyc:
                break
            cyc.append(nxt)
            cur = nxt
        cyc.append(cyc[0])
        errors.append("cycle: " + " -> ".join(cyc))
    # File-ownership overlap between concurrent (no dependency path) nodes.
    if not dag.get("isolated"):
        for i, a in enumerate(nodes):
            for b in nodes[i + 1 :]:
                if _reachable(by_id, a.get("id"), b.get("id")) or _reachable(
                    by_id, b.get("id"), a.get("id")
                ):
                    continue
                for pa in a.get("files") or []:
                    for pb in b.get("files") or []:
                        if _patterns_overlap(pa, pb):
                            errors.append(
                                f"{a.get('id')} and {b.get('id')} both own {pa} "
                                f"but have no dependency path"
                            )
    return errors


def _reachable(by_id, src, dst):
    """True if dst is in the transitive depends_on closure of src."""
    stack = [src]
    vis = set()
    while stack:
        cur = stack.pop()
        if cur == dst:
            return True
        if cur in vis:
            continue
        vis.add(cur)
        for dep in by_id.get(cur, {}).get("depends_on") or []:
            if dep in by_id:
                stack.append(dep)
    return False


def _patterns_overlap(a: str, b: str) -> bool:
    """Two glob patterns overlap if either matches a literal token of the other
    (approximation: test mutual matching of the raw patterns, then of their
    non-glob prefixes)."""
    if a == b or fnmatch.fnmatch(a, b) or fnmatch.fnmatch(b, a):
        return True
    return _lit_prefix(a) == _lit_prefix(b) and _lit_prefix(a) is not None


def _lit_prefix(p):
    head = p.split("*")[0].split("?")[0].split("[")[0]
    return head or None


def _worker_prompt(dag: dict, node: dict, attempt: int, upstream_chars: int) -> str:
    by_id = {n.get("id"): n for n in dag["nodes"]}
    parts = [f"# Goal\n{dag.get('goal', '')}"]
    parts.append(f"# Your node: {node['id']} — {node.get('title', '')}")
    parts.append(f"# Task\n{node.get('task', '')}")
    files = node.get("files") or []
    parts.append(
        "# Files you own\n"
        + ("\n".join(f"- {f}" for f in files) if files else "- (not restricted)")
        + "\nDo not edit files outside this list. If the task requires it, stop and report instead."
    )
    deps = node.get("depends_on") or []
    if deps:
        ups = []
        for dep in deps:
            ev = (by_id.get(dep, {}).get("evidence") or "(no upstream result)")[:upstream_chars]
            ups.append(f"## {dep}\n{ev}")
        parts.append("# Upstream results\n" + "\n".join(ups))
    crit = node.get("acceptance_criteria") or []
    parts.append(
        "# Acceptance criteria\n"
        + "\n".join(f"{i}. {c}" for i, c in enumerate(crit, 1))
    )
    if attempt > 1:
        findings = (node.get("verdict") or {}).get("findings") or []
        if findings:
            fl = "\n".join(
                f"- [{f.get('severity')}] {f.get('issue')} — fix: {f.get('fix')}"
                for f in findings
                if f.get("severity") in ("blocker", "major")
            )
            if fl:
                parts.append(f"# Prior critic findings (must fix)\n{fl}")
    parts.append(
        "# Rules\n"
        "- Skip formatters, linters, and project-wide test suites; run only the "
        "commands the acceptance criteria name.\n"
        "- Finish with a `## Evidence` section listing, per criterion, the command "
        "you ran and its observed result.\n"
        "- List every file you changed."
    )
    return "\n\n".join(parts)


def _critic_prompt(dag: dict, node: dict, out: str) -> str:
    files = node.get("files") or []
    crit = node.get("acceptance_criteria") or []
    parts = [
        f"# Node {node['id']} — {node.get('title', '')}",
        f"# Task\n{node.get('task', '')}",
        "# Files the worker was allowed to edit\n"
        + ("\n".join(f"- {f}" for f in files) if files else "- (not restricted)"),
        "# Acceptance criteria\n"
        + "\n".join(f"{i}. {c}" for i, c in enumerate(crit, 1)),
        f"# Worker report\n{out}",
        "# Instructions\n"
        "- Independently verify each criterion: run the named commands, read the "
        "changed files, check for edits outside the owned files "
        "(`git status --porcelain` / `git diff --stat` are allowed).\n"
        "- verdict \"approve\" only if all criteria hold with evidence and you "
        "have no blocker or major finding.\n"
        f"- Use node_id = \"{node['id']}\" in every finding.",
    ]
    return "\n\n".join(parts)


def _persist(dag: dict, state_path: str) -> None:
    write(state_path, json.dumps(dag, indent=2))


async def _wait_handle(h, timeout: float = 0.0):
    """Await a subagent handle's result.

    `await h` (the __await__ path) hits omp's /v1/tool bridge without the
    session/run/name context in some kernel states and raises
    `RuntimeError: Missing session/run/name`. The polling path
    (`h.done()` + `h.wait()`) does not, so fall back to it on that error.
    """
    try:
        return await h
    except RuntimeError as e:
        if "Missing session/run/name" not in str(e):
            raise
        log(f"handle await failed ({e}); falling back to polling")
        while not h.done():
            await asyncio.sleep(2)
        return h.wait(timeout)


async def run_dag(
    dag: dict,
    *,
    state_path: str,
    max_attempts: int = 2,
    isolated: bool = False,
    upstream_chars: int = 4000,
) -> dict:
    phase(f"dag: {dag.get('goal', '')}")
    dag.setdefault("isolated", isolated)
    nodes = dag["nodes"]
    by_id = {n["id"]: n for n in nodes}
    events = {nid: asyncio.Event() for nid in by_id}
    for n in nodes:
        st = n.get("status")
        if st == "done":
            events[n["id"]].set()
        elif st in ("blocked", "skipped"):
            if dag.get("retry_blocked"):
                n["status"] = "pending"
                n["blocked_reason"] = None
                _persist(dag, state_path)
            else:
                events[n["id"]].set()  # remains failed; dependents will skip

    async def run_node(node: dict):
        nid = node["id"]
        try:
            st = node.get("status")
            if st == "done":
                events[nid].set()
                return
            if st in ("blocked", "skipped") and not dag.get("retry_blocked"):
                events[nid].set()
                return
            for dep in node.get("depends_on") or []:
                await events[dep].wait()
            dep_statuses = {by_id[d].get("status") for d in node.get("depends_on") or []}
            if dep_statuses - {"done"}:
                node["status"] = "skipped"
                node["blocked_reason"] = "dependency " + ", ".join(
                    sorted(d for d in node.get("depends_on") or [] if by_id[d].get("status") != "done")
                ) + " not done"
                log(f"{nid} skipped")
                events[nid].set()
                _persist(dag, state_path)
                return
            node.setdefault("attempts", 0)
            node.setdefault("worker_ids", [])
            node.setdefault("critic_ids", [])
            out = ""
            verdict = None
            for attempt in range(1, max_attempts + 1):
                prompt = _worker_prompt(dag, node, attempt, upstream_chars)
                h = agent(prompt, agent=node.get("agent", "task"), label=nid, isolated=isolated)
                node["status"] = "running"
                node["attempts"] = attempt
                node["worker_ids"].append(h.id)
                _persist(dag, state_path)
                out = str(await _wait_handle(h))
                c = agent(
                    _critic_prompt(dag, node, out),
                    agent="critic",
                    label=f"critic:{nid}",
                    schema=CRITIC_SCHEMA,
                )
                node["status"] = "review"
                node["critic_ids"].append(c.id)
                _persist(dag, state_path)
                verdict = await _wait_handle(c)
                if isinstance(verdict, str):
                    try:
                        verdict = json.loads(verdict)
                    except (ValueError, TypeError):
                        verdict = dict(UNPARSEABLE_VERDICT)
                node["verdict"] = verdict
                if verdict.get("verdict") == "approve":
                    node["status"] = "done"
                    node["evidence"] = out[-upstream_chars:]
                    node.pop("blocked_reason", None)
                    log(f"{nid} done")
                    events[nid].set()
                    _persist(dag, state_path)
                    return
                log(f"{nid} revise (attempt {attempt}/{max_attempts})")
                _persist(dag, state_path)
            node["status"] = "blocked"
            node["blocked_reason"] = json.dumps(
                (verdict or UNPARSEABLE_VERDICT).get("findings", [])
            )
            log(f"{nid} blocked after {max_attempts} attempts")
            events[nid].set()
            _persist(dag, state_path)
        except Exception as e:  # noqa: BLE001 — one node must never hang the DAG
            node["status"] = "blocked"
            node["blocked_reason"] = f"{type(e).__name__}: {e}"
            log(f"{nid} failed: {e}")
            events[nid].set()
            _persist(dag, state_path)

    await asyncio.gather(*(run_node(n) for n in nodes), return_exceptions=True)
    return dag


def summarize(dag: dict) -> str:
    lines = ["| id | status | attempts | critic verdict | evidence |", "|---|---|---|---|---|"]
    for n in dag.get("nodes") or []:
        verdict = (n.get("verdict") or {}).get("verdict", "-")
        ev = (n.get("evidence") or n.get("blocked_reason") or "").replace("\n", " ")[:120]
        lines.append(
            f"| {n.get('id')} | {n.get('status')} | {n.get('attempts', '-')} | {verdict} | {ev} |"
        )
    return "\n".join(lines)
