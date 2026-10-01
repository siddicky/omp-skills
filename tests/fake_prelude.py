"""Scriptable stand-in for the omp eval prelude.

`make_namespace(host)` builds a globals dict holding fake `agent()`, `log`, `phase`,
`display` and `judge_batch`, then execs skills/dag/runner.py and judgments.py into it
exactly as the dag skill does inside the kernel.

Behaviour is scripted per label (workers use the node id, critics `critic:<id>`):
each spawn takes the next entry; when none are left the default applies (an
approving critic, a worker that reports one passing piece of evidence).
"""

import os
import re
import threading

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAG_DIR = os.path.join(REPO, "skills", "dag")

_AGENT_KWARGS = {"agent", "label", "schema", "schema_mode", "isolated", "apply", "merge", "tools"}
_CODE = {}


# --- behaviours ---------------------------------------------------------------


class Ret:
    """The job completes and wait() returns `value` (may be None, a list, a str, a dict)."""

    def __init__(self, value, delay=0.0):
        self.value, self.delay = value, delay


class Fail:
    """The job fails: wait() raises RuntimeError(message), like a failed omp job."""

    def __init__(self, message, delay=0.0):
        self.message, self.delay = message, delay


class Raise:
    """wait() raises this exact exception (for example ValueError from json.loads)."""

    def __init__(self, exc, delay=0.0):
        self.exc, self.delay = exc, delay


class Hang:
    """The job never finishes on its own; cancel() ends it with RuntimeError('Cancelled by user')."""

    delay = 0.0


class FailSpawn:
    """agent() itself raises RuntimeError(message): the subagent never starts."""

    def __init__(self, message):
        self.message = message


def worker_ok(files=(), summary="done", n=1):
    """A well-formed worker result with `n` passing evidence entries."""
    return {
        "status": "done",
        "summary": summary,
        "files_changed": list(files),
        "evidence": [
            {"criterion": i, "command": f"check {i}", "observed": f"ok {i}", "passed": True}
            for i in range(1, n + 1)
        ],
        "notes": "",
    }


def approve(summary="looks good", findings=()):
    return {"verdict": "approve", "summary": summary, "findings": list(findings)}


def revise(*findings, summary="needs work"):
    return {"verdict": "revise", "summary": summary, "findings": list(findings)}


def finding(severity="major", issue="something is wrong", fix="fix it", target=None, **extra):
    out = {"severity": severity, "issue": issue, "fix": fix, **extra}
    if target is not None:
        out["target"] = target
    return out


# --- jobs and handles -----------------------------------------------------------


class Call:
    """One successful agent() spawn."""

    def __init__(self, prompt, kwargs, job_id):
        self.prompt, self.kwargs, self.job_id = prompt, kwargs, job_id
        self.label = kwargs.get("label")
        self.agent = kwargs.get("agent")
        self.schema = kwargs.get("schema")


class FakeHandle:
    """Same surface as the prelude's AgentHandle: id, done(), wait(), cancel()."""

    def __init__(self, host, job_id, behaviour):
        self.host, self.id, self.behaviour = host, job_id, behaviour
        self.cancelled = False
        self.outcome = None
        self._cancel = threading.Event()
        self._done = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True, name=f"fake-job-{job_id}")

    def _run(self):
        beh = self.behaviour
        try:
            if isinstance(beh, Hang):
                self._cancel.wait()
            elif beh.delay:
                self._cancel.wait(beh.delay)
            if self.cancelled:
                outcome = ("err", RuntimeError("Cancelled by user"))
            elif isinstance(beh, Fail):
                outcome = ("err", RuntimeError(beh.message))
            elif isinstance(beh, Raise):
                outcome = ("err", beh.exc)
            else:
                outcome = ("ok", beh.value)
        finally:
            self.host._job_finished(self)
        self.outcome = outcome
        self._done.set()

    def done(self):
        return self._done.is_set()

    def wait(self, timeout=None):
        if not self._done.wait(timeout):
            raise TimeoutError(f"agent handle {self.id} is still running")
        kind, value = self.outcome
        if kind == "err":
            raise value
        return value

    def cancel(self):
        if self._done.is_set():
            return False
        self.cancelled = True
        self.host.cancelled.append(self.id)
        self._cancel.set()
        return True


# --- judge_batch ------------------------------------------------------------------


class FakeItem:
    """A settled judge_batch item: ok when error is None."""

    def __init__(self, key, answers=None, error=None, model="typesafe/jev-test"):
        self.key, self.answers, self.error, self.model = key, answers, error, model

    @property
    def ok(self):
        return self.error is None


class FakeBatch:
    def __init__(self, host, states, questions, options):
        self.host, self.states, self.questions, self.options = host, states, questions, options
        self.closed = False

    async def drain_iter(self, timeout=None):
        self.host.drain_timeouts.append(timeout)
        if self.host.drain_error is not None:
            raise self.host.drain_error
        for key in list(self.states):
            yield key, self.host.judge_item(key, self.states[key], self.questions)

    def close(self):
        self.closed = True


def default_answers(questions):
    out = {}
    for qid, q in questions.items():
        if q["type"] == "bool":
            out[qid] = {"type": "bool", "bool": 0.95}
        elif q["type"] == "choice":
            label = next(iter(q["criteria"]))
            out[qid] = {"type": "choice", "choice": label, "probabilities": {label: 0.99}, "confidence": 0.99}
    return out


# --- the host -----------------------------------------------------------------------


class FakeHost:
    def __init__(self):
        self.scripts = {}
        self.calls = []
        self.spawn_errors = []
        self.cancelled = []
        self.handles = []
        self.logs = []
        self.phases = []
        self.displays = []
        self.running = 0
        self.peak = 0
        self.reject_kwarg = None  # (key, message): agent() raises when that kwarg is passed
        self._lock = threading.Lock()
        self._used = {}
        # judge_batch
        self.judge_batches = []
        self.judge_model = "typesafe/jev-test"
        self.judge_answers = None  # callable(key, state, questions) -> answers dict or FakeItem
        self.judge_create_error = None
        self.drain_error = None
        self.drain_timeouts = []

    # scripting
    def script(self, label, *behaviours):
        """Queue behaviours for `label`; a behaviour can also be a callable(Call) -> behaviour."""
        self.scripts.setdefault(label, []).extend(behaviours)

    def spawns(self, label):
        return [c for c in self.calls if c.label == label]

    def prompts(self, label):
        return [c.prompt for c in self.spawns(label)]

    def worker_calls(self):
        return [c for c in self.calls if c.agent != "critic"]

    def critic_calls(self):
        return [c for c in self.calls if c.agent == "critic"]

    # prelude surface
    def log(self, message):
        self.logs.append(str(message))

    def phase(self, title):
        self.phases.append(str(title))

    def display(self, value):
        self.displays.append(value)

    def agent(self, prompt, **kwargs):
        unknown = set(kwargs) - _AGENT_KWARGS
        if unknown:
            raise TypeError(f"agent() got an unexpected keyword argument {sorted(unknown)[0]!r}")
        label = kwargs.get("label")
        if self.reject_kwarg and self.reject_kwarg[0] in kwargs:
            self.spawn_errors.append((label, self.reject_kwarg[1]))
            raise RuntimeError(self.reject_kwarg[1])
        queue = self.scripts.get(label)
        entry = queue.pop(0) if queue else None
        pre = Call(prompt, kwargs, None)
        if callable(entry) and not isinstance(entry, (Ret, Fail, Raise, Hang, FailSpawn)):
            entry = entry(pre)
        if entry is None:
            entry = self._default(kwargs)
        if isinstance(entry, FailSpawn):
            self.spawn_errors.append((label, entry.message))
            raise RuntimeError(entry.message)
        job_id = self._job_id(label)
        handle = FakeHandle(self, job_id, entry)
        call = Call(prompt, kwargs, job_id)
        with self._lock:
            self.calls.append(call)
            self.handles.append(handle)
            self.running += 1
            self.peak = max(self.peak, self.running)
        handle.thread.start()
        return handle

    def _default(self, kwargs):
        if kwargs.get("agent") == "critic":
            return Ret(approve())
        return Ret(worker_ok())

    def _job_id(self, label):
        base = re.sub(r"[^A-Za-z0-9_-]+", "", label or "") or "agent"
        with self._lock:
            n = self._used.get(base, 0) + 1
            self._used[base] = n
        return base if n == 1 else f"{base}-{n}"

    def _job_finished(self, handle):
        with self._lock:
            self.running -= 1

    def judge_batch(self, states, questions, **options):
        if self.judge_create_error is not None:
            raise self.judge_create_error
        if isinstance(states, (list, tuple)):
            states = dict(enumerate(states))
        batch = FakeBatch(self, dict(states), questions, options)
        self.judge_batches.append(batch)
        return batch

    def judge_item(self, key, state, questions):
        if self.judge_answers is not None:
            made = self.judge_answers(key, state, questions)
            if isinstance(made, FakeItem):
                return made
            return FakeItem(key, answers=made, model=self.judge_model)
        return FakeItem(key, answers=default_answers(questions), model=self.judge_model)

    # cleanup
    def close(self):
        """Cancel every unfinished job so no executor thread is left blocked in wait()."""
        for handle in self.handles:
            handle.cancel()
        for handle in self.handles:
            handle.thread.join(timeout=5)


# --- loading the skill code -----------------------------------------------------------


def _compiled(name):
    if name not in _CODE:
        path = os.path.join(DAG_DIR, name)
        with open(path, encoding="utf-8") as f:
            _CODE[name] = compile(f.read(), path, "exec")
    return _CODE[name]


def make_namespace(host, *, judgments=True, judge=True):
    """A kernel-like globals dict with runner.py (and judgments.py) exec'd into it."""
    ns = {
        "__name__": "dag_ns",
        "agent": host.agent,
        "log": host.log,
        "phase": host.phase,
        "display": host.display,
    }
    if judge:
        ns["judge_batch"] = host.judge_batch
    exec(_compiled("runner.py"), ns)
    if judgments:
        exec(_compiled("judgments.py"), ns)
    return ns


# --- dag builders -----------------------------------------------------------------------


def node(nid, deps=(), files=None, criteria=None, **extra):
    """A valid node owning `<id>.txt` unless `files` says otherwise."""
    return {
        "id": nid,
        "title": f"Title {nid}",
        "task": f"Do {nid}",
        "acceptance_criteria": list(criteria) if criteria is not None else [f"`check {nid}` exits 0"],
        "depends_on": list(deps),
        "files": [f"{nid}.txt"] if files is None else list(files),
        "status": "pending",
        **extra,
    }


def make_dag(*nodes, approved=True, **extra):
    return {"goal": "test goal", "approved": approved, "nodes": list(nodes), **extra}
