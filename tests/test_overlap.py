"""paths_overlap: the audit's cases (D1, ts-10, X-05, M3), the normalization rules, case folding of classes
(R2-01), and brute-force cross-checks against independent glob matchers."""

import fnmatch
import functools
import itertools
import os
import random
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fake_prelude  # noqa: E402

NS = fake_prelude.make_namespace(fake_prelude.FakeHost())
overlap = NS["paths_overlap"]

# (a, b, expected). Every row is checked in both argument orders.
TABLE = [
    # D1 false negatives: real overlaps the old fnmatch check missed
    ("src/**/x.ts", "src/a/*.ts", True),
    ("src/*/x.ts", "src/a/*.ts", True),
    ("src/a*.ts", "src/*b.ts", True),
    ("**/x.ts", "x.ts", True),
    ("src/**/x.ts", "src/x.ts", True),
    ("src/", "src/a.ts", True),
    ("src", "src/a.ts", True),
    ("lib/util", "lib/util/x.ts", True),
    ("./a", "a", True),
    ("src//a.ts", "src/a.ts", True),
    # D1 false positives: provably disjoint globs the old check rejected
    ("src/*.ts", "src/*.py", False),
    ("src/**/*.ts", "src/**/*.md", False),
    ("packages/*/package.json", "packages/*/README.md", False),
    ("docs/*.md", "docs/*.txt", False),
    ("tests/test_*.py", "tests/test_*.js", False),
    # D1 verifier: realistic planner output
    ("src/**/*.ts", "src/auth/**", True),
    ("src/**/*.ts", "src/index.ts", True),
    ("src/auth", "src/auth/login.ts", True),
    ("src/auth/", "src/auth/login.ts", True),
    ("./src/index.ts", "src/index.ts", True),
    ("src/*.ts", "src/*.css", False),
    ("tests/*.test.ts", "tests/*.spec.ts", False),
    # ts-10
    ("src/api/*.ts", "src/api/*.md", False),
    ("src/auth/", "src/auth/login.ts", True),
    ("src/{a,b}.ts", "src/a.ts", True),
    ("src/{a,b}.ts", "src/c.ts", False),
    ("SRC/a.ts", "src/a.ts", True),
    ("src/a.ts", "src/../src/a.ts", True),
    # X-05 and M3
    ("src/**/*.ts", "src/foo.ts", True),
    ("src/foo.ts", "SRC/foo.ts", True),
    ("src/*.ts", "src/*.md", False),
    ("packages/**/*.py", "packages/a.py", True),
    ("src/**/test_*.py", "src/test_x.py", True),
    ("**/*.md", "README.md", True),
    ("docs/**", "docs/a.md", True),
    ("src/a/**/*.ts", "src/b/x.ts", False),
    ("src/**/*.ts", "lib/x.ts", False),
    # plain distinctions that must stay disjoint
    ("src/a.ts", "src/b.ts", False),
    ("src/auth", "src/authz/x.ts", False),
    ("src/a.ts", "src/a.tsx", False),
    ("docs/", "src/a.ts", False),
    ("a/b/", "a/b.ts", False),
    ("src/a.ts", "src/a.ts", True),
    ("Makefile", "Makefile", True),
    ("Makefile", "makefile", True),
    # character classes, ?, ranges, negation. Names that differ only in case are one file (macOS), so a
    # negated class takes a letter whose other case it does not list: [!a] matches "A", and "A" is "a".
    ("src/[ab].ts", "src/a.ts", True),
    ("src/[ab].ts", "src/c.ts", False),
    ("src/[!a].ts", "src/a.ts", True),
    ("src/[!a].ts", "src/b.ts", True),
    ("src/[^a].ts", "src/a.ts", True),
    ("src/[!aA].ts", "src/a.ts", False),
    ("src/[!0].ts", "src/0.ts", False),
    ("src/[a-c].ts", "src/d.ts", False),
    ("src/[a-c].ts", "src/b.ts", True),
    ("src/[a-c].ts", "src/[c-e].ts", True),
    ("src/[a-b].ts", "src/[c-e].ts", False),
    ("src/a?.ts", "src/abc.ts", False),
    ("src/a?.ts", "src/ab.ts", True),
    ("src/?.ts", "src/ab.ts", False),
    ("src/f?.ts", "src/f.*", True),  # witness f..ts
    # braces
    ("src/{a,b}/**", "src/b/x", True),
    ("src/{a,b}/**", "src/c/x", False),
    ("src/{a,{b,c}}.ts", "src/c.ts", True),
    ("src/{a,b}.{ts,md}", "src/b.md", True),
    ("src/{a,b}.{ts,md}", "src/b.py", False),
    # a slash-less glob may mean "at any depth"; a literal file name is the root file only
    ("*.md", "src/README.md", True),
    ("*.md", "src/a.ts", False),
    ("README.md", "docs/README.md", False),
    # dir-like entries (last segment glob-free, no dot) are the path and everything below
    (".github", ".github/workflows/ci.yml", True),
    ("packages/*/src", "packages/a/src/x.ts", True),
    ("src/auth", "src/auth", True),
    # a dotted last segment looks like a file but may be a directory (audit adversarial-review M2): it
    # overlaps whatever the other pattern spells out below it
    ("packages/ui.kit", "packages/ui.kit/src/button.tsx", True),
    ("docs/v1.0", "docs/v1.0/index.md", True),
    ("src/app.module", "src/app.module/x.ts", True),
    ("socket.io", "socket.io/lib/x.js", True),
    ("packages/ui.kit", "packages/ui.kit/**/*.ts", True),
    ("packages/ui.kit", "packages/*/src/x.ts", True),
    ("packages/*/ui.kit", "packages/a/ui.kit/src/x.ts", True),
    ("{docs,site}/v1.0", "site/v1.0/a.md", True),
    ("DOCS\\V1.0", "docs/v1.0/a.md", True),
    (".eslintrc.json", ".eslintrc.json/x", True),
    ("packages/ui.kit", "packages/ui.kit", True),
    # ... but a broad glob does not turn every dotted file into a directory, and near misses stay disjoint
    ("package.json", "**/*.md", False),
    ("src/index.ts", "*.test.ts", False),
    ("pyproject.toml", "**/*.py", False),
    ("src/main.rs", "src/**/*.md", False),
    ("packages/ui.kit", "packages/**/button.tsx", False),
    ("packages/ui.kit", "packages/ui.kit.bak/x.ts", False),
    ("packages/ui.kit", "packages/other/src/button.tsx", False),
    ("docs/v1.0", "docs/v1.1/index.md", False),
    ("docs/v1.0", "docs/v1/0/index.md", False),
    ("a/b.ts", "a/b/c.ts", False),
    # the whole tree
    (".", "src/a.ts", True),
    ("**", "src/a.ts", True),
    ("**/*", "x", True),
    # normalization
    ("src\\a.ts", "src/a.ts", True),
    ("./src/./a.ts", "src/a.ts", True),
    ("src/x/../a.ts", "src/a.ts", True),
    # case, audit adversarial-review R2-01: a class is read as written and then closed under case, never
    # read after lower-casing the pattern. Each of these overlaps under a case-sensitive match too.
    ("docs/[!a-z]*.md", "docs/README.md", True),
    ("[!a-z].md", "B.md", True),
    ("[A-z].md", "_.md", True),
    ("[A-z].md", "[.md", True),
    ("[A-z].md", "`.md", True),
    ("[A-z].md", "^.md", True),
    ("[A-z].md", "7.md", False),
    ("[!A-Z].md", "b.md", True),
    ("[!A-Z].md", "7.md", True),
    ("[!a-z].md", "7.md", True),
    # ... and the case-insensitive side: a range of one case takes the letters of the other
    ("[A-Z].md", "a.md", True),
    ("[a-z].md", "A.md", True),
    ("[A-C].md", "b.md", True),
    ("[A-C].md", "d.md", False),
    ("README.md", "readme.md", True),
    ("docs/README.md", "DOCS/readme.MD", True),
    ("[R-T]*.md", "readme.md", True),
    ("[!a-z0-9].md", "7.md", False),
    ("[!a-z0-9].md", "_.md", True),
    ("[!a-zA-Z].md", "B.md", False),
    ("[!a-zA-Z].md", "_.md", True),
    ("[!a-zA-Z].md", "\u212a.md", True),  # the Kelvin sign is not in a-zA-Z, and it is a "k" to the file system
    ("[!a-zA-Z].md", "k.md", True),
    ("[a-zA-Z].md", "[!a-zA-Z].md", True),  # so even these two meet, on that one character
    ("[k].md", "\u212a.md", True),
    ("s.md", "\u017f.md", True),  # long s
    ("\u00e9.md", "\u00c9.md", True),
    ("[!\u03c3\u03a3].md", "\u03c3.md", True),  # final sigma is outside the class and is a sigma
    ("\u00df.md", "SS.md", False),  # one-character folds only: a sharp s is not "ss"
    ("[" + chr(1) + "-" + chr(0x10FFFF) + "].md", "a.md", True),  # a range that wide is cheap to fold
    ("[!" + chr(1) + "-" + chr(0x10FFFF) + "].md", "a.md", False),
    # a class that takes nothing, or only "/", which no segment holds: it overlaps nothing, not even a bare "*"
    ("[!" + chr(0) + "-" + chr(0x10FFFF) + "].md", "*.md", False),
    ("[!" + chr(0) + "-." + "0-" + chr(0x10FFFF) + "].md", "*.md", False),
    # braces: a group without a comma is literal text, an unbalanced one too, and a group that explodes is unsure
    ("src/{a}.ts", "src/{a}.ts", True),
    ("src/{a}.ts", "src/a.ts", False),
    ("src/{a,b.ts", "src/a.ts", False),
    ("src/{a,b.ts", "src/{a,b.ts", True),
    ("{a,b}/{c}/{d,e}.ts", "b/{c}/e.ts", True),
    ("{a,b}/{c}/{d,e}.ts", "b/c/e.ts", False),
    ("{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}x", "zzz", True),
    # when unsure: True
    ("src/[z-a].ts", "src/q.ts", True),
    ("src/[[:alpha:]].ts", "src/q.ts", True),
    ("/abs/a.ts", "src/a.ts", True),
    ("/abs/a.ts", "/abs/b.ts", False),
]


class OverlapTable(unittest.TestCase):
    def test_table(self):
        bad = []
        for a, b, expected in TABLE:
            for x, y in ((a, b), (b, a)):
                got = overlap(x, y)
                if got is not expected:
                    bad.append(f"paths_overlap({x!r}, {y!r}) = {got}, expected {expected}")
        self.assertEqual(bad, [])

    def test_a_dotdot_beside_a_globstar_is_unsure_when_overlap_is_asked_directly(self):
        # audit tests-prelude V3-01. "**" may match no directory, so src/**/../../x can be ../x and normpath cannot
        # say where it points. validate_dag refuses these entries (test_validate), so only a direct call reaches the
        # guard in paths_overlap, and there the answer is "unsure", which is True, whatever the other entry is.
        for entry in ("src/**/../../x", "**/../x", "a/**/..", "src/{a,**}/../x"):
            self.assertIs(overlap(entry, "y"), True, entry)
            self.assertIs(overlap("y", entry), True, entry)
        # without a "**" the same ".." is resolved, so these stay exact
        self.assertIs(overlap("src/lib/../x", "y"), False)
        self.assertIs(overlap("src/lib/../x", "src/x"), True)

    def test_a_dotted_directory_is_caught_when_a_pattern_goes_below_it(self):
        # audit adversarial-review M2: two concurrent nodes working inside one dotted directory
        self.assertIs(overlap("packages/ui.kit", "packages/ui.kit/src/button.tsx"), True)
        self.assertIs(overlap("packages/ui.kit/src/button.tsx", "packages/ui.kit"), True)

    def test_a_dotted_file_is_not_a_directory_for_a_broad_glob(self):
        for file_name in ("package.json", "src/index.ts", "pyproject.toml", "docs/guide.v2.md"):
            for glob in ("**/*.md", "*.test.ts", "**/*.py", "**/x"):
                if (file_name, glob) == ("docs/guide.v2.md", "**/*.md"):
                    continue  # this one really is Markdown
                self.assertIs(overlap(file_name, glob), False, (file_name, glob))

    def test_never_raises_and_non_strings_are_unsure(self):
        self.assertIs(overlap(None, "a"), True)
        self.assertIs(overlap("a", 3), True)
        self.assertIs(overlap("", "a.ts"), True)  # blank is the repo root
        self.assertIs(overlap("{a,b}" * 12, "zzz"), True)  # brace explosion is undecidable

    def test_a_caret_negates_a_class_like_a_bang(self):
        self.assertIs(overlap("[^0-9].ts", "5.ts"), False)
        self.assertIs(overlap("[^0-9].ts", "x.ts"), True)
        self.assertIs(overlap("[^aA].ts", "a.ts"), False)

    def test_a_trailing_hyphen_in_a_class_is_a_member_not_a_range(self):
        self.assertIs(overlap("[a-].ts", "-.ts"), True)
        self.assertIs(overlap("[a-].ts", "b.ts"), False)

    def test_a_leading_dot_does_not_make_a_name_a_file(self):
        # .github and .config are directory names, unlike package.json
        self.assertIs(overlap(".github", "**/workflows/ci.yml"), True)
        self.assertIs(overlap(".config", "**/*.toml"), True)

    def test_three_stars_are_a_globstar(self):
        self.assertIs(overlap("src/***/x.ts", "src/a/b/x.ts"), True)
        self.assertIs(overlap("src/***/x.ts", "lib/a/b/x.ts"), False)

    def test_entries_are_stripped_of_surrounding_whitespace(self):
        self.assertIs(overlap(" a.ts ", "a.ts"), True)
        self.assertIs(overlap("src/a.ts\t", "src/a.ts"), True)

    def test_an_internal_failure_is_unsure_not_disjoint(self):
        def boom(entry):
            raise RuntimeError("internal failure")

        self.addCleanup(NS.__setitem__, "_variants", NS["_variants"])
        NS["_variants"] = boom
        self.assertIs(overlap("a.ts", "b.ts"), True)

    def test_a_slashless_glob_with_a_trailing_slash_matches_at_any_depth(self):
        self.assertIs(overlap("build*/", "packages/app/build-out/x.js"), True)
        self.assertIs(overlap("build*/", "packages/app/src/x.js"), False)


# --- independent oracle ----------------------------------------------------------------


def _segment_regex(seg):
    out, i = [], 0
    while i < len(seg):
        c = seg[i]
        if c == "*":
            out.append(".*")
        elif c == "?":
            out.append(".")
        elif c == "[":
            j = i + 1
            if j < len(seg) and seg[j] in "!^":
                j += 1
            if j < len(seg) and seg[j] == "]":
                j += 1
            while j < len(seg) and seg[j] != "]":
                j += 1
            if j >= len(seg):
                out.append(re.escape(c))
            else:
                body = seg[i + 1 : j]
                negate = body[:1] in ("!", "^")
                out.append("[" + ("^" if negate else "") + (body[1:] if negate else body) + "]")
                i = j
        else:
            out.append(re.escape(c))
        i += 1
    return re.compile("".join(out) + r"\Z", re.S)


@functools.lru_cache(maxsize=None)
def _segment_matches(pattern, segment):
    """Case-insensitively, as on macOS: does some upper/lower-case spelling of `segment` match the glob
    `pattern` (read as written, case-sensitively)? Spelling out every case is the definition, not a shortcut."""
    regex = _segment_regex(pattern)
    spellings = itertools.product(*[sorted({c.lower(), c.upper()}) for c in segment])
    return any(regex.match("".join(s)) for s in spellings)


def matches(pattern, path):
    """Does the path (tuple of segments) match the glob, ignoring case? "**" is zero or more segments."""
    psegs = tuple(pattern.split("/"))

    @functools.lru_cache(maxsize=None)
    def go(i, j):
        if i == len(psegs):
            return j == len(path)
        if psegs[i] == "**":
            return go(i + 1, j) or (j < len(path) and go(i, j + 1))
        return j < len(path) and _segment_matches(psegs[i], path[j]) and go(i + 1, j + 1)

    return go(0, 0)


SEGMENTS = [
    "a", "b", "f", "g", "ff", "fg", "af", "ab",
    "f.ts", "g.ts", "ff.ts", "fg.ts", "a.ts", "ab.ts", "f.md", "g.md", "af.md", ".ts", "f..ts",
]  # fmt: skip
UNIVERSE = [p for depth in (1, 2, 3) for p in itertools.product(SEGMENTS, repeat=depth)]

# Patterns the normalizer leaves alone: lowercase, no braces, a "/" (or no glob), and a last
# segment that has a "." or a wildcard, so the only special rule the oracle needs is the one for
# a glob-free dotted last segment (see `below`).
PLAIN = [
    "a/f.ts", "a/g.ts", "b/f.ts", "a/f.md", "f.ts", "g.ts", "a/b/f.ts", "a/b/g.ts",
    "a/*.ts", "a/*.md", "b/*.ts", "*/f.ts", "*/*.ts", "a/*/f.ts", "a/*/*.ts",
    "a/**", "b/**", "**/f.ts", "**/*.ts", "**/*.md", "a/**/f.ts", "a/**/*.ts", "a/**/b/**", "**/a/**", "**",
    "a/f*", "a/*f.ts", "a/f?.ts", "a/?.ts", "a/??.ts", "a/[fg].ts", "a/[!f].ts", "a/[f-g].ts", "a/[ab]/f.ts",
    "a/f.*", "a/*.t?", "a/*.[tm]*", "a/a*b.ts", "a/*", "*/*", "**/*", "a/b/**", "**/b/*.ts", "a/**/*",
    "**/f.*", "a/?/*.ts", "a/[!a-f].ts",
]  # fmt: skip


def _random_patterns(count, seed):
    rng = random.Random(seed)
    dirs = ["a", "b", "*", "**", "?", "[ab]", "a*", "[!a]"]
    files = ["f.ts", "*.ts", "f.*", "*", "f?.ts", "[fg].ts", "*.md", "g.md", "**", "*f.ts"]
    out = set()
    while len(out) < count:
        out.add("/".join([rng.choice(dirs) for _ in range(rng.randint(1, 2))] + [rng.choice(files)]))
    return sorted(out)


TAILS = [p for depth in (1, 2) for p in itertools.product(SEGMENTS, repeat=depth)]


def maybe_dir(pattern):
    """A glob-free last segment with a dot: looks like a file, may be a directory."""
    last = pattern.split("/")[-1]
    return not any(c in last for c in "*?[") and "." in last.lstrip(".")


def below(x, y):
    """The dotted-name rule, stated independently of the implementation: y spells out something under x
    when each of its leading segments shares a witness segment with x's segment at the same position
    ("**" never stands in for one) and a non-empty tail below that matches the rest of y."""
    xs, ys = x.split("/"), y.split("/")
    k = len(xs)
    if "**" in xs or len(ys) <= k or "**" in ys[:k]:
        return False
    if not all(any(_segment_matches(xs[i], s) and _segment_matches(ys[i], s) for s in SEGMENTS) for i in range(k)):
        return False
    rest = "/".join(ys[k:])
    return any(matches(rest, tail) for tail in TAILS)


class OverlapBruteForce(unittest.TestCase):
    """Enumerate candidate witness paths and compare with paths_overlap."""

    def check(self, patterns, label):
        matched = {p: frozenset(u for u in UNIVERSE if matches(p, u)) for p in patterns}
        missed, spurious = [], []
        for a, b in itertools.combinations_with_replacement(patterns, 2):
            witness = not matched[a].isdisjoint(matched[b])
            witness = witness or (maybe_dir(a) and below(a, b)) or (maybe_dir(b) and below(b, a))
            got = overlap(a, b)
            if witness and not got:
                missed.append((a, b))  # a false negative would let two workers edit one file
            if got and not witness:
                spurious.append((a, b))
        self.assertEqual(missed, [], f"{label}: false negatives (witness path exists)")
        self.assertEqual(spurious, [], f"{label}: reported overlap without any witness path")

    def test_broad_pattern_table(self):
        self.check(PLAIN, "table")

    def test_seeded_random_patterns(self):
        self.check(_random_patterns(90, seed=20260930), "random")


# --- case folding (audit adversarial-review R2-01) --------------------------------------------
#
# Oracle: stdlib fnmatch reads a pattern exactly as written and case-sensitively, so two patterns overlap
# when a name one matches equals, ignoring case, a name the other matches. Names are enumerated in every
# upper/lower-case spelling, so nothing here assumes how the implementation folds case.


def spellings(name):
    return ["".join(s) for s in itertools.product(*[sorted({c.lower(), c.upper()}) for c in name])]


@functools.lru_cache(maxsize=None)
def _fn(name, pattern):
    return fnmatch.fnmatchcase(name, pattern)


def exact_matches(pattern, path):
    """Case-sensitive: does the path (tuple of names) match the glob? "**" is zero or more names."""
    psegs = pattern.split("/")

    def go(i, j):
        if i == len(psegs):
            return j == len(path)
        if psegs[i] == "**":
            return go(i + 1, j) or (j < len(path) and go(i, j + 1))
        return j < len(path) and _fn(path[j], psegs[i]) and go(i + 1, j + 1)

    return go(0, 0)


BASES = [
    "a", "b", "c", "z", "k", "s", "_", "[", "]", "0", "7", "ab", "a.md", "b.md", "c.md", "z.md", "k.md", "s.md", "_.md",
    "[.md", "].md", "0.md", "rd.md", "dc",
]  # fmt: skip
# The Kelvin sign and the long s are not upper/lower-case spellings of "k" and "s", but a case-insensitive file
# system folds them into those letters, so a name written with either is a witness too.
EXOTIC = ["\u212a", "\u017f", "\u212a.md", "\u017f.md"]
NAMES = sorted({s for base in BASES for s in spellings(base)} | set(EXOTIC))
CASE_UNIVERSE = [(n,) for n in NAMES] + [(x, y) for x in NAMES for y in NAMES]
# Every pattern has a "/", so none is read as "at any depth", and none ends in a glob-free dotted name that
# another pattern goes below: the only rule the oracle needs is "**".
CASE_PATTERNS = [
    "dc/" + tail
    for tail in (
        "a.md", "A.md", "B.md", "_.md", "[.md", "0.md", "rd.md", "RD.md", "*.md", "?.md", "[ab].md", "[AB].md",
        "[a-b].md", "[A-B].md", "[B-C].md", "[a-z].md", "[A-Z].md", "[A-z].md", "[!a-z].md", "[!A-Z].md",
        "[!a-zA-Z].md", "[!A].md", "[!0-9].md", "[0-9].md", "[!_].md", "[!a-z0-9].md", "[!a-z]*.md", "[A-Z]*", "*",
        "**", "[Z-a].md", "[!Z-a].md", "[]a].md", "[!]a].md",
    )
] + ["DC/rd.md", "DC/*", "[A-Z]*/rd.md", "[!a-z]*/rd.md", "[a-z]*/RD.md", "**/[A-Z]*.md", "**/[!a-z].md", "**/[A-z].md"]  # fmt: skip


class OverlapCaseFolding(unittest.TestCase):
    def test_two_negated_classes_that_list_a_whole_case_group_between_them_overlap(self):
        # audit tests-prelude V3-02, the negated branch of _fold_class. As written the first class takes only "A" and
        # the second only "a"; a case-insensitive file system calls those one name, so a node owning one of them owns
        # the other. Only closing a negated class under case finds it: nothing else in the two classes meets.
        low, high = chr(0), chr(0x10FFFF)
        only_upper_a = f"[!{low}-@B-{high}].md"
        only_lower_a = f"[!{low}-`b-{high}].md"
        self.assertTrue(fnmatch.fnmatchcase("A.md", only_upper_a) and not fnmatch.fnmatchcase("a.md", only_upper_a))
        self.assertTrue(fnmatch.fnmatchcase("a.md", only_lower_a) and not fnmatch.fnmatchcase("A.md", only_lower_a))
        self.assertIs(overlap(only_upper_a, only_lower_a), True)
        self.assertIs(overlap(only_lower_a, only_upper_a), True)
        # the closure takes in the listed variant of an unlisted letter, and no letter that is listed in both spellings
        self.assertIs(overlap(only_upper_a, "a.md"), True)
        self.assertIs(overlap(only_upper_a, "b.md"), False)
        self.assertIs(overlap(only_upper_a, "B.md"), False)

    def test_the_audit_repros(self):
        # lower-casing the pattern made [!a-z] a class that takes no letter and [A-z] one that lost "[", "_" and "`":
        # each pair below overlaps even on a case-sensitive file system
        for pattern, name in (
            ("docs/[!a-z]*.md", "docs/README.md"),
            ("[!a-z].md", "B.md"),
            ("[A-z].md", "_.md"),
        ):
            self.assertTrue(all(fnmatch.fnmatchcase(n, p) for p, n in zip(pattern.split("/"), name.split("/"))), pattern)
            self.assertIs(overlap(pattern, name), True, (pattern, name))
            self.assertIs(overlap(name, pattern), True, (name, pattern))

    def test_every_pair_against_enumerated_spellings(self):
        reach = {
            p: frozenset(tuple(n.casefold() for n in u) for u in CASE_UNIVERSE if exact_matches(p, u)) for p in CASE_PATTERNS
        }
        missed, spurious = [], []
        for a, b in itertools.combinations_with_replacement(CASE_PATTERNS, 2):
            witness = not reach[a].isdisjoint(reach[b])
            got = overlap(a, b)
            if witness and not got:
                missed.append((a, b))  # two workers could own one file
            if got and not witness:
                spurious.append((a, b))
        self.assertEqual(missed, [], "false negatives (a witness name exists)")
        self.assertEqual(spurious, [], "reported overlap without any witness name")

    def test_a_glob_and_a_name_overlap_exactly_when_some_spelling_of_the_name_matches(self):
        # seeded: a random glob against a random glob-free dotted name. The answer is exact, in both directions
        rng = random.Random(20261001)
        atoms = [
            "a", "b", "z", "A", "B", "Z", "_", "0", "7", ".", "?", "*", "[a-z]", "[A-Z]", "[A-z]", "[a-c]", "[B-Z]",
            "[!a-z]", "[!A-Z]", "[!a-zA-Z]", "[!b]", "[!B]", "[_0]", "[0-9]", "[!0-9]", "[a-zA-Z]", "[Z-a]", "[!Z-a]",
        ]  # fmt: skip
        letters = "abzABZ_07`"  # no ".": a name such as "...md" would be a dotted directory, not a file
        checked = 0
        for _ in range(2500):
            pattern = "".join(rng.choice(atoms) for _ in range(rng.randint(1, 4)))
            name = "".join(rng.choice(letters) for _ in range(rng.randint(1, 3))) + ".md"
            if not any(c in pattern for c in "*?["):
                continue
            expected = any(_fn(s, pattern) for s in spellings(name))
            self.assertIs(overlap(pattern, name), expected, (pattern, name))
            self.assertIs(overlap(name, pattern), expected, (name, pattern))
            checked += 1
        self.assertGreater(checked, 1800)

    def test_case_folding_reaches_beyond_the_basic_plane(self):
        self.assertIs(overlap("\U00010400.md", "\U00010428.md"), True)  # Deseret capital and small long I


if __name__ == "__main__":
    unittest.main()
