"""load_dag and validate_dag."""

import copy
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fake_prelude as fp  # noqa: E402
from fake_prelude import make_dag, node  # noqa: E402

NS = fp.make_namespace(fp.FakeHost())
validate_dag = NS["validate_dag"]
load_dag = NS["load_dag"]


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def errors_of(*nodes, **kw):
    return validate_dag(make_dag(*nodes), **kw)


class LoadDag(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def write(self, name, text):
        path = os.path.join(self.tmp.name, name)
        with open(path, "w") as f:
            f.write(text)
        return path

    def test_nodes_pass_through(self):
        path = self.write("a.json", json.dumps({"goal": "g", "nodes": [node("A")]}))
        self.assertEqual(load_dag(path)["nodes"][0]["id"], "A")

    def test_stories_normalized_to_nodes_without_touching_the_file(self):
        doc = {"goal": "g", "approved": True, "source_spec": "s.md", "stories": [node("US-001")]}
        path = self.write("prd.json", json.dumps(doc, indent=2))
        before = read_bytes(path)
        dag = load_dag(path)
        self.assertIn("nodes", dag)
        self.assertNotIn("stories", dag)
        self.assertEqual(list(dag), ["goal", "approved", "source_spec", "nodes"])  # position kept
        self.assertEqual(read_bytes(path), before)

    def test_a_byte_order_mark_is_tolerated(self):
        path = os.path.join(self.tmp.name, "bom.json")
        with open(path, "wb") as f:
            f.write(b"\xef\xbb\xbf" + json.dumps({"nodes": [node("A")]}).encode("utf-8"))
        self.assertEqual(load_dag(path)["nodes"][0]["id"], "A")

    def test_both_keys_is_an_error(self):
        path = self.write("a.json", json.dumps({"nodes": [], "stories": []}))
        with self.assertRaisesRegex(ValueError, "both"):
            load_dag(path)

    def test_neither_key_is_an_error(self):
        path = self.write("a.json", json.dumps({"goal": "g"}))
        with self.assertRaisesRegex(ValueError, "neither"):
            load_dag(path)

    def test_invalid_json_names_the_file(self):
        path = self.write("broken.json", '{"nodes": [')
        with self.assertRaisesRegex(ValueError, "broken.json"):
            load_dag(path)

    def test_top_level_must_be_an_object(self):
        path = self.write("a.json", "[1, 2]")
        with self.assertRaises(ValueError):
            load_dag(path)

    def test_a_top_level_that_is_not_an_object_is_refused_by_name(self):
        # a JSON string that contains the word "nodes" must not slip past the check and die on .items()
        for text in ("[1, 2]", '"nodes"', '"nodes and stories"', "5", "null", "true"):
            path = self.write("a.json", text)
            with self.assertRaisesRegex(ValueError, "top level must be a JSON object"):
                load_dag(path)


class ValidateShapes(unittest.TestCase):
    def test_valid_dag(self):
        self.assertEqual(errors_of(node("A"), node("B", deps=["A"])), [])

    def test_example_json_is_valid(self):
        dag = load_dag(os.path.join(fp.DAG_DIR, "example.json"))
        self.assertEqual(validate_dag(dag), [])

    def test_read_only_node_has_empty_files(self):
        self.assertEqual(errors_of(node("A", files=[])), [])

    def test_not_a_dict(self):
        self.assertEqual(validate_dag([1]), ["dag must be a JSON object"])
        self.assertEqual(validate_dag(None), ["dag must be a JSON object"])

    def test_nodes_must_be_a_non_empty_list(self):
        for bad in (None, [], {}, "x", {"A": 1}):
            self.assertEqual(validate_dag({"nodes": bad}), ["nodes must be a non-empty list"], bad)
        self.assertEqual(validate_dag({}), ["nodes must be a non-empty list"])

    def test_node_must_be_an_object(self):
        errs = validate_dag(make_dag(node("A"), "B", 3, None))
        self.assertEqual(len([e for e in errs if "must be an object" in e]), 3)

    def test_id_must_be_a_non_empty_string(self):
        for bad in (None, "", "  ", 5, ["A"]):
            n = node("A")
            n["id"] = bad
            errs = errors_of(n)
            self.assertEqual(len(errs), 1, (bad, errs))
            self.assertIn("id must be a non-empty string", errs[0])

    def test_duplicate_id(self):
        errs = errors_of(node("A"), node("A", files=["other.txt"]))
        self.assertEqual(errs, ["duplicate id A"])

    def test_task_must_be_a_non_blank_string(self):
        for bad in ("", "   ", None, 5):
            n = node("A")
            n["task"] = bad
            self.assertEqual(errors_of(n), ["A task must be a non-blank string"], bad)

    def test_criteria_shapes(self):
        for bad in (None, [], "`cat x` prints Y", {}):
            n = node("A")
            n["acceptance_criteria"] = bad
            errs = errors_of(n)
            self.assertEqual(len(errs), 1, (bad, errs))
            self.assertIn("acceptance_criteria", errs[0])
        n = node("A", criteria=["ok", "", "   ", 7])
        errs = errors_of(n)
        self.assertEqual(
            errs,
            [
                "A acceptance_criteria[1] must be a non-blank string",
                "A acceptance_criteria[2] must be a non-blank string",
                "A acceptance_criteria[3] must be a non-blank string",
            ],
        )

    def test_string_depends_on_is_one_error_not_one_per_character(self):
        n = node("B")
        n["depends_on"] = "US-002"
        errs = errors_of(node("A"), n)
        self.assertEqual(errs, ["B depends_on must be a list of strings"])

    def test_depends_on_items_must_be_strings(self):
        n = node("B")
        n["depends_on"] = [["A"], 3]
        self.assertEqual(len(errors_of(node("A"), n)), 2)

    def test_missing_depends_on_means_none(self):
        n = node("A")
        del n["depends_on"]
        self.assertEqual(errors_of(n), [])

    def test_unknown_dependency(self):
        self.assertEqual(errors_of(node("A", deps=["ZZZ"])), ["A depends on unknown ZZZ"])

    def test_files_are_required(self):
        n = node("A")
        del n["files"]
        self.assertEqual(errors_of(n), ["A files missing; use [] for a read-only node"])

    def test_files_shapes(self):
        n = node("A")
        n["files"] = "src/a.ts"
        errs = errors_of(n)
        self.assertEqual(errs, ["A files must be a list of strings"])  # not one error per character
        errs = errors_of(node("A", files=["ok.ts", "", 5]))
        self.assertEqual(len(errs), 2)

    def test_absolute_and_escaping_files(self):
        cases = {
            "/etc/passwd": "absolute",
            "~/x": "absolute",
            "C:\\x": "absolute",
            "../x": "escapes",
            "src/../../x": "escapes",
            "{ok,../bad}.ts": "escapes",
        }
        for entry, word in cases.items():
            errs = errors_of(node("A", files=[entry]))
            self.assertEqual(len(errs), 1, (entry, errs))
            self.assertIn(word, errs[0])
            self.assertIn(repr(entry), errs[0])
        self.assertEqual(errors_of(node("A", files=["src/../src/a.ts", "./b.ts"])), [])

    def test_dotdot_next_to_a_globstar_cannot_be_checked(self):
        # audit adversarial-review R2-05: "**" may match no directory, so normpath is wrong about where
        # src/**/../../x ends up: it is ../x when "**" matches nothing
        for entry in ("src/**/../../x", "**/../x", "a/**/..", "{ok,src/**/../../x}.ts"):
            errs = errors_of(node("A", files=[entry]))
            self.assertEqual(len(errs), 1, (entry, errs))
            self.assertIn(repr(entry), errs[0])
            self.assertIn("'..'", errs[0])
        # no ".." segment, or no "**": the lexical answer is right
        for entry in ("src/**/v1..v2.txt", "lib/*/../b.ts", "src/**/a.ts"):
            self.assertEqual(errors_of(node("A", files=[entry])), [], entry)

    def test_status_must_be_a_known_value(self):
        for good in ("pending", "running", "review", "done", "blocked", "skipped"):
            self.assertEqual(errors_of(node("A", status=good)), [], good)
        for bad in ("completed", "Done", "in_progress", None, 3):
            errs = errors_of(node("A", status=bad))
            self.assertEqual(len(errs), 1, (bad, errs))
            self.assertIn("status", errs[0])

    def test_missing_status_is_fine(self):
        n = node("A")
        del n["status"]
        self.assertEqual(errors_of(n), [])

    def test_agent_must_be_a_non_empty_string(self):
        self.assertEqual(errors_of(node("A", agent="sonic")), [])
        for bad in ("", "  ", None, 4):
            errs = errors_of(node("A", agent=bad))
            self.assertEqual(errs, ["A agent must be a non-empty string"], bad)

    def test_goal_must_be_a_string_when_present(self):
        dag = make_dag(node("A"))
        dag["goal"] = 5
        self.assertEqual(validate_dag(dag), ["goal must be a string"])

    def test_a_bare_dotdot_escapes_the_repo(self):
        for entry in ("..", "./..", "src/../.."):
            errs = errors_of(node("A", files=[entry]))
            self.assertEqual(len(errs), 1, (entry, errs))
            self.assertIn("escapes", errs[0])


class ValidateCycles(unittest.TestCase):
    def test_self_dependency_is_reported_once(self):
        errs = errors_of(node("A", deps=["A"]))
        self.assertEqual(len(errs), 1)
        self.assertIn("cycle: A -> A", errs[0])

    def test_two_node_cycle(self):
        errs = errors_of(node("A", deps=["B"]), node("B", deps=["A"]))
        self.assertEqual(errs, ["cycle: A -> B -> A"])

    def test_cycle_message_shows_only_the_cycle(self):
        # A is merely downstream of the B <-> C cycle and must not be named in it.
        errs = errors_of(node("A", deps=["B"]), node("B", deps=["C"]), node("C", deps=["B"]))
        self.assertEqual(errs, ["cycle: B -> C -> B"])

    def test_cycle_below_a_chain(self):
        errs = errors_of(
            node("X", deps=["P"]), node("P", deps=["Q"]), node("Q", deps=["P"]), node("Z")
        )
        self.assertEqual(errs, ["cycle: P -> Q -> P"])

    def test_three_cycle_starts_at_the_earliest_node(self):
        errs = errors_of(node("C", deps=["A"]), node("A", deps=["B"]), node("B", deps=["C"]))
        self.assertEqual(errs, ["cycle: C -> A -> B -> C"])

    def test_acyclic_diamond_is_fine(self):
        self.assertEqual(
            errors_of(node("A"), node("B", deps=["A"]), node("C", deps=["A"]), node("D", deps=["B", "C"])),
            [],
        )

    def test_duplicate_dependency_entries_are_tolerated(self):
        self.assertEqual(errors_of(node("A"), node("B", deps=["A", "A"])), [])

    def test_a_cycle_is_named_from_its_earliest_node_when_a_walk_first_lands_on_a_later_one(self):
        # A hangs below the B <-> C cycle; a walk from A reaches it at C, but the message starts at B
        errs = errors_of(node("A", deps=["C"]), node("B", deps=["C"]), node("C", deps=["B"]))
        self.assertEqual(errs, ["cycle: B -> C -> B"])


class ValidateOverlap(unittest.TestCase):
    def test_files_that_are_not_lists_are_reported_and_do_not_break_the_overlap_check(self):
        a, b = node("A"), node("B")
        a["files"], b["files"] = "a.txt", None  # the builder would turn these into lists
        errs = errors_of(a, b, node("C", files=["c.txt"]))
        self.assertEqual(errs, ["A files must be a list of strings", "B files must be a list of strings"])

    def test_only_the_usable_entries_take_part_in_the_overlap_check(self):
        errs = errors_of(node("A", files=["", None, "a.txt", "a.txt"]), node("B", files=["a.txt"]))
        self.assertEqual(
            errs,
            [
                "A files[0] must be a non-blank string",
                "A files[1] must be a non-blank string",
                "A and B have no dependency path but their files overlap: A owns 'a.txt', B owns 'a.txt'",
            ],
        )

    def test_an_entry_that_expands_to_too_many_variants_is_still_checked_as_written(self):
        many = "{a,b}" * 7  # 128 variants: more than the matcher expands
        self.assertEqual(
            errors_of(node("A", files=["/" + many])),
            [f"A files entry {'/' + many!r} is absolute; use a path relative to the repo root"],
        )
        self.assertEqual(
            errors_of(node("A", files=["../" + many])), [f"A files entry {'../' + many!r} escapes the repo via '..'"]
        )
        # undecidable means "may overlap", so a second node cannot own anything beside it
        self.assertEqual(len(errors_of(node("A", files=[many + "x.ts"]), node("B", files=["zzz/y.md"]))), 1)

    def test_overlap_without_a_dependency_path(self):
        errs = errors_of(node("A", files=["src/*.ts"]), node("B", files=["src/a.ts"]))
        self.assertEqual(len(errs), 1)
        for needle in ("A", "B", "'src/*.ts'", "'src/a.ts'", "no dependency path"):
            self.assertIn(needle, errs[0])

    def test_no_error_for_disjoint_globs(self):
        self.assertEqual(errors_of(node("A", files=["src/*.ts"]), node("B", files=["src/*.py"])), [])

    def test_direct_dependency_orders_them(self):
        self.assertEqual(
            errors_of(node("A", files=["x.txt"]), node("B", deps=["A"], files=["x.txt"])), []
        )

    def test_transitive_dependency_in_either_direction(self):
        chain = [node("A", files=["x"]), node("B", deps=["A"], files=["y"]), node("C", deps=["B"], files=["x"])]
        self.assertEqual(errors_of(*chain), [])
        reverse = [node("C", files=["x"]), node("B", deps=["C"], files=["y"]), node("A", deps=["B"], files=["x"])]
        self.assertEqual(errors_of(*reverse), [])

    def test_an_earlier_node_may_depend_on_a_later_one(self):
        # list order is not dependency order: only the pair's ancestry decides whether they may overlap
        self.assertEqual(errors_of(node("A", deps=["B"], files=["x.txt"]), node("B", files=["x.txt"])), [])
        self.assertEqual(errors_of(node("B", files=["x.txt"]), node("A", deps=["B"], files=["x.txt"])), [])

    def test_a_later_node_that_is_an_ancestor_of_an_earlier_one_through_a_chain(self):
        chain = [node("A", deps=["B"], files=["x"]), node("M", deps=["C"], files=["m"]), node("B", deps=["M"], files=["y"]), node("C", files=["x"])]
        self.assertEqual(errors_of(*chain), [])  # A -> B -> M -> C, and A and C share a file

    def test_a_dotted_directory_overlaps_a_file_inside_it(self):
        errs = errors_of(node("A", files=["packages/ui.kit"]), node("B", files=["packages/ui.kit/src/button.tsx"]))
        self.assertEqual(len(errs), 1)
        for needle in ("A and B", "'packages/ui.kit'", "'packages/ui.kit/src/button.tsx'"):
            self.assertIn(needle, errs[0])
        ordered = errors_of(node("A", files=["docs/v1.0"]), node("B", deps=["A"], files=["docs/v1.0/index.md"]))
        self.assertEqual(ordered, [])

    def test_a_dotted_file_does_not_collide_with_a_broad_glob(self):
        self.assertEqual(errors_of(node("A", files=["package.json"]), node("B", files=["**/*.md"])), [])

    def test_siblings_under_a_common_parent_still_collide(self):
        errs = errors_of(
            node("P", files=["p.txt"]),
            node("A", deps=["P"], files=["x.txt"]),
            node("B", deps=["P"], files=["x.txt"]),
        )
        self.assertEqual(len(errs), 1)
        self.assertIn("A and B", errs[0])

    def test_isolated_skips_the_overlap_check(self):
        nodes = (node("A", files=["x.txt"]), node("B", files=["x.txt"]))
        self.assertEqual(len(errors_of(*nodes)), 1)
        self.assertEqual(errors_of(*nodes, isolated=True), [])

    def test_isolated_does_not_skip_structural_checks(self):
        n = node("A")
        del n["files"]
        self.assertEqual(len(errors_of(n, isolated=True)), 1)

    def test_read_only_nodes_never_overlap(self):
        self.assertEqual(errors_of(node("A", files=[]), node("B", files=["x"]), node("C", files=[])), [])

    def test_repeated_patterns_give_one_message_per_pair(self):
        errs = errors_of(
            node("A", files=["x.txt", "x.txt", "./x.txt"]), node("B", files=["x.txt", "x.txt"])
        )
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs, sorted(set(errs)))

    def test_many_hits_are_capped_in_one_message(self):
        errs = errors_of(
            node("A", files=[f"f{i}.txt" for i in range(6)]),
            node("B", files=[f"f{i}.txt" for i in range(6)]),
        )
        self.assertEqual(len(errs), 1)
        self.assertIn("(+3 more)", errs[0])

    def test_one_error_per_colliding_pair(self):
        errs = errors_of(node("A", files=["x"]), node("B", files=["x"]), node("C", files=["x"]))
        self.assertEqual(len(errs), 3)

    def test_validation_does_not_mutate_the_dag(self):
        dag = make_dag(node("A", files=["x"]), node("B", files=["x"]))
        before = copy.deepcopy(dag)
        validate_dag(dag)
        self.assertEqual(dag, before)

    def test_exactly_four_hits_say_how_many_were_left_out(self):
        files = [f"f{i}.txt" for i in range(4)]
        errs = errors_of(node("A", files=files), node("B", files=files))
        self.assertEqual(len(errs), 1)
        self.assertIn("(+1 more)", errs[0])


if __name__ == "__main__":
    unittest.main()
