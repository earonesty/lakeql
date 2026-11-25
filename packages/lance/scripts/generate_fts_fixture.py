"""Official-writer full-text (INVERTED) index fixture. Run with pylance==8.0.0.

Builds a multi-fragment dataset with a positional inverted index, appends a second index
partition with `optimize_indices`, then deletes rows after indexing (deleted documents stay in
the index and in BM25's corpus statistics). Expected results are recorded from pylance itself,
in a fresh process (`_fts_oracle.py`) so they reflect what a reader of the committed files sees.
"""
import argparse
import base64
import hashlib
import json
import random
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import lance
import pyarrow as pa
from lance.query import FullTextOperator

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[1] / "fixtures" / "fts-inverted-v2.0.lance")
parser.add_argument("--rows", type=int, default=1200)
parser.add_argument("--appended", type=int, default=400)
parser.add_argument("--workerd", action="store_true",
                    help="write a compact dataset to a temporary directory and emit src/fts-fixture.generated.ts")
args = parser.parse_args()
if args.workerd:
    args.rows, args.appended = 240, 80
    args.output = Path(tempfile.mkdtemp()) / "fts-workerd.lance"
ROOT = args.output
if ROOT.exists():
    shutil.rmtree(ROOT)

rng = random.Random(7)
COMMON = ["the", "a", "of", "and", "is", "with", "quick", "brown", "fox", "jumps", "over", "lazy", "dog"]
RARE = ["running", "runs", "ran", "runner", "café", "naïve", "straße", "database", "databases",
        "indexing", "indexes", "search", "searching", "searched", "lance", "vector", "storage",
        "quickly", "browsing", "foxes", "dogs", "jumped", "jumping", "Résumé", "ÉCOLE", "zebra", "yak"]
SELDOM = [f"term{i}" for i in range(200)]


def doc(i: int) -> str:
    words = []
    for _ in range(rng.randint(4, 18)):
        r = rng.random()
        words.append(rng.choice(COMMON) if r < 0.55 else rng.choice(RARE) if r < 0.9 else rng.choice(SELDOM))
    if i % 53 == 0:
        words[1:1] = ["quick", "brown", "fox"]
    if i % 97 == 0:
        words[2:2] = ["quick", "very", "brown"]
    return " ".join(words)


def table(start: int, count: int) -> pa.Table:
    return pa.table({"id": list(range(start, start + count)), "text": [doc(i) for i in range(start, start + count)]})


ds = lance.write_dataset(table(0, args.rows), ROOT, data_storage_version="2.0",
                         enable_stable_row_ids=True, enable_v2_manifest_paths=True,
                         max_rows_per_file=400)
ds.create_scalar_index("text", "INVERTED", with_position=True, name="text_idx")
ds = lance.write_dataset(table(args.rows, args.appended), ROOT, mode="append",
                         data_storage_version="2.0")
ds.optimize.optimize_indices()
ds = lance.dataset(ROOT)
ds.delete("id % 11 = 0 AND id % 2 = 0")  # soft deletes after indexing
ds = lance.dataset(ROOT)

QUERIES = {
    "match_or_fox": ("match", dict(query="fox"), 20),
    "match_or_multi": ("match", dict(query="quick brown fox"), 25),
    "match_stem": ("match", dict(query="running databases"), 25),
    "match_stopwords_only": ("match", dict(query="the of and"), 10),
    "match_unicode": ("match", dict(query="café naïve"), 20),
    "match_fold_query": ("match", dict(query="cafe resume ecole"), 20),
    "match_and": ("match", dict(query="quick brown fox", operator=FullTextOperator.AND), 25),
    "match_and_missing_term": ("match", dict(query="quick zzzzunknown", operator=FullTextOperator.AND), 10),
    "match_fuzzy_1": ("match", dict(query="quik", fuzziness=1), 20),
    "match_fuzzy_2": ("match", dict(query="databse serch", fuzziness=2), 20),
    "match_fuzzy_prefix": ("match", dict(query="quik", fuzziness=1, prefix_length=1, max_expansions=3), 20),
    "match_fuzzy_and_shared": ("match", dict(query="quik quick", fuzziness=1, operator=FullTextOperator.AND), 30),
    "match_and_expansion_cutoff": ("match", dict(query="term1", operator=FullTextOperator.AND, fuzziness=1, max_expansions=2), 30),
    "match_fuzzy_multi_cutoff": ("match", dict(query="quik term1", fuzziness=1, max_expansions=2), 30),
    "match_missing": ("match", dict(query="zzzzunknown"), 10),
    "match_long": ("match", dict(query="the quick brown fox jumps over the lazy dog"), 50),
    "phrase_repeated_term": ("phrase", dict(query="quick quick quick"), 10),
    "phrase_rare_and_common": ("phrase", dict(query="term184 quick"), 10),
    "phrase_slop_three_terms": ("phrase", dict(query="quick brown fox", slop=1), 60),
    "phrase_exact": ("phrase", dict(query="quick brown fox"), 30),
    "phrase_slop": ("phrase", dict(query="quick brown", slop=1), 30),
    "phrase_slop_wide": ("phrase", dict(query="quick fox", slop=3), 30),
    "phrase_missing": ("phrase", dict(query="quick zzzzunknown"), 10),
    "bool_must_should": ("bool", dict(must=[("match", dict(query="fox"))], should=[("match", dict(query="jumps lazy"))]), 30),
    "bool_must_not": ("bool", dict(should=[("match", dict(query="fox dog"))], must_not=[("match", dict(query="lazy"))]), 30),
}


def jsonable_spec(spec):
    return json.loads(json.dumps(spec, default=lambda o: getattr(o, "value", str(o))))


oracle = json.loads(subprocess.run(
    [sys.executable, str(Path(__file__).with_name("_fts_oracle.py"))],
    input=json.dumps({"root": str(ROOT), "version": ds.version,
                      "queries": {n: [k, jsonable_spec(sp), lim] for n, (k, sp, lim) in QUERIES.items()}}),
    capture_output=True, text=True, check=True).stdout)

expected = {
    "producer": {"package": "pylance", "version": lance.__version__},
    "storageVersion": ds.data_storage_version,
    "datasetVersion": ds.version,
    "rowCount": ds.count_rows(),
    "indexName": "text_idx",
    "queries": {name: {"kind": kind, "spec": jsonable_spec(spec), "limit": limit, **oracle[name]}
                for name, (kind, spec, limit) in QUERIES.items()},
}

if args.workerd:
    keep = ("match_or_multi", "match_stem", "phrase_exact", "bool_must_should")
    files = {f"fixtures/fts-workerd.lance/{p.relative_to(ROOT)}": base64.b64encode(p.read_bytes()).decode()
             for p in sorted(ROOT.rglob("*")) if p.is_file() and "_transactions" not in p.parts}
    out = Path(__file__).resolve().parents[1] / "src" / "fts-fixture.generated.ts"
    lines = ["/* Generated by scripts/generate_fts_fixture.py --workerd. */",
             "export const FTS_WORKERD_FIXTURE_BASE64: Readonly<Record<string, string>> = {"]
    lines += [f'  "{name}":\n    "{data}",' for name, data in files.items()]
    lines += ["};", "",
              "export const FTS_WORKERD_EXPECTED = " + json.dumps(
                  {"datasetVersion": ds.version, "queries": {k: expected["queries"][k] for k in keep}}, indent=2) + " as const;", ""]
    out.write_text("\n".join(lines))
    print("wrote", out, sum(len(d) for d in files.values()))
    raise SystemExit(0)

# Unindexed append: a later version no longer covered by the index.
lance.write_dataset(table(args.rows + args.appended, 5), ROOT, mode="append", data_storage_version="2.0")
expected["unindexedAppendVersion"] = lance.dataset(ROOT).version
expected["sha256"] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in sorted(ROOT.rglob("*")) if p.is_file()}
(ROOT / "expected.json").write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n")
print(json.dumps({k: len(v["hits"]) if "hits" in v else v.get("error") for k, v in expected["queries"].items()}, indent=1))
