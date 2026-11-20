"""Official-writer inverted-index analyzer variants. Run with pylance==8.0.0.

One dataset, several text columns, each indexed with different analyzer settings, so the
query-side analyzer can be checked against pylance for every supported configuration.
"""
import argparse
import hashlib
import json
import os
import random
import shutil
from pathlib import Path

import lance
import pyarrow as pa
from lance.query import MatchQuery, PhraseQuery

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[1] / "fixtures" / "fts-variants-v2.0.lance")
args = parser.parse_args()
ROOT = args.output
if ROOT.exists():
    shutil.rmtree(ROOT)

rng = random.Random(11)
WORDS = ["Running", "runners", "RUN", "Café", "cafe", "Naïve", "straße", "Strasse", "the", "of", "and",
         "quick", "Quick", "brown", "Brown", "fox", "Foxes", "dogs", "Dog's", "over-the-top", "e-mail",
         "x" * 45, "Generously", "generous", "communism", "ÉCOLE", "école", "ß", "İstanbul", "Œuvre", "ÅNGSTRÖM"]
N = 400


def text() -> str:
    return " ".join(rng.choice(WORDS) for _ in range(rng.randint(3, 10)))


cols = {name: [text() for _ in range(N)] for name in ("plain", "cased", "custom", "raw", "nostem", "tabs", "casestem", "french")}
cols["casestem"] = [t + " YELL Yell yell SKY sky" if i % 3 == 0 else t for i, t in enumerate(cols["casestem"])]
cols["tabs"] = [t.replace(" ", "\t") if i % 2 else t for i, t in enumerate(cols["tabs"])]
table = pa.table({"id": list(range(N)), **cols})
ds = lance.write_dataset(table, ROOT, data_storage_version="2.0", enable_stable_row_ids=True,
                         enable_v2_manifest_paths=True, max_rows_per_file=200)
ds.create_scalar_index("plain", "INVERTED", name="plain_idx")  # defaults, no positions
ds.create_scalar_index("cased", "INVERTED", name="cased_idx", base_tokenizer="whitespace", lower_case=False,
                       stem=False, remove_stop_words=False, ascii_folding=False, with_position=True)
ds.create_scalar_index("custom", "INVERTED", name="custom_idx", custom_stop_words=["quick", "fox"],
                       stem=False, with_position=True)
ds.create_scalar_index("raw", "INVERTED", name="raw_idx", base_tokenizer="raw", lower_case=True,
                       stem=False, remove_stop_words=False, with_position=False)
ds.create_scalar_index("nostem", "INVERTED", name="nostem_idx", stem=False, ascii_folding=False, with_position=True,
                       max_token_length=8)
ds.create_scalar_index("casestem", "INVERTED", name="casestem_idx", base_tokenizer="whitespace", lower_case=False,
                       stem=True, remove_stop_words=False, ascii_folding=False, with_position=True)
# A language this reader does not implement: listed as unsupported, never silently approximated.
ds.create_scalar_index("french", "INVERTED", name="french_idx", language="French")
ds.create_scalar_index("tabs", "INVERTED", name="tabs_idx", base_tokenizer="whitespace", stem=True, with_position=True)
ds = lance.dataset(ROOT)

PROBES = {
    "plain_idx": ["running", "Runners the RUN", "cafe Naive ecole", "straße strasse", "dog's e-mail over", "generously communism",
                  "ÅNGSTRÖM Œuvre İstanbul", "quick"],
    "cased_idx": ["Running", "running", "Café", "cafe", "The the", "Dog's", "Quick brown"],
    "custom_idx": ["quick fox", "brown", "dogs", "Running"],
    "raw_idx": ["quick brown", "cafe", "the"],
    "nostem_idx": ["running", "runners", "café cafe", "generously", "communism"],
    "tabs_idx": ["running", "quick brown", "Dog's"],
    "casestem_idx": ["YELL", "Yell", "yell", "SKY", "Running"],
}
PHRASES = {"cased_idx": ["quick brown", "Quick brown"], "custom_idx": ["brown dogs"], "nostem_idx": ["quick brown", "the quick"],
           "tabs_idx": ["quick brown"]}
column_of = {"casestem_idx": "casestem", "plain_idx": "plain", "cased_idx": "cased", "custom_idx": "custom", "raw_idx": "raw", "nostem_idx": "nostem", "tabs_idx": "tabs"}
expected = {"producer": {"package": "pylance", "version": lance.__version__}, "datasetVersion": ds.version, "queries": []}
for index, probes in PROBES.items():
    for text_query in probes:
        result = ds.to_table(columns=["id"], full_text_query=MatchQuery(text_query, column_of[index]), limit=40).to_pydict()
        expected["queries"].append({"index": index, "kind": "match", "text": text_query,
                                    "hits": [{"id": int(i), "score": float(s)} for i, s in zip(result["id"], result["_score"])]})
for index, probes in PHRASES.items():
    for text_query in probes:
        result = ds.to_table(columns=["id"], full_text_query=PhraseQuery(text_query, column_of[index]), limit=40).to_pydict()
        expected["queries"].append({"index": index, "kind": "phrase", "text": text_query,
                                    "hits": [{"id": int(i), "score": float(s)} for i, s in zip(result["id"], result["_score"])]})
# Fuzzy prefixes are measured in bytes, so a two-byte prefix is one accented letter.
result = ds.to_table(columns=["id"], full_text_query=MatchQuery("ÉxOLE", "cased", fuzziness=1, prefix_length=2), limit=40).to_pydict()
expected["queries"].append({"index": "cased_idx", "kind": "match", "text": "ÉxOLE", "fuzziness": 1, "prefixLength": 2,
                            "hits": [{"id": int(i), "score": float(s)} for i, s in zip(result["id"], result["_score"])]})
expected["sha256"] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in sorted(ROOT.rglob("*")) if p.is_file()}
(ROOT / "expected.json").write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n")
print([(q["index"], q["text"], len(q["hits"])) for q in expected["queries"]])


def write_expected(root, expected):
    expected["sha256"] = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in sorted(root.rglob("*")) if p.is_file() and p.name != "expected.json"}
    (root / "expected.json").write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n")


# An index over documents whose every token is a stop word has no partitions at all.
edge = ROOT.parent / "fts-edge-v2.0.lance"
if edge.exists():
    shutil.rmtree(edge)
eds = lance.write_dataset(pa.table({"id": [0, 1], "text": ["the and", "of the"]}), edge, data_storage_version="2.0",
                          enable_stable_row_ids=True, enable_v2_manifest_paths=True)
eds.create_scalar_index("text", "INVERTED", name="text_idx", with_position=True)
write_expected(edge, {"producer": {"package": "pylance", "version": lance.__version__}, "datasetVersion": lance.dataset(edge).version})

# FTS format version 2: varint-delta posting tails and shared position streams.
os.environ["LANCE_FTS_FORMAT_VERSION"] = "2"
f2 = ROOT.parent / "fts-format2-v2.0.lance"
if f2.exists():
    shutil.rmtree(f2)
f2ds = lance.write_dataset(pa.table({"id": list(range(N)), "text": cols["plain"]}), f2, data_storage_version="2.0",
                           enable_stable_row_ids=True, enable_v2_manifest_paths=True, max_rows_per_file=200)
f2ds.create_scalar_index("text", "INVERTED", name="text_idx", with_position=True)
f2ds = lance.dataset(f2)
f2expected = {"producer": {"package": "pylance", "version": lance.__version__}, "datasetVersion": f2ds.version, "queries": []}
for text_query in ["running", "Runners the RUN", "cafe Naive ecole", "quick brown", "generously communism"]:
    result = f2ds.to_table(columns=["id"], full_text_query=MatchQuery(text_query, "text"), limit=40).to_pydict()
    f2expected["queries"].append({"kind": "match", "text": text_query,
                                  "hits": [{"id": int(i), "score": float(s)} for i, s in zip(result["id"], result["_score"])]})
write_expected(f2, f2expected)
print("format2 index version:", [i for i in f2ds.list_indices()])
