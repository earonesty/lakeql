"""Run recorded full-text queries in a fresh process (stdin: JSON, stdout: JSON hits).

Recording in a process that also wrote, optimized and modified the dataset can observe
stale index state; a clean process reflects what a reader of the committed files sees.
"""
import json
import sys

import lance
from lance.query import BooleanQuery, FullTextOperator, MatchQuery, Occur, PhraseQuery


def build(kind, spec):
    if kind == "match":
        kwargs = {k: v for k, v in spec.items() if k not in ("query", "operator")}
        if "operator" in spec:
            kwargs["operator"] = FullTextOperator[str(spec["operator"]).upper()]
        return MatchQuery(spec["query"], "text", **kwargs)
    if kind == "phrase":
        return PhraseQuery(spec["query"], "text", slop=spec.get("slop", 0))
    clauses = []
    for occur, key in ((Occur.MUST, "must"), (Occur.SHOULD, "should"), (Occur.MUST_NOT, "must_not")):
        for sub_kind, sub_spec in spec.get(key, []):
            clauses.append((occur, build(sub_kind, sub_spec)))
    return BooleanQuery(clauses)


if __name__ == "__main__":
    request = json.load(sys.stdin)
    dataset = lance.dataset(request["root"], version=request["version"])
    out = {}
    for name, (kind, spec, limit) in request["queries"].items():
        try:
            table = dataset.to_table(columns=["id"], full_text_query=build(kind, spec), limit=limit).to_pydict()
            out[name] = {"hits": [{"id": int(i), "score": float(s)} for i, s in zip(table["id"], table["_score"])]}
        except Exception as error:  # record upstream rejections explicitly
            out[name] = {"error": str(error)[:200]}
    json.dump(out, sys.stdout)
