# Parquet Types

Reference page: use this page when you need exact Parquet decoding and rejection
rules.

Lakeql delegates physical Parquet decoding to `hyparquet` and has explicit rules
for nested columns. The rule is: decode correctly or reject before rows are
returned.

## Supported And Tested

- Primitive scalar columns used by the fixture suite: strings, booleans, 32-bit integers, 64-bit integers, and doubles.
- Nullable scalar values.
- DuckDB-authored logical values for `DECIMAL(9,2)`, `TIME`, `DATE`, local millisecond `TIMESTAMP`, and UTC-adjusted millisecond `TIMESTAMP`.
- Precision-preserving Parquet `TIMESTAMP_MILLIS`, `TIMESTAMP_MICROS`, and `TIMESTAMP_NANOS` values.
- DuckDB-authored unsigned integer logical values, binary payloads, and fixed-length byte arrays.
- DuckDB-authored list and map values, including null nested cells normalized to `null`.
- Struct assembly preserving nullable containers, members, and nested integer precision; covered by external fixtures and HTTP scans.
- DuckDB-authored null-heavy rows across scalar, date/timestamp, list, map, and binary columns.
- Column projection.
- Row-group pruning from footer statistics.

## Detected And Rejected

Selected BSON-annotated leaves are rejected during schema validation with
`LAKEQL_UNSUPPORTED_PARQUET_FEATURE`, including the column path and `feature: "bson"`.
The actual external BSON fixture verifies rejection before page reads and before
owned-query row emission. Selecting an unrelated top-level column does not require
BSON decoding.

Ordinary decimal decoding checks the unscaled integer before returning a JavaScript
number. Values outside the exact integer range reject rather than silently round.
Top-level scalar decimal leaves can use this value-checked path; ordinary nested
decimals above precision 15 remain rejected at schema validation. Decimal values
inside supported VARIANT containers have a separate exact coefficient/scale path,
including supported precision up to 38. This does not imply general exact decimal
arithmetic or ordering support across every operator.

ALP encoding and Brotli compression are not implemented in the current reader/codec
paths. Their external read-conformance cases remain failing, alongside BSON reads;
explicit rejection is not a conformance waiver or a claim that the release gate passes.

See [the compatibility matrix](compatibility.md) for tested capabilities and evidence.
New logical types must update `docs/compatibility.json` before becoming compatibility
promises. Allocation admission for a supported path does not establish a total
JavaScript-heap bound.
