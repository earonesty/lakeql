# Parquet Types

Reference page: use this page when you need exact Parquet decoding and rejection
rules.

Lakeql delegates physical Parquet decoding to `hyparquet` and has explicit rules
for nested columns. The rule is: decode correctly or reject before rows are
returned.

## Supported And Tested

- Primitive scalar columns used by the fixture suite: strings, booleans, 32-bit integers, 64-bit integers, and doubles.
- Nullable scalar values.
- ALP FLOAT/DOUBLE encoding, with exact exception bits and bounded output allocation.
- BSON logical byte columns, including nested values and dictionaries.
- DuckDB-authored logical values for `DECIMAL(9,2)`, `TIME`, `DATE`, local millisecond `TIMESTAMP`, and UTC-adjusted millisecond `TIMESTAMP`.
- Precision-preserving Parquet `TIMESTAMP_MILLIS`, `TIMESTAMP_MICROS`, and `TIMESTAMP_NANOS` values.
- DuckDB-authored unsigned integer logical values, binary payloads, and fixed-length byte arrays.
- DuckDB-authored list and map values, including null nested cells normalized to `null`.
- Struct assembly preserving nullable containers, members, and nested integer precision; covered by external fixtures and HTTP scans.
- DuckDB-authored null-heavy rows across scalar, date/timestamp, list, map, and binary columns.
- Column projection.
- Row-group pruning from footer statistics.

## Detected And Rejected

BSON annotations on physical types other than BYTE_ARRAY reject during schema
validation. Malformed BSON lengths, duplicate keys and invalid UTF-8 reject during
logical decoding. Selecting an unrelated top-level column does not require BSON
decoding.

Ordinary decimal decoding checks the unscaled integer before returning a JavaScript
number. Values outside the exact integer range reject rather than silently round.
Top-level scalar decimal leaves can use this value-checked path; ordinary nested
decimals above precision 15 remain rejected at schema validation. Decimal values
inside supported VARIANT containers have a separate exact coefficient/scale path,
including supported precision up to 38. This does not imply general exact decimal
arithmetic or ordering support across every operator.

## Logical Value Contracts

ALP FLOAT/DOUBLE decoding supports V1/V2 pages through public and owned readers,
with exact fixture bit parity and admitted output buffers. BSON BYTE_ARRAY leaves
also decode through shared public and owned paths. Documents use dynamic maps
internally and materialize as records, preserving missing versus null fields.
Brotli compression remains unimplemented; its two external read-conformance cases
remain failing. Explicit rejection is not a conformance waiver.

BSON int64 values remain bigint and UTC datetimes use exact TimestampValue values.
BSON-only types materialize as records tagged by `$bson`: binary (subtype, bytes),
objectId (bytes), regex (pattern, options), dbPointer (namespace, id), code and symbol
(value), codeWithScope (code, scope), timestamp (increment, seconds), decimal128
(bytes), undefined, minKey and maxKey. Byte payloads are detached. Decimal128
preserves all 16 encoded bytes rather than implying SQL decimal arithmetic. Code
and regex values are data and are never executed. These records are not an
automatic BSON re-encoding API; source documents can contain the same field names.
Duplicate document keys and invalid UTF-8 reject instead of silently losing data.

See [the compatibility matrix](compatibility.md) for tested capabilities and evidence.
New logical types must update `docs/compatibility.json` before becoming compatibility
promises. Allocation admission for a supported path does not establish a total
JavaScript-heap bound.
