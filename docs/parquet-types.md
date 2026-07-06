# Parquet Types

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
- DuckDB-authored null-heavy rows across scalar, date/timestamp, list, map, and binary columns.
- Column projection.
- Row-group pruning from footer statistics.

## Detected And Rejected

Struct columns are rejected with `LAKEQL_UNSUPPORTED_PARQUET_FEATURE`. `hyparquet` exposes struct
leaves as sub-column data rather than assembling the parent object, so Lakeql rejects unannotated
group nodes before scanning or row-group planning. This prevents silent flattening from looking like
a successful read.

Decimals above precision 15 are rejected with `LAKEQL_UNSUPPORTED_PARQUET_FEATURE`. The JS-facing
decode path would otherwise lose precision by returning wide decimals as numbers.

The current Parquet type matrix is either supported and tested or detected and rejected. New logical
types should update `docs/compatibility.json` before they become compatibility promises.
