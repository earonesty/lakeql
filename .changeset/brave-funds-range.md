---
"lakeql": patch
"lakeql-http": patch
"lakeql-parquet": patch
---

Keep HTTP range reads active when an unencoded response merely varies on
`Accept-Encoding`, and read `INT64 DECIMAL` columns with declared precision above
15 when every decoded unscaled value remains exactly representable in JavaScript.
