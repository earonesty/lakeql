---
"@laql/http": patch
---

`httpStore.getRange` now slices the response client-side when a server ignores
the `Range` header and returns `200` with the full body (legal, since `Range`
is advisory — GitHub Pages does this for some assets). Previously the full
object was returned as if it were the requested window, which corrupted Parquet
footer reads ("footer != PAR1").
