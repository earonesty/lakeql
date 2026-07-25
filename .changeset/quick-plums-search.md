---
"lakeql-lance": patch
---

Reduce object-storage requests for exact B-tree lookups by materializing candidate pages once, searching their sorted keys locally, reusing index metadata already fetched with the manifest, coalescing bounded Lance metadata tails, and retaining decoded lookup/page state in a configurable snapshot-scoped byte cache.
