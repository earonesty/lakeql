---
"lakeql": patch
---

Stream deferred top-k and filtered LIMIT payloads through bounded, measured output
windows with consumer backpressure. Charge remaining references, projected results
and overlapping modeled decoded batches; release completed state between windows.
Preserve result order and cancellation, shrink unexpectedly wide windows, and allow
streaming results larger than the live operator allowance.
