# Security

Lakeql security controls are caller-owned and explicit:

- `QueryPolicy.allowedColumns` restricts selected and predicate columns.
- `QueryPolicy.maxLimit` applies a hard row cap.
- `QueryPolicy.rowFilter` injects a required predicate into every query.
- `DESCRIBE` filters schema metadata through `allowedColumns` and rejects physical row counts when
  `rowFilter` is active.
- Query budgets fail with `LAKEQL_BUDGET_EXCEEDED` before unbounded work continues.

Bookmarks and pagination tokens can be HMAC-signed with `signPaginationToken` and verified with `verifyPaginationToken`.
