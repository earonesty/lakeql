# Recipe: R2 Iceberg API

Plan an Iceberg table from metadata stored in R2:

```ts
import { planR2Iceberg } from "../../examples/r2-iceberg";

const plan = await planR2Iceberg(env.DATA);
```

Fixture metadata is at `fixtures/data/iceberg/warehouse/places/metadata/v2.metadata.json`.
