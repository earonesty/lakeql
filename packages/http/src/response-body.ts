import {
  MemoryBudget,
  type MemoryReservation,
  MemoryScope,
  type OwnedMemory,
  readOwnedStream,
} from "lakeql-core";

export { byteView } from "lakeql-core";

/** Legacy byte-returning API hands the final payload to its caller. */
export async function boundedResponseBody(
  response: Response,
  maxBytes: number,
): Promise<Uint8Array> {
  const scope = new MemoryScope(new MemoryBudget(Math.min(Number.MAX_SAFE_INTEGER, maxBytes * 2)));
  try {
    return (await ownedResponseBody(response, maxBytes, scope)).value;
  } finally {
    scope.dispose();
  }
}

/** Reserve every engine buffer before allocating; incoming Fetch chunks are runtime-owned. */
export async function ownedResponseBody(
  response: Response,
  maxBytes: number,
  scope: MemoryScope,
  initialReservation?: MemoryReservation,
): Promise<OwnedMemory<Uint8Array>> {
  const advertised = Number(response.headers.get("content-length"));
  const encoding = response.headers.get("content-encoding")?.toLowerCase();
  const knownLength =
    (!encoding || encoding === "identity") && Number.isSafeInteger(advertised) && advertised >= 0
      ? advertised
      : 0;
  return readOwnedStream(response.body, scope, {
    maxBytes,
    knownLength,
    purpose: "HTTP response buffer",
    metric: "HTTP response bytes",
    ...(initialReservation === undefined ? {} : { initialReservation }),
  });
}
