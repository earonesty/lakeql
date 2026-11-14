/** Store arbitrary data keys without invoking Object.prototype setters. */
export function setRecordValue(record: Record<string, unknown>, key: string, value: unknown): void {
  if (key === "__proto__") {
    Object.defineProperty(record, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true,
    });
  } else {
    record[key] = value;
  }
}
