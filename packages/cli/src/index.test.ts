import { expect, it } from "vitest";
import { COMMANDS, usage } from "./index.js";

it("usage lists every command", () => {
  const text = usage();
  for (const cmd of COMMANDS) {
    expect(text).toContain(cmd);
  }
});
