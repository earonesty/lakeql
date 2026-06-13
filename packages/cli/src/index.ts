export const COMMANDS = ["query", "explain", "inspect", "write", "compact", "schema"] as const;

export type Command = (typeof COMMANDS)[number];

export function usage(): string {
  return [
    "usage: laql <command> [options]",
    "",
    `commands: ${COMMANDS.join(", ")}`,
    "",
    "Commands land per BUILD_PLAN.md phases; run `laql <command> --help` once implemented.",
  ].join("\n");
}
