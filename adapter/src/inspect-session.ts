import { resolve } from "node:path";

import { AhpCore } from "./ahp-core.js";
import { defaultUserDataDirectory } from "./endpoint-registry.js";

interface Arguments {
  readonly sessionUri: string;
  readonly userDataDirectory: string;
}

async function main(): Promise<void> {
  const args = parseArguments(process.argv.slice(2));
  const core = new AhpCore({
    userDataDirectory: args.userDataDirectory,
    clientId: "qq-copilot-session-inspector",
    locale: "zh-CN",
    watch: false,
  });
  try {
    await core.start();
    const catalogue = await core.listSessions();
    const endpoint = catalogue.endpoints.find((item) =>
      item.sessions.some((session) => session.resource === args.sessionUri),
    );
    if (!endpoint) {
      throw new Error("session is not present on a live Agent Host");
    }
    const binding = await core.bindSession(
      endpoint.endpoint.id,
      args.sessionUri,
    );
    try {
      const snapshot = binding.snapshot();
      process.stdout.write(
        `${JSON.stringify(
          {
            endpointId: endpoint.endpoint.id,
            selectedProtocol: endpoint.selectedProtocol,
            session: {
              status: snapshot.session?.status,
              config: snapshot.session?.config ?? null,
              activeClients:
                snapshot.session?.activeClients.map((client) => ({
                  clientId: client.clientId,
                  displayName: client.displayName,
                  toolCount: client.tools.length,
                })) ?? [],
              inputNeededKinds:
                snapshot.session?.inputNeeded?.map((input) => input.kind) ??
                [],
            },
            chat: {
              status: snapshot.defaultChat?.status,
              interactivity: snapshot.defaultChat?.interactivity ?? null,
              activeTurn: snapshot.defaultChat?.activeTurn?.id ?? null,
            },
          },
          null,
          2,
        )}\n`,
      );
      await binding.close();
    } finally {
      await core.stop();
    }
  } catch (error) {
    await core.stop();
    throw error;
  }
}

function parseArguments(args: readonly string[]): Arguments {
  let sessionUri: string | undefined;
  let userDataDirectory: string | undefined;
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    const value = args[index + 1];
    if (argument === "--session" && value) {
      sessionUri = value;
      index += 1;
    } else if (argument === "--user-data-dir" && value) {
      userDataDirectory = resolve(value);
      index += 1;
    } else {
      throw new Error(
        "usage: inspect-session --session <uri> [--user-data-dir <path>]",
      );
    }
  }
  if (!sessionUri) {
    throw new Error("--session is required");
  }
  return {
    sessionUri,
    userDataDirectory: userDataDirectory ?? defaultUserDataDirectory(),
  };
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : "unknown error";
  process.stderr.write(`AHP session inspection failed: ${message}\n`);
  process.exitCode = 1;
});
