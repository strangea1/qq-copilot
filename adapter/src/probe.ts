import { resolve } from "node:path";

import {
  SUPPORTED_PROTOCOL_VERSIONS,
  type SessionSummary,
} from "@microsoft/agent-host-protocol";

import {
  AhpCore,
  type CoreErrorEvent,
} from "./ahp-core.js";
import { defaultUserDataDirectory } from "./endpoint-registry.js";

interface ProbeResult {
  readonly endpointId: string;
  readonly pid: number;
  readonly advertisedProtocol: string;
  readonly selectedProtocol?: string;
  readonly sessions?: readonly SessionSummary[];
  readonly error?: string;
}

async function main(): Promise<void> {
  const userDataDirectory = parseUserDataDirectory(process.argv.slice(2));
  const endpointErrors = new Map<string, string>();
  const unscopedErrors: string[] = [];
  const onError = (event: CoreErrorEvent): void => {
    if (event.endpointId) {
      endpointErrors.set(event.endpointId, event.code);
    } else {
      unscopedErrors.push(event.code);
    }
  };
  const core = new AhpCore({
    userDataDirectory,
    clientId: "qq-copilot-ahp-probe",
    locale: "zh-CN",
    watch: false,
    callbacks: { onError },
  });

  try {
    await core.start();
    const catalogue = await core.listSessions();
    const results: ProbeResult[] = catalogue.endpoints.map((entry) => ({
      endpointId: entry.endpoint.id,
      pid: entry.endpoint.pid,
      advertisedProtocol: entry.endpoint.advertisedProtocol,
      ...(entry.selectedProtocol
        ? { selectedProtocol: entry.selectedProtocol }
        : {}),
      ...(entry.connection === "connected"
        ? { sessions: entry.sessions }
        : {
            error:
              endpointErrors.get(entry.endpoint.id) ?? entry.connection,
          }),
    }));

    process.stdout.write(
      `${JSON.stringify(
        {
          userDataDirectory,
          supportedProtocols: SUPPORTED_PROTOCOL_VERSIONS,
          endpoints: results,
          ...(unscopedErrors.length > 0
            ? { errors: unscopedErrors }
            : {}),
        },
        null,
        2,
      )}\n`,
    );
    if (!results.some((result) => result.selectedProtocol && result.sessions)) {
      process.exitCode = 2;
    }
  } finally {
    await core.stop();
  }
}

function parseUserDataDirectory(args: readonly string[]): string {
  if (args.length === 0) {
    return defaultUserDataDirectory();
  }
  if (args.length === 2 && args[0] === "--user-data-dir" && args[1]) {
    return resolve(args[1]);
  }
  throw new Error("usage: probe [--user-data-dir <path>]");
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : "unknown error";
  process.stderr.write(`AHP probe failed: ${message}\n`);
  process.exitCode = 1;
});
