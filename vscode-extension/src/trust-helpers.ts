export interface PendingTrustRequest {
  readonly request_id: string;
  readonly workspace_uri: string;
  readonly open_trust_ui: boolean;
  readonly trusted: boolean;
}

export interface InspectedStringConfiguration {
  readonly defaultValue?: string;
  readonly globalValue?: string;
  readonly workspaceValue?: string;
  readonly workspaceFolderValue?: string;
}

export function userConfigurationValue(
  inspected: InspectedStringConfiguration | undefined,
): string | undefined {
  const value = inspected?.globalValue ?? inspected?.defaultValue;
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

export function workspaceTrustCommandArgs(
  workspaceUris: readonly string[],
  trusted: boolean,
): string[] {
  const args = ["report-trust"];
  for (const workspaceUri of workspaceUris) {
    args.push("--workspace-uri", workspaceUri);
  }
  if (trusted) {
    args.push("--trusted");
  }
  return args;
}

export function matchingTrustRequest(
  workspaceUris: readonly string[],
  trusted: boolean,
  requests: readonly PendingTrustRequest[],
  openedRequestIds: ReadonlySet<string>,
): PendingTrustRequest | undefined {
  if (trusted) {
    return undefined;
  }
  return requests.find(
    (request) =>
      request.open_trust_ui &&
      !request.trusted &&
      workspaceUris.includes(request.workspace_uri) &&
      !openedRequestIds.has(request.request_id),
  );
}
