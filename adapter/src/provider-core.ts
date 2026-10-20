import {
  AhpOperationError,
  type AhpSessionBinding,
  type CatalogueSnapshot,
} from "./ahp-core.js";

export interface ProviderCore {
  readonly catalogue: CatalogueSnapshot;
  start(): Promise<CatalogueSnapshot | void>;
  stop(): Promise<void>;
  refreshEndpoints(): Promise<CatalogueSnapshot>;
  bindSession(endpointId: string, sessionUri: string): Promise<AhpSessionBinding>;
}

export class CombinedCore implements ProviderCore {
  readonly #cores: readonly ProviderCore[];

  constructor(cores: readonly ProviderCore[]) {
    this.#cores = cores;
  }

  get catalogue(): CatalogueSnapshot {
    return {
      revision: this.#cores.reduce((sum, core) => sum + core.catalogue.revision, 0),
      endpoints: this.#cores.flatMap((core) => core.catalogue.endpoints),
    };
  }

  async start(): Promise<CatalogueSnapshot> {
    await Promise.all(this.#cores.map((core) => core.start()));
    return this.catalogue;
  }

  async stop(): Promise<void> {
    const results = await Promise.allSettled(this.#cores.map((core) => core.stop()));
    const errors = results.flatMap((result) => result.status === "rejected" ? [result.reason] : []);
    if (errors.length > 0) {
      throw new AggregateError(errors, "provider-stop-failed");
    }
  }

  async refreshEndpoints(): Promise<CatalogueSnapshot> {
    await Promise.all(this.#cores.map((core) => core.refreshEndpoints()));
    return this.catalogue;
  }

  async bindSession(endpointId: string, sessionUri: string): Promise<AhpSessionBinding> {
    const core = this.#cores.find((candidate) =>
      candidate.catalogue.endpoints.some((entry) => entry.endpoint.id === endpointId));
    if (!core) {
      throw new AhpOperationError("endpoint-not-found", "Provider endpoint is not available");
    }
    return core.bindSession(endpointId, sessionUri);
  }
}
