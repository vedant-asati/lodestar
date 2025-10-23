// Plan:
// get configs from node startup
// spawn backfill worker using compatible(to be cloned) configs
// create other configs inside worker
// create and expose its api: (inside worker file)
// handler will use api to interact with it

import {EventEmitter} from "node:events";
import workerThreads from "node:worker_threads";
import {StrictEventEmitter} from "strict-event-emitter-types";
import {ModuleThread, Thread, Worker, spawn} from "@chainsafe/threads";
import {BeaconConfig, chainConfigToJson} from "@lodestar/config";
import {LoggerNode} from "@lodestar/logger/node";
import {SLOTS_PER_EPOCH} from "@lodestar/params";
import {BeaconStateAllForks, computeAnchorCheckpoint} from "@lodestar/state-transition";
import {Root, SignedBeaconBlock, Slot, phase0} from "@lodestar/types";
import {ErrorAborted, Logger, toRootHex} from "@lodestar/utils";
import {IBeaconChain} from "../../../chain/index.js";
import {IBeaconDb} from "../../../db/index.js";
import {Metrics} from "../../../metrics/metrics.js";
import {INetwork, NetworkEvent, NetworkEventData} from "../../../network/index.js";
import {ItTrigger} from "../../../util/itTrigger.js";
import {PeerIdStr} from "../../../util/peerId.js";
import {BackfillBlock, BackfillBlockHeader} from "../verify.js";
import {BackfillWorkerApi} from "./types.js";

export type BackfillSyncModules = {
  chain: IBeaconChain;
  db: IBeaconDb;
  network: INetwork;
  config: BeaconConfig;
  logger: LoggerNode;
  metrics: Metrics | null;
  anchorState: BeaconStateAllForks;
  wsCheckpoint?: phase0.Checkpoint;
  signal: AbortSignal;
};

// type BackfillModules = BackfillSyncModules & {
//   syncAnchor: BackFillSyncAnchor;
//   backfillStartFromSlot: Slot;
//   wsCheckpointHeader: BackfillBlockHeader | null;
// };

export type BackfillSyncOpts = {
  backfillBatchSize: number;
};

export enum BackfillSyncEvent {
  completed = "BackfillSync-completed",
}

export enum BackfillSyncMethod {
  rangesync = "rangesync",
  blockbyroot = "blockbyroot",
}

export enum BackfillSyncStatus {
  pending = "pending",
  syncing = "syncing",
  completed = "completed",
  aborted = "aborted",
}

type BackfillSyncEvents = {
  [BackfillSyncEvent.completed]: (oldestSlotSynced: Slot) => void;
};

type BackfillSyncEmitter = StrictEventEmitter<EventEmitter, BackfillSyncEvents>;

// type BackFillSyncAnchor =
//   | {
//       anchorBlock: SignedBeaconBlock;
//       anchorBlockRoot: Root;
//       anchorSlot: Slot;
//       lastBackSyncedBlock: BackfillBlock;
//     }
//   | {anchorBlock: null; anchorBlockRoot: Root; anchorSlot: null; lastBackSyncedBlock: BackfillBlock}
//   | {anchorBlock: null; anchorBlockRoot: Root; anchorSlot: Slot; lastBackSyncedBlock: null};

export class BackfillSyncWorkerHandler extends (EventEmitter as {new (): BackfillSyncEmitter}) {
  backfillWorkerApi: BackfillWorkerApi;
  private readonly logger: Logger;

  constructor(backfillWorkerApi: ModuleThread<BackfillWorkerApi>, modules: BackfillSyncModules) {
    super();
    this.backfillWorkerApi = backfillWorkerApi;
    this.logger = modules.logger;

    this.backfillWorkerApi.ping();

    this.sync()
      .then(() => {
        this.logger.info("Backfill sync completed from worker thread.");
      })
      .catch((err) => {
        this.logger.error("Backfill sync errored from worker thread.", err);
      });
  }

  static async init(opts: BackfillSyncOpts, modules: BackfillSyncModules): Promise<BackfillSyncWorkerHandler> {
    const {logger} = modules;

    logger.info("Initializing Backfill sync: BackfillWorkerHandler.");

    const compatibleModules = {
      chainConfigJson: chainConfigToJson(modules.config),
      genesisValidatorsRoot: modules.config.genesisValidatorsRoot,
      wsCheckpoint: modules.wsCheckpoint,
      // anchorState: modules.anchorState, // not sure of compatibility
      anchorSlot: modules.anchorState.latestBlockHeader.slot,
      anchorCp: computeAnchorCheckpoint(modules.config, modules.anchorState),

      loggerOpts: modules.logger.toOpts(),
      // db: IBeaconDb; // don't know how to share db connection
      // network: INetwork; // don't know how to share db connection
      // metrics: Metrics | null; // ignoring for now
    };
    const workerData = {
      opts,
      compatibleModules,
    };
    const workerOpts: ConstructorParameters<typeof Worker>[1] = {
      workerData,
    };
    if (globalThis.Bun) {
      workerOpts.suppressTranspileTS = true;
    } else {
      workerOpts.resourceLimits = {maxYoungGenerationSizeMb: 152};
    }

    logger.info("Creating Backfill thread: BackfillWorkerHandler.");
    const worker = new Worker("./backfillWorker.js", workerOpts);

    const backfillThreadApi = (await spawn(worker, {
      timeout: 5 * 60 * 1000,
    })) as unknown as ModuleThread<BackfillWorkerApi>;

    return new BackfillSyncWorkerHandler(backfillThreadApi, modules);
  }

  async sync(): Promise<void> {
    await this.backfillWorkerApi.sync();
  }

  close(): void {
    this.backfillWorkerApi.close();
    // this.network.events.off(NetworkEvent.peerConnected, this.addPeer);
    // this.network.events.off(NetworkEvent.peerDisconnected, this.removePeer);
  }

  ping(): void {
    this.backfillWorkerApi.ping();
  }
}
