// get configs from node startup
// spawn backfill worker using the configs
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
  //   syncAnchor: BackFillSyncAnchor;

  //   private readonly chain: IBeaconChain;
  //   private readonly network: INetwork;
  //   private readonly db: IBeaconDb;
  //   private readonly config: BeaconConfig;
  // private readonly logger: Logger;
  //   private readonly metrics: Metrics | null;
  //   private opts: BackfillSyncOpts;
  //   private wsCheckpointHeader: BackfillBlockHeader | null;
  //   private backfillStartFromSlot: Slot;

  //   private processor = new ItTrigger();
  //   private peers = new Set<PeerIdStr>();
  //   private status: BackfillSyncStatus = BackfillSyncStatus.pending;
  //   private signal: AbortSignal;

  constructor(backfillWorkerApi: ModuleThread<BackfillWorkerApi>) {
    super();
    this.backfillWorkerApi = backfillWorkerApi;
    // biome-ignore lint/suspicious/noConsole: jsr test
    console.log("Triggering backfillWorkerApi.ping(): BackfillWorkerHandler.");
    this.backfillWorkerApi.ping();
    this.sync()
      .then(() => {
        // biome-ignore lint/suspicious/noConsole: jsr test
        console.log("Backfill sync completed from worker thread.");
      })
      .catch((err) => {
        // biome-ignore lint/suspicious/noConsole: jsr test
        console.error("Backfill sync errored from worker thread.", err);
      });

    // this.syncAnchor = modules.syncAnchor;
    // this.backfillStartFromSlot = modules.backfillStartFromSlot;
    // this.wsCheckpointHeader = modules.wsCheckpointHeader;

    // this.chain = modules.chain;
    // this.network = modules.network;
    // this.db = modules.db;
    // this.config = modules.config;
    // this.logger = modules.logger;
    // this.metrics = modules.metrics;

    // this.opts = opts;
    // this.network.events.on(NetworkEvent.peerConnected, this.addPeer);
    // this.network.events.on(NetworkEvent.peerDisconnected, this.removePeer);
    // this.signal = modules.signal;

    // this.sync()
    //   .then(() => {
    //     this.logger.info("BackfillSync completed");
    //     this.close();
    //   })
    //   .catch((e) => {
    //     this.logger.error("BackfillSync processor error", e);
    //     this.status = BackfillSyncStatus.aborted;
    //     this.close();
    //   });
  }

  static async init(opts: BackfillSyncOpts, modules: BackfillSyncModules): Promise<BackfillSyncWorkerHandler> {
    // biome-ignore lint/suspicious/noConsole: jsr test
    console.log("Initializing Backfill sync: BackfillWorkerHandler.");
    // const {config, anchorState, wsCheckpoint, logger} = modules;

    // const {checkpoint: anchorCp} = computeAnchorCheckpoint(config, anchorState);
    // const anchorSlot = anchorState.latestBlockHeader.slot;
    // const syncAnchor = {
    //   anchorBlock: null,
    //   anchorBlockRoot: anchorCp.root,
    //   anchorSlot,
    //   lastBackSyncedBlock: null,
    // };

    // const backfillStartFromSlot = anchorSlot;
    // logger.info("Initializing from Checkpoint", {
    //   root: toRootHex(anchorCp.root),
    //   epoch: anchorCp.epoch,
    //   backfillStartFromSlot,
    // });

    // const wsCheckpointHeader: BackfillBlockHeader | null = wsCheckpoint
    //   ? {root: wsCheckpoint.root, slot: wsCheckpoint.epoch * SLOTS_PER_EPOCH}
    //   : null;

    // const secretMessage = "JGD";
    // const workerData = {
    //   secretMessage,
    // };

    // config: BeaconConfig;
    // logger: LoggerNode;
    // anchorState: BeaconStateAllForks;
    // wsCheckpoint?: phase0.Checkpoint;

    // chain: IBeaconChain; // this.chain.bls, this.chain.getHeadState()
    // db: IBeaconDb;
    // network: INetwork;
    // metrics: Metrics | null;

    // signal: AbortSignal;

    const compatibleModules = {
      chainConfigJson: chainConfigToJson(modules.config), // compatible
      genesisValidatorsRoot: modules.config.genesisValidatorsRoot,
      wsCheckpoint: modules.wsCheckpoint, // compatible
      // anchorState: modules.anchorState, // not sure
      anchorSlot: modules.anchorState.latestBlockHeader.slot,
      anchorCp: computeAnchorCheckpoint(modules.config, modules.anchorState),

      loggerOpts: modules.logger.toOpts(),
      // db: IBeaconDb; // don't know how to share db connection
      // network: INetwork; // don't know how to share db connection
      // metrics: Metrics | null; // ignoring for now
    };
    const workerData = {
      opts, // compatible
      compatibleModules,
      // modules,
      // secretMessage, // compatible
      //   syncAnchor,
      //   backfillStartFromSlot, // syncAnchor.anchorSlot
      //   wsCheckpointHeader, // from checkpoint sync
      //   ...modules,
    };
    const workerOpts: ConstructorParameters<typeof Worker>[1] = {
      workerData,
    };
    if (globalThis.Bun) {
      workerOpts.suppressTranspileTS = true;
    } else {
      workerOpts.resourceLimits = {maxYoungGenerationSizeMb: 152};
    }

    // biome-ignore lint/suspicious/noConsole: jsr test
    console.log("Creating Backfill thread: BackfillWorkerHandler.");
    const worker = new Worker("./backfillWorker.js", workerOpts);

    const backfillThreadApi = (await spawn(worker, {
      timeout: 5 * 60 * 1000,
    })) as unknown as ModuleThread<BackfillWorkerApi>;

    // return new BackfillSyncWorkerHandler(opts, {
    //   backfillThreadApi,
    //   syncAnchor,
    //   backfillStartFromSlot, // syncAnchor.anchorSlot
    //   wsCheckpointHeader, // from checkpoint sync
    //   ...modules,
    // }) as T;
    return new BackfillSyncWorkerHandler(backfillThreadApi);
  }

  async sync(): Promise<void> {
    await this.backfillWorkerApi.sync();
    // this.logger.info("Trying to sync.");
  }

  close(): void {
    this.backfillWorkerApi.close();
    // this.network.events.off(NetworkEvent.peerConnected, this.addPeer);
    // this.network.events.off(NetworkEvent.peerDisconnected, this.removePeer);
    // this.processor.end(new ErrorAborted("BackfillSync"));
  }

  ping(): void {
    this.backfillWorkerApi.ping();
    // this.logger.info("Ping.");
  }

  //   private addPeer = (data: NetworkEventData[NetworkEvent.peerConnected]): void => {
  //     const requiredSlot = this.syncAnchor.lastBackSyncedBlock?.slot ?? this.backfillStartFromSlot;
  //     this.logger.debug("Add peer", {peerhead: data.status.headSlot, requiredSlot});
  //     if (data.status.headSlot >= requiredSlot) {
  //       this.peers.add(data.peer);
  //       this.processor.trigger();
  //     }
  //   };

  //   private removePeer = (data: NetworkEventData[NetworkEvent.peerDisconnected]): void => {
  //     this.peers.delete(data.peer);
  //   };
}
