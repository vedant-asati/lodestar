import {EventEmitter} from "node:events";
import {BroadcastChannel} from "node:worker_threads";
import {StrictEventEmitter} from "strict-event-emitter-types";
import {BeaconConfig} from "@lodestar/config";
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

export type BackfillSyncModules = {
  chain: IBeaconChain;
  db: IBeaconDb;
  network: INetwork;
  config: BeaconConfig;
  logger: Logger;
  metrics: Metrics | null;
  anchorState: BeaconStateAllForks;
  wsCheckpoint?: phase0.Checkpoint;
  signal: AbortSignal;
};

// biome-ignore lint/correctness/noUnusedVariables: test
type BackfillModules = BackfillSyncModules & {
  syncAnchor: BackFillSyncAnchor;
  backfillStartFromSlot: Slot;
  wsCheckpointHeader: BackfillBlockHeader | null;
};

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

type BackFillSyncAnchor =
  | {
      anchorBlock: SignedBeaconBlock;
      anchorBlockRoot: Root;
      anchorSlot: Slot;
      lastBackSyncedBlock: BackfillBlock;
    }
  | {anchorBlock: null; anchorBlockRoot: Root; anchorSlot: null; lastBackSyncedBlock: BackfillBlock}
  | {anchorBlock: null; anchorBlockRoot: Root; anchorSlot: Slot; lastBackSyncedBlock: null}
  | {anchorBlock: null; anchorBlockRoot: null; anchorSlot: null; lastBackSyncedBlock: null};

// init this class from worker
export class BackfillSync extends (EventEmitter as {new (): BackfillSyncEmitter}) {
  // syncAnchor: BackFillSyncAnchor;

  // private readonly chain: IBeaconChain;
  // private readonly network: INetwork;
  // private readonly db: IBeaconDb;
  // private readonly config: BeaconConfig;
  // private readonly logger: Logger;
  // private readonly metrics: Metrics | null;
  private opts: BackfillSyncOpts;
  // private wsCheckpointHeader: BackfillBlockHeader | null;
  // private backfillStartFromSlot: Slot;

  private processor = new ItTrigger();
  private peers = new Set<PeerIdStr>();
  private status: BackfillSyncStatus = BackfillSyncStatus.pending;
  // private signal: AbortSignal;

  private bc: BroadcastChannel;

  constructor(opts: BackfillSyncOpts) {
    super();

    // this.syncAnchor = modules.syncAnchor;
    // this.backfillStartFromSlot = modules.backfillStartFromSlot;
    // this.wsCheckpointHeader = modules.wsCheckpointHeader;

    // this.chain = modules.chain;
    // this.network = modules.network;
    // this.db = modules.db;
    // this.config = modules.config;
    // this.logger = modules.logger;
    // this.metrics = modules.metrics;

    // // biome-ignore lint/suspicious/noConsole: jsr test
    // console.log("Initializing Backfill sync: BackfillV2.constructor.");

    // // biome-ignore lint/suspicious/noConsole: jsr test
    // console.log("Creating BroadcastChannel: BackfillV2.constructor.");
    this.bc = new BroadcastChannel("test_channel");
    // // biome-ignore lint/suspicious/noConsole: jsr test
    // console.log("Created BroadcastChannel: BackfillV2.constructor.\n", this.bc);

    this.opts = opts;
    // this.network.events.on(NetworkEvent.peerConnected, this.addPeer);
    // this.network.events.on(NetworkEvent.peerDisconnected, this.removePeer);
    this.bc.onmessage = (event) => {
      // biome-ignore lint/suspicious/noConsole: testing
      console.log("It works from backfillV2.ts", event.data);
      switch (event.data.event) {
        case NetworkEvent.peerConnected:
          // biome-ignore lint/suspicious/noConsole: testing
          console.log("Inside Backfill Thread. NetworkEvent.peerConnected: ", event?.data?.message?.peer!);
          this.addPeer(event.data.message as NetworkEventData[NetworkEvent.peerConnected]);
          break;
        case NetworkEvent.peerDisconnected:
          // biome-ignore lint/suspicious/noConsole: testing
          console.log("Inside Backfill Thread. NetworkEvent.peerDisconnected: ", event?.data?.message?.peer!);
          this.removePeer(event.data.message as NetworkEventData[NetworkEvent.peerDisconnected]);
          break;
        default:
        // do nothing
      }
    };
    // this.signal = modules.signal;

    this.sync()
      .then(() => {
        // biome-ignore lint/suspicious/noConsole: testing
        console.log("BackfillSync completed");
        this.close();
      })
      .catch((e) => {
        // biome-ignore lint/suspicious/noConsole: testing
        console.log("BackfillSync processor error", e);
        this.status = BackfillSyncStatus.aborted;
        this.close();
      });
  }

  static async init<T extends BackfillSync = BackfillSync>(
    opts: BackfillSyncOpts
    // modules: BackfillSyncModules
  ): Promise<T> {
    // const {config, anchorState, wsCheckpoint, logger} = modules;

    // const {checkpoint: anchorCp} = computeAnchorCheckpoint(config, anchorState);
    // const anchorSlot = anchorState.latestBlockHeader.slot;
    // const syncAnchor = {
    //   anchorBlock: null,
    //   anchorBlockRoot: null,
    //   // anchorBlockRoot: anchorCp.root,
    //   // anchorSlot,
    //   anchorSlot: null,
    //   lastBackSyncedBlock: null,
    // };

    // const backfillStartFromSlot = 20;
    // // logger.info("Initializing from Checkpoint", {
    // //   root: toRootHex(anchorCp.root),
    // //   epoch: anchorCp.epoch,
    // //   backfillStartFromSlot,
    // // });

    // const wsCheckpointHeader: BackfillBlockHeader | null =
    //   // wsCheckpoint
    //   //   ? {root: wsCheckpoint.root, slot: wsCheckpoint.epoch * SLOTS_PER_EPOCH}
    //   // :
    //   null;

    // biome-ignore lint/suspicious/noConsole: jsr test
    console.log("Initializing Backfill sync: BackfillV2.init.");
    return new BackfillSync(opts) as T;
  }

  async sync(): Promise<void> {
    // biome-ignore lint/suspicious/noConsole: testing
    console.log("Trying to sync.");
    let i = 20;
    const timerId = setInterval(() => {
      // biome-ignore lint/suspicious/noConsole: testing
      console.log("Syncing slot=", i--);
    }, 2000);
    setTimeout(() => {
      clearInterval(timerId);
    }, 20000);
    // run for 5 mins
    await new Promise((res) => setTimeout(res, 5 * 60 * 1000));
  }

  close(): void {
    // this.network.events.off(NetworkEvent.peerConnected, this.addPeer);
    // this.network.events.off(NetworkEvent.peerDisconnected, this.removePeer);
    this.bc.close();
    this.processor.end(new ErrorAborted("BackfillSync"));
  }

  ping(): void {
    // biome-ignore lint/suspicious/noConsole: testing
    console.log("Ping.");
  }

  private addPeer = (data: NetworkEventData[NetworkEvent.peerConnected]): void => {
    // const requiredSlot = this.syncAnchor.lastBackSyncedBlock?.slot ?? this.backfillStartFromSlot;
    // // biome-ignore lint/suspicious/noConsole: testing
    // console.log("Add peer", {peerhead: data.status.headSlot, requiredSlot});
    // biome-ignore lint/suspicious/noConsole: testing
    console.log("Add peer");
    // if (data.status.headSlot >= requiredSlot) {
    this.peers.add(data.peer);
    this.processor.trigger();
    // }
  };

  private removePeer = (data: NetworkEventData[NetworkEvent.peerDisconnected]): void => {
    this.peers.delete(data.peer);
  };
}
