import worker from "node:worker_threads";
import {ModuleThread} from "@chainsafe/threads";
import {expose} from "@chainsafe/threads/worker";
import {chainConfigFromJson, createBeaconConfig} from "@lodestar/config";
import {getNodeLogger} from "@lodestar/logger/node";
import {NetworkEventBus} from "../../../network/events.js";
import {Clock} from "../../../util/clock.js";
import {BackfillSync} from "./backfillV2.js";
import {BackfillWorkerApi} from "./types.js";

const workerData = worker.workerData;
const parentPort = worker.parentPort;
if (!workerData) throw Error("workerData must be defined");
if (!parentPort) throw Error("parentPort must be defined");

// construct config data reqd for backfill
const config = createBeaconConfig(
  chainConfigFromJson(workerData.compatibleModules.chainConfigJson),
  workerData.compatibleModules.genesisValidatorsRoot
);

const logger = getNodeLogger(workerData.compatibleModules.loggerOpts);

// Alive and consistency check
logger.info("backfill worker started");

const abortController = new AbortController();

// Set up metrics
const modules = {
  config,
  logger,
  anchorSlot: workerData.compatibleModules.anchorSlot,
  anchorCp: workerData.compatibleModules.anchorCp,
  wsCheckpoint: workerData.compatibleModules.wsCheckpoint,
  // chain: // dont know
  // db: // dont know
  // network: // dont know
  // metrics: // skip for now
  signal: abortController.signal,
};

// logger.info("Initializing Backfill sync: BackfillWorker.");

const backfillclass = await BackfillSync.init(workerData.opts, modules);

const backfillWorkerApi: BackfillWorkerApi = {
  sync: async () => backfillclass.sync(),
  close: () => backfillclass.close(),
  ping: () => backfillclass.ping(),
};

expose(backfillWorkerApi /*as ModuleThread<BackfillWorkerApi>*/);
