import worker from "node:worker_threads";
import {ModuleThread} from "@chainsafe/threads";
import {expose} from "@chainsafe/threads/worker";
import {BackfillSync} from "./backfillV2.ts";
import {BackfillWorkerApi} from "./types.ts";

const workerData = worker.workerData;
const parentPort = worker.parentPort;
if (!workerData) throw Error("workerData must be defined");
if (!parentPort) throw Error("parentPort must be defined");

// construct config data reqd for backfill

// biome-ignore lint/suspicious/noConsole: jsr test
console.log("Initializing Backfill sync: BackfillWorker.");

const backfillclass = await BackfillSync.init(workerData.opts);

const backfillWorkerApi: BackfillWorkerApi = {
  sync: async () => backfillclass.sync(),
  close: () => backfillclass.close(),
  ping: () => backfillclass.ping(),
};

expose(backfillWorkerApi /*as ModuleThread<BackfillWorkerApi>*/);
