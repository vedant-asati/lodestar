export type BackfillWorkerApi = {
  sync(): Promise<void>;
  close(): void;
  ping(): void;
};
