import { log } from "./logger.ts";

export class CrawlerMetrics {
  private blocksProcessed = 0;
  private blocksProcessedPerSecond = 0;
  private accountsProcessed = 0;
  private accountsProcessedPerSecond = 0;
  private startTime: number;
  private lastReportTime: number;
  private reportIntervalMs: number;
  private intervalHandler: number | null = null;
  private queueInserted = 0;
  private queueInsertedPerSecond = 0;
  private accountProcessingTimes: number[] = [];
  private pendingAccountsExtracted = 0;
  private pendingAccountsExtractedPerSecond = 0;
  private blocksQueried = 0;
  private blocksQueriedPerSecond = 0;

  constructor(reportIntervalMs = 60000) { // Default to reporting every minute
    this.startTime = Date.now();
    this.lastReportTime = this.startTime;
    this.reportIntervalMs = reportIntervalMs;
    this.intervalHandler = setInterval(
      () => this.reportMetrics(),
      this.reportIntervalMs,
    );
  }

  public addBlocks(count: number): void {
    this.blocksProcessed += count;
    this.blocksProcessedPerSecond += count;
  }

  public addAccount(count: number = 1): void {
    this.accountsProcessed += count;
    this.accountsProcessedPerSecond += count;
  }

  public addQueueInserted(count: number): void {
    this.queueInserted += count;
    this.queueInsertedPerSecond += count;
  }

  public addAccountProcessingTime(timeMs: number): void {
    this.accountProcessingTimes.push(timeMs);
  }

  public addPendingAccountExtraction(count: number = 1): void {
    this.pendingAccountsExtracted += count;
    this.pendingAccountsExtractedPerSecond += count;
  }

  public addBlocksQueried(count: number): void {
    this.blocksQueried += count;
    this.blocksQueriedPerSecond += count;
  }

  private reportMetrics(): void {
    const now = Date.now();
    const elapsedSeconds = (now - this.lastReportTime) / 1000;
    const blocksPerSecond = this.blocksProcessedPerSecond / elapsedSeconds;
    const accountsPerSecond = this.accountsProcessedPerSecond / elapsedSeconds;
    const queueInsertedPerSecond = this.queueInsertedPerSecond / elapsedSeconds;
    const pendingExtractedPerSecond = this.pendingAccountsExtractedPerSecond /
      elapsedSeconds;
    const blocksQueriedPerSecond = this.blocksQueriedPerSecond / elapsedSeconds;

    // Process account processing times
    const sortedTimes = [...this.accountProcessingTimes].sort((a, b) => a - b);
    const length = sortedTimes.length;

    const getPercentile = (p: number) => {
      if (length === 0) return 0;
      const index = Math.floor(length * p / 100);
      return sortedTimes[Math.min(index, length - 1)];
    };

    log.info(
      `Crawler Metrics:
      Total Blocks: ${this.blocksProcessed.toLocaleString()}
      Total Accounts: ${this.accountsProcessed.toLocaleString()}
      Total Pending Extracted: ${this.pendingAccountsExtracted.toLocaleString()}
      Total Blocks Queried: ${this.blocksQueried.toLocaleString()}
      Blocks/sec: ${blocksPerSecond.toFixed(2)}
      Accounts/sec: ${accountsPerSecond.toFixed(2)}
      Queue Inserted/sec: ${queueInsertedPerSecond.toFixed(2)}
      Pending Extracted/sec: ${pendingExtractedPerSecond.toFixed(2)}
      Blocks Queried/sec: ${blocksQueriedPerSecond.toFixed(2)}
      Processing Times (ms):
        Median: ${getPercentile(50).toFixed(2)}
        90th %ile: ${getPercentile(90).toFixed(2)}
        99th %ile: ${getPercentile(99).toFixed(2)}
      Running time: ${(now - this.startTime) / (60 * 1000)} minutes`,
    );

    this.blocksProcessedPerSecond = 0;
    this.accountsProcessedPerSecond = 0;
    this.queueInsertedPerSecond = 0;
    this.pendingAccountsExtractedPerSecond = 0;
    this.blocksQueriedPerSecond = 0;
    this.accountProcessingTimes = [];
    this.lastReportTime = now;
  }

  public stop(): void {
    this.cleanup();
  }

  public cleanup(): void {
    if (this.intervalHandler) {
      clearInterval(this.intervalHandler);
      this.intervalHandler = null;
    }
  }

  public reset(): void {
    this.blocksProcessed = 0;
    this.accountsProcessed = 0;
    this.pendingAccountsExtracted = 0;
    this.blocksQueried = 0;
    this.startTime = Date.now();
    this.lastReportTime = this.startTime;
    if (!this.intervalHandler) {
      this.intervalHandler = setInterval(
        () => this.reportMetrics(),
        this.reportIntervalMs,
      );
    }
  }
}
