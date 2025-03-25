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

  private reportMetrics(): void {
    const now = Date.now();
    const elapsedSeconds = (now - this.lastReportTime) / 1000;
    const blocksPerSecond = this.blocksProcessedPerSecond / elapsedSeconds;
    const accountsPerSecond = this.accountsProcessedPerSecond / elapsedSeconds;

    log.info(
      `Crawler Metrics:
      Total Blocks: ${this.blocksProcessed.toLocaleString()}
      Total Accounts: ${this.accountsProcessed.toLocaleString()}
      Blocks/sec: ${blocksPerSecond.toFixed(2)}
      Accounts/sec: ${accountsPerSecond.toFixed(2)}
      Running time: ${(now - this.startTime) / (60 * 1000)} minutes`,
    );

    this.blocksProcessedPerSecond = 0;
    this.accountsProcessedPerSecond = 0;
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
