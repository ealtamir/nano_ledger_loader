import { log } from "./logger.ts";

export class CrawlerMetrics {
  private blocksProcessed = 0;
  private accountsProcessed = 0;
  private startTime: number;
  private lastReportTime: number;
  private reportIntervalMs: number;
  private intervalHandler: number | null = null;

  constructor(reportIntervalMs = 60000) { // Default to reporting every minute
    this.startTime = Date.now();
    this.lastReportTime = this.startTime;
    this.reportIntervalMs = reportIntervalMs;
    this.intervalHandler = setInterval(() => this.reportMetrics(), this.reportIntervalMs);
  }

  public addBlocks(count: number): void {
    this.blocksProcessed += count;
  }

  public addAccount(): void {
    this.accountsProcessed++;
  }


  private reportMetrics(): void {
    const elapsedSeconds = (Date.now() - this.startTime) / 1000;
    const blocksPerSecond = this.blocksProcessed / elapsedSeconds;
    const accountsPerSecond = this.accountsProcessed / elapsedSeconds;

    log.info(
      `Crawler Metrics:
      Total Blocks: ${this.blocksProcessed.toLocaleString()}
      Total Accounts: ${this.accountsProcessed.toLocaleString()}
      Blocks/sec: ${blocksPerSecond.toFixed(2)}
      Accounts/sec: ${accountsPerSecond.toFixed(2)}
      Running time: ${(elapsedSeconds / 60).toFixed(2)} minutes`
    );
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
      this.intervalHandler = setInterval(() => this.reportMetrics(), this.reportIntervalMs);
    }
  }
}
