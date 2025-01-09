import { log } from "./logger.ts";

export class CrawlerMetrics {
  private blocksProcessed = 0;
  private accountsProcessed = 0;
  private startTime: number;
  private lastReportTime: number;
  private reportIntervalMs: number;

  constructor(reportIntervalMs = 60000) { // Default to reporting every minute
    this.startTime = Date.now();
    this.lastReportTime = this.startTime;
    this.reportIntervalMs = reportIntervalMs;
  }

  public addBlocks(count: number): void {
    this.blocksProcessed += count;
    this.checkAndReport();
  }

  public addAccount(): void {
    this.accountsProcessed++;
    this.checkAndReport();
  }

  private checkAndReport(): void {
    const now = Date.now();
    if (now - this.lastReportTime >= this.reportIntervalMs) {
      this.reportMetrics();
      this.lastReportTime = now;
    }
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

  public reset(): void {
    this.blocksProcessed = 0;
    this.accountsProcessed = 0;
    this.startTime = Date.now();
    this.lastReportTime = this.startTime;
  }
}
