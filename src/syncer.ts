import { Database } from "sqlite";
import { Pool } from "postgres";
import { log } from "./logger.ts";
import { config } from "./config_loader.ts";

interface SyncState {
  last_synced_timestamp: number;
}

interface BlockRow {
  hash: string;
  type: string | null;
  account: string | null;
  previous: string | null;
  representative: string | null;
  balance: string | null; // TEXT to preserve 128-bit integer precision
  link: string | null;
  link_as_account: string | null;
  destination: string | null;
  signature: string | null;
  work: string | null;
  subtype: string | null;
  height: number | null;
  confirmed: boolean | null;
  successor: string | null;
  amount: string | null; // TEXT to preserve 128-bit integer precision
  local_timestamp: number | null;
}

/**
 * Syncer class that synchronizes blocks from SQLite to PostgreSQL.
 * Runs periodically and tracks sync progress in SQLite.
 *
 * Design decisions:
 * - Uses SQLite as source of truth, PostgreSQL as destination
 * - Tracks progress via last_synced_timestamp in SQLite
 * - Processes blocks in batches to avoid memory issues
 * - Prevents overlapping syncs with isSyncing flag
 * - Stores run statistics for monitoring/debugging
 */
export class Syncer {
  private sqlite: Database;
  private pgPool: Pool;
  private isSyncing: boolean = false;
  private shouldContinue: boolean = true;

  constructor(sqlite: Database, pgPool: Pool) {
    this.sqlite = sqlite;
    this.pgPool = pgPool;
    this.initializeTables();
  }

  /**
   * Initialize SQLite tables for sync state and statistics tracking.
   */
  private initializeTables(): void {
    // Table to track sync progress
    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS sync_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        last_synced_timestamp INTEGER NOT NULL DEFAULT 0
      )
    `);

    // Table to track sync run statistics
    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS sync_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at INTEGER NOT NULL,
        completed_at INTEGER,
        blocks_checked INTEGER NOT NULL DEFAULT 0,
        blocks_inserted INTEGER NOT NULL DEFAULT 0,
        last_timestamp_processed INTEGER,
        status TEXT NOT NULL DEFAULT 'running'
      )
    `);

    // Ensure sync_state has an initial row
    const existing = this.sqlite.prepare(
      "SELECT 1 FROM sync_state WHERE id = 1",
    ).get();
    if (!existing) {
      this.sqlite.exec(
        "INSERT INTO sync_state (id, last_synced_timestamp) VALUES (1, 0)",
      );
    }

    log.info("Syncer tables initialized");
  }

  /**
   * Get the last synced timestamp from SQLite.
   */
  private getSyncState(): SyncState {
    const row = this.sqlite.prepare(
      "SELECT last_synced_timestamp FROM sync_state WHERE id = 1",
    ).get() as { last_synced_timestamp: number } | undefined;

    return {
      last_synced_timestamp: row?.last_synced_timestamp ?? 0,
    };
  }

  /**
   * Update the last synced timestamp in SQLite.
   */
  private updateSyncState(timestamp: number): void {
    this.sqlite.prepare(
      "UPDATE sync_state SET last_synced_timestamp = ? WHERE id = 1",
    ).run(timestamp);
  }

  /**
   * Create a new sync run record and return its ID.
   */
  private startSyncRun(): number {
    const now = Math.floor(Date.now() / 1000);
    this.sqlite.prepare(
      "INSERT INTO sync_runs (started_at, status) VALUES (?, 'running')",
    ).run(now);

    const result = this.sqlite.prepare(
      "SELECT last_insert_rowid() as id",
    ).get() as { id: number };

    return result.id;
  }

  /**
   * Complete a sync run with statistics.
   */
  private completeSyncRun(
    runId: number,
    blocksChecked: number,
    blocksInserted: number,
    lastTimestamp: number | null,
    status: "completed" | "failed" | "interrupted",
  ): void {
    const now = Math.floor(Date.now() / 1000);
    this.sqlite.prepare(`
      UPDATE sync_runs 
      SET completed_at = ?, blocks_checked = ?, blocks_inserted = ?, 
          last_timestamp_processed = ?, status = ?
      WHERE id = ?
    `).run(now, blocksChecked, blocksInserted, lastTimestamp, status, runId);
  }

  /**
   * Fetch block hashes from SQLite starting from a timestamp.
   * Returns blocks ordered by local_timestamp.
   */
  private fetchBlockHashes(
    fromTimestamp: number,
    limit: number,
  ): { hash: string; local_timestamp: number }[] {
    const rows = this.sqlite.prepare(`
      SELECT hash, local_timestamp 
      FROM blocks 
      WHERE local_timestamp >= ? 
      ORDER BY local_timestamp ASC 
      LIMIT ?
    `).all(fromTimestamp, limit) as { hash: string; local_timestamp: number }[];

    return rows;
  }

  /**
   * Check which hashes are missing in PostgreSQL.
   */
  private async findMissingHashes(hashes: string[]): Promise<Set<string>> {
    if (hashes.length === 0) return new Set();

    using client = await this.pgPool.connect();
    const result = await client.queryObject<{ hash: string }>(
      "SELECT hash FROM block_confirmations WHERE hash = ANY($1)",
      [hashes],
    );

    const existingHashes = new Set(result.rows.map((r) => r.hash));
    return new Set(hashes.filter((h) => !existingHashes.has(h)));
  }

  /**
   * Fetch full block rows from SQLite by hashes.
   * Note: balance and amount are cast to TEXT to preserve 128-bit integer precision.
   * Reason: SQLite stores large integers as REAL (64-bit float) which loses precision.
   *         Casting to TEXT preserves the original string representation.
   */
  private fetchBlocksByHashes(hashes: string[]): BlockRow[] {
    if (hashes.length === 0) return [];

    // SQLite doesn't support array parameters directly, so we use IN clause
    const placeholders = hashes.map(() => "?").join(", ");
    const query = `
      SELECT hash, type, account, previous, representative, 
             CAST(balance AS TEXT) as balance,
             link, link_as_account, destination, signature, work,
             subtype, height, confirmed, successor, 
             CAST(amount AS TEXT) as amount, 
             local_timestamp
      FROM blocks
      WHERE hash IN (${placeholders})
    `;

    return this.sqlite.prepare(query).all(...hashes) as BlockRow[];
  }

  /**
   * Insert blocks into PostgreSQL block_confirmations table.
   * Uses unnest for efficient bulk insertion.
   */
  private async insertBlocksToPostgres(blocks: BlockRow[]): Promise<number> {
    if (blocks.length === 0) return 0;

    const columns = [
      "hash",
      "type",
      "account",
      "previous",
      "representative",
      "balance",
      "link",
      "link_as_account",
      "destination",
      "signature",
      "work",
      "subtype",
      "height",
      "confirmed",
      "successor",
      "amount",
      "local_timestamp",
    ];

    const dataTypes = [
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "numeric",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "integer",
      "boolean",
      "varchar",
      "numeric",
      "timestamptz",
    ];

    // Transpose to columnar format for unnest
    const columnarData: unknown[][] = columns.map(() => []);

    for (const block of blocks) {
      columnarData[0].push(block.hash);
      columnarData[1].push(block.type);
      columnarData[2].push(block.account);
      columnarData[3].push(block.previous);
      columnarData[4].push(block.representative);
      columnarData[5].push(block.balance); // Already string from SQLite CAST
      columnarData[6].push(block.link);
      columnarData[7].push(block.link_as_account);
      columnarData[8].push(block.destination);
      columnarData[9].push(block.signature);
      columnarData[10].push(block.work);
      columnarData[11].push(block.subtype);
      columnarData[12].push(block.height);
      columnarData[13].push(block.confirmed);
      columnarData[14].push(block.successor);
      columnarData[15].push(block.amount); // Already string from SQLite CAST
      // Convert Unix timestamp to ISO string for timestamptz
      columnarData[16].push(
        block.local_timestamp
          ? new Date(block.local_timestamp * 1000).toISOString()
          : null,
      );
    }

    const unnestClauses = columns
      .map((_, i) => `unnest($${i + 1}::${dataTypes[i]}[])`)
      .join(", ");

    const query = `
      INSERT INTO block_confirmations (${columns.join(", ")})
      SELECT ${unnestClauses}
      ON CONFLICT (hash, local_timestamp) DO NOTHING;
    `;

    using client = await this.pgPool.connect();
    const result = await client.queryArray(query, columnarData);
    return result.rowCount ?? 0;
  }

  /**
   * Process a single batch of blocks.
   * Returns stats and whether there are more blocks to process.
   */
  private async syncBatch(
    fromTimestamp: number,
    batchSize: number,
  ): Promise<{
    checked: number;
    inserted: number;
    nextTimestamp: number;
    hasMore: boolean;
  }> {
    // Fetch batch of block hashes from SQLite
    const blockRefs = this.fetchBlockHashes(fromTimestamp, batchSize);

    if (blockRefs.length === 0) {
      return {
        checked: 0,
        inserted: 0,
        nextTimestamp: fromTimestamp,
        hasMore: false,
      };
    }

    const hashes = blockRefs.map((b) => b.hash);
    let inserted = 0;

    // Find which hashes are missing in PostgreSQL
    const missingHashes = await this.findMissingHashes(hashes);

    if (missingHashes.size > 0) {
      // Fetch full block data from SQLite
      const blocksToInsert = this.fetchBlocksByHashes([...missingHashes]);

      // Insert into PostgreSQL
      inserted = await this.insertBlocksToPostgres(blocksToInsert);
    }

    // Get max timestamp from this batch
    // Important: We need to handle the case where multiple blocks have the same timestamp
    const maxTimestamp = Math.max(...blockRefs.map((b) => b.local_timestamp));
    const blocksAtMaxTimestamp = blockRefs.filter(
      (b) => b.local_timestamp === maxTimestamp,
    ).length;

    let nextTimestamp: number;

    // If we got a full batch and the last blocks all have the same timestamp,
    // there might be more blocks with that timestamp. Keep the same timestamp.
    // Otherwise, move past it.
    if (blockRefs.length === batchSize && blocksAtMaxTimestamp === batchSize) {
      // All blocks have the same timestamp - this is an edge case
      // We need a secondary ordering mechanism. Since we don't have one,
      // log a warning and proceed (might cause duplicate processing, but ON CONFLICT handles it)
      log.warn(
        `All ${batchSize} blocks have timestamp ${maxTimestamp}. May cause duplicate processing.`,
      );
      nextTimestamp = maxTimestamp;
    } else if (blockRefs.length < batchSize) {
      // We got less than a full batch, so we've processed all available blocks
      nextTimestamp = maxTimestamp + 1;
    } else {
      // Full batch with mixed timestamps - move past the max timestamp
      nextTimestamp = maxTimestamp + 1;
    }

    return {
      checked: blockRefs.length,
      inserted,
      nextTimestamp,
      hasMore: blockRefs.length === batchSize,
    };
  }

  /**
   * Perform a full sync - loops continuously until all blocks are synced.
   * Returns the total number of blocks checked and inserted.
   */
  public async sync(): Promise<{ checked: number; inserted: number }> {
    // Prevent overlapping syncs
    if (this.isSyncing) {
      log.warn("Sync already in progress, skipping this cycle");
      return { checked: 0, inserted: 0 };
    }

    this.isSyncing = true;
    const runId = this.startSyncRun();
    let totalChecked = 0;
    let totalInserted = 0;
    let lastProcessedTimestamp: number | null = null;
    let batchCount = 0;

    try {
      const state = this.getSyncState();
      const batchSize = config.syncer_batch_size;
      let currentTimestamp = state.last_synced_timestamp;

      log.info(`Starting full sync from timestamp ${currentTimestamp}`);

      // Loop until we've processed all available blocks
      while (this.shouldContinue) {
        const result = await this.syncBatch(currentTimestamp, batchSize);
        batchCount++;

        totalChecked += result.checked;
        totalInserted += result.inserted;

        if (result.checked > 0) {
          lastProcessedTimestamp = result.nextTimestamp;
          // Update sync state after each batch to track progress
          this.updateSyncState(result.nextTimestamp);
          currentTimestamp = result.nextTimestamp;

          // Log progress periodically
          if (batchCount % 10 === 0 || result.inserted > 0) {
            log.info(
              `Sync progress: batch ${batchCount}, checked ${totalChecked}, inserted ${totalInserted}, timestamp ${currentTimestamp}`,
            );
          }
        }

        // Exit loop if no more blocks to process
        if (!result.hasMore) {
          log.info(
            `Sync caught up - no more blocks after timestamp ${currentTimestamp}`,
          );
          break;
        }
      }

      const status = this.shouldContinue ? "completed" : "interrupted";
      this.completeSyncRun(
        runId,
        totalChecked,
        totalInserted,
        lastProcessedTimestamp,
        status,
      );

      log.info(
        `Sync ${status}: ${batchCount} batches, checked ${totalChecked}, inserted ${totalInserted}`,
      );

      return { checked: totalChecked, inserted: totalInserted };
    } catch (error) {
      log.error(
        `Sync failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      this.completeSyncRun(
        runId,
        totalChecked,
        totalInserted,
        lastProcessedTimestamp,
        "failed",
      );
      throw error;
    } finally {
      this.isSyncing = false;
    }
  }

  /**
   * Start the periodic sync process.
   * Syncs all blocks continuously, then waits for interval before next sync.
   */
  public start(): void {
    if (!config.syncer_enabled) {
      log.info("Syncer is disabled in configuration");
      return;
    }

    log.info(
      `Starting syncer with interval ${config.syncer_interval_ms}ms between sync cycles`,
    );
    this.shouldContinue = true;

    // Run the sync loop
    this.runSyncLoop();
  }

  /**
   * Run the sync loop - syncs all blocks, waits for interval, repeats.
   */
  private async runSyncLoop(): Promise<void> {
    while (this.shouldContinue) {
      try {
        // Perform a full sync (processes all available blocks)
        const result = await this.sync();

        if (!this.shouldContinue) break;

        // If we synced blocks, check again immediately in case new blocks arrived
        // Otherwise, wait for the configured interval
        if (result.checked > 0) {
          log.info("Sync cycle complete, checking for new blocks...");
          // Small delay to prevent tight loop if blocks are arriving continuously
          await new Promise((resolve) => setTimeout(resolve, 100));
        } else {
          log.info(
            `No blocks to sync, waiting ${config.syncer_interval_ms}ms before next check`,
          );
          await this.sleep(config.syncer_interval_ms);
        }
      } catch (error) {
        log.error(
          `Sync cycle failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        // Wait before retrying on error
        if (this.shouldContinue) {
          await this.sleep(config.syncer_interval_ms);
        }
      }
    }

    log.info("Sync loop stopped");
  }

  /**
   * Sleep for the specified duration, but can be interrupted by stop().
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const checkInterval = 100; // Check every 100ms if we should stop
      let elapsed = 0;

      const intervalId = setInterval(() => {
        elapsed += checkInterval;
        if (!this.shouldContinue || elapsed >= ms) {
          clearInterval(intervalId);
          resolve();
        }
      }, checkInterval);
    });
  }

  /**
   * Stop the sync process gracefully.
   * Will complete the current batch before stopping.
   */
  public stop(): void {
    log.info("Stopping syncer...");
    this.shouldContinue = false;

    // If a sync is in progress, it will stop after the current batch
    if (this.isSyncing) {
      log.info("Waiting for current sync batch to complete...");
    }
  }

  /**
   * Get sync statistics from the database.
   */
  public getStats(): {
    totalRuns: number;
    totalBlocksInserted: number;
    lastSyncedTimestamp: number;
    recentRuns: Array<{
      id: number;
      started_at: number;
      completed_at: number | null;
      blocks_checked: number;
      blocks_inserted: number;
      status: string;
    }>;
  } {
    const state = this.getSyncState();

    const totalRuns = this.sqlite.prepare(
      "SELECT COUNT(*) as count FROM sync_runs",
    ).get() as { count: number };

    const totalInserted = this.sqlite.prepare(
      "SELECT COALESCE(SUM(blocks_inserted), 0) as total FROM sync_runs WHERE status = 'completed'",
    ).get() as { total: number };

    const recentRuns = this.sqlite.prepare(`
      SELECT id, started_at, completed_at, blocks_checked, blocks_inserted, status
      FROM sync_runs
      ORDER BY id DESC
      LIMIT 10
    `).all() as Array<{
      id: number;
      started_at: number;
      completed_at: number | null;
      blocks_checked: number;
      blocks_inserted: number;
      status: string;
    }>;

    return {
      totalRuns: totalRuns.count,
      totalBlocksInserted: totalInserted.total,
      lastSyncedTimestamp: state.last_synced_timestamp,
      recentRuns,
    };
  }
}
