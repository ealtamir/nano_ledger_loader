import { NanoRPC } from "./nano-rpc.ts";
import { BlockInfo } from "./types.ts";
import { log } from "./logger.ts";
import { Database } from "jsr:@db/sqlite@0.12";
import { CrawlerMetrics } from "./metrics.ts";
import { config } from "./config_loader.ts";
import { getCurrentLedgerPosition, updateLedgerPosition } from "./db.ts";
export class NanoCrawler {
  private rpc: NanoRPC;
  private accountQueue: string[];
  private db: Database; // <-- better-sqlite3 Database instance
  private metrics: CrawlerMetrics; // Add this line

  private shouldContinue: boolean = true;

  constructor(rpcUrl: string, db: Database) {
    this.rpc = new NanoRPC(rpcUrl);
    this.accountQueue = [];
    this.db = db;
    this.metrics = new CrawlerMetrics(1000 * 30); // Print logs every 30 seconds
    this.shouldContinue = true;

    log.info("Processing blocks queue...");
    this.processBlocksQueue();

    this.parseLedger();
  }
  private async parseLedger(): Promise<void> {
    log.info("Starting ledger parsing...");
    if (!this.shouldContinue) return;

    try {
      const BATCH_SIZE = config.ledger_parse_batch_size || 1000;

      // Get the last processed account from the database, or use genesis if none exists
      let lastProcessedAccount = getCurrentLedgerPosition(
        config.burn_address,
      );
      log.info(`Starting ledger parsing from account: ${lastProcessedAccount}`);

      while (this.shouldContinue) {
        // Get batch of accounts from ledger
        const ledgerAccounts = await this.rpc.getLedger(
          lastProcessedAccount,
          BATCH_SIZE,
        );
        if (ledgerAccounts.error) {
          log.error(
            `Error getting ledger accounts: ${ledgerAccounts.error} with account ${lastProcessedAccount}`,
          );
          throw new Error(ledgerAccounts.error);
        }

        const accounts = Object.keys(ledgerAccounts.accounts);
        // Get frontiers from database for these accounts
        const dbFrontiers = this.getFrontiers(accounts);

        // Collect accounts that need updating (frontier mismatch or new account)
        const accountsToProcess: string[] = [];

        for (
          const [account, accountInfo] of Object.entries(
            ledgerAccounts.accounts,
          )
        ) {
          // If we don't have a frontier or our frontier is different from the chain
          const dbFrontier = dbFrontiers[account] || "";
          const chainFrontier = accountInfo.frontier;

          if (dbFrontier !== chainFrontier) {
            accountsToProcess.push(account);
          }
        }

        if (accounts.length <= 1) {
          log.info("Reached end of ledger, starting over");
          lastProcessedAccount = config.burn_address;
          // Update the ledger position in the database when starting over
          updateLedgerPosition(lastProcessedAccount);
          await new Promise((resolve) =>
            setTimeout(resolve, config.ledger_parse_interval || 60000)
          );
          continue;
        }

        const totalAccounts = accounts.length;
        if (accountsToProcess.length > 0) {
          log.debug(
            `Found ${accountsToProcess.length} accounts to process out of ${totalAccounts} accounts checked`,
          );
        }

        this.addToPendingAccounts(accountsToProcess);
        lastProcessedAccount = accounts[totalAccounts - 1];
        updateLedgerPosition(lastProcessedAccount);

        // Queue only accounts that need updating
        if (!this.shouldContinue) {
          return;
        }

        // Add small delay between batches to prevent overwhelming the node
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
    } catch (error) {
      log.error(
        `Error in ledger parsing: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      // Retry after delay
      if (this.shouldContinue) {
        setTimeout(
          () => this.parseLedger(),
          config.ledger_parse_interval || 60000,
        );
      }
    }
  }

  private removePendingAccount(account: string): void {
    const deleteStmt = this.db.prepare(
      "DELETE FROM pending_accounts WHERE account = ?",
    );
    deleteStmt.run(account);
  }

  private saveAccount(account: string, frontier: string): void {
    try {
      // Create a transaction for both operations
      log.debug(`Saving account ${account} with frontier ${frontier}`);
      if (frontier === "" || frontier === null) {
        log.error(`Frontier is empty for account ${account}`);
        Deno.exit(1);
      }
      const transaction = this.db.transaction((account: string) => {
        // Replace INSERT OR IGNORE with INSERT OR REPLACE to perform an upsert
        const insertStmt = this.db.prepare(
          "INSERT OR REPLACE INTO accounts (account, frontier) VALUES (?, ?)",
        );
        insertStmt.run(account, frontier);

        // Remove from pending_accounts
        const deleteStmt = this.db.prepare(
          "DELETE FROM pending_accounts WHERE account = ?",
        );
        deleteStmt.run(account);
      });

      // Execute the transaction
      transaction(account);
      this.metrics.addAccount();
    } catch (error) {
      log.error(
        `Failed to save account ${account}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error;
    }
  }

  private async saveBlocks(
    blocks: { [key: string]: BlockInfo },
  ): Promise<void> {
    if (Object.keys(blocks).length === 0) return;

    // Build values array for all blocks
    const values: any[] = [];

    const entries = Object.entries(blocks);
    for (const [hash, info] of entries as [string, BlockInfo][]) {
      if (info.contents.type !== "state") {
        values.push([
          hash,
          info.contents.type,
          info.block_account,
          info.contents.previous || null,
          info.contents.representative || null,
          info.balance || null,
          info.contents.link || null,
          info.contents.link_as_account || info.contents.destination || null,
          null,
          info.contents.signature,
          info.contents.work,
          info.contents.type || null,
          info.height,
          info.confirmed === "true" ? 1 : 0,
          info.successor || null,
          info.amount || null,
          info.local_timestamp || null,
        ]);
      } else {
        values.push([
          hash,
          info.contents.type,
          info.contents.account,
          info.contents.previous || null,
          info.contents.representative || null,
          info.contents.balance || null,
          info.contents.link || null,
          info.contents.link_as_account || null,
          info.contents.destination || null,
          info.contents.signature,
          info.contents.work,
          info.contents.subtype || info.subtype || null,
          info.height,
          info.confirmed ? 1 : 0,
          info.successor || null,
          info.amount || null,
          info.local_timestamp || null,
        ]);
      }
    }
    if (values.length === 0) return;

    // Prepare the SQL query template
    const query = `
      INSERT OR IGNORE INTO blocks (
        hash,
        type,
        account,
        previous,
        representative,
        balance,
        link,
        link_as_account,
        destination,
        signature,
        work,
        subtype,
        height,
        confirmed,
        successor,
        amount,
        local_timestamp
      ) VALUES (${values[0].map(() => "?").join(",")})
    `.trim();

    try {
      // Get batch size from config
      const batchSize = config.block_insert_batch_size || 50;

      // Process in batches to avoid SQLite parameter limit
      const stmt = this.db.prepare(query);

      // Use a transaction for better performance
      const insertBatch = this.db.transaction((batch: any[]) => {
        let totalInserted = 0;
        for (const row of batch) {
          stmt.run(...row);
          // Comment out this debug log
          // log.debug(`Changes from this insert: ${this.db.changes}`);
          totalInserted += this.db.changes;
        }
        return totalInserted;
      });

      let batchTotalInserted = 0;
      for (let i = 0; i < values.length; i += batchSize) {
        const batch = values.slice(i, i + batchSize);
        const inserted = await insertBatch(batch);
        batchTotalInserted += inserted;
      }

      // Keep this debug log since it's counting blocks inserted
      log.debug(
        `Successfully inserted ${batchTotalInserted} new blocks out of ${values.length} attempted in batches of up to ${batchSize}`,
      );
      this.metrics.addBlocks(batchTotalInserted);
    } catch (error) {
      log.error(
        `Failed to save blocks: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error; // Re-throw to be handled by caller
    }
  }

  private addToPendingAccounts(
    account: string | string[],
    nodeWebsocket: boolean = false,
  ): void {
    try {
      if (Array.isArray(account)) {
        const BATCH_SIZE = 500; // SQLite parameter limit
        for (let i = 0; i < account.length; i += BATCH_SIZE) {
          const batch = account.slice(i, i + BATCH_SIZE);
          const placeholders = batch.map(() => "(?)").join(",");

          // Create and execute transaction for this batch
          const insertBatch = this.db.transaction((accounts: string[]) => {
            const stmt = this.db.prepare(
              `INSERT OR IGNORE INTO pending_accounts (account) VALUES ${placeholders}`,
            );
            stmt.run(...accounts);
          });

          insertBatch(batch);
        }
      } else {
        const insertStmt = this.db.prepare(
          "INSERT OR IGNORE INTO pending_accounts (account) VALUES (?)",
        );
        insertStmt.run(account);
      }
    } catch (error) {
      log.error(
        `Failed to add account ${account} to pending: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error; // Re-throw to be handled by caller
    }
  }

  private loadPendingAccounts(batchSize: number = -1): string[] {
    if (batchSize === -1) {
      batchSize = config.pending_accounts_batch_size;
    }
    const stmt = this.db.prepare(
      "SELECT account FROM pending_accounts ORDER BY id LIMIT ?",
    );
    const rows = stmt.all(batchSize) as Array<{ account: string }>;
    return rows.map((row) => row.account);
  }

  public queueAccount(account: string, nodeWebsocket: boolean = false): void {
    // this.accountQueue.push(account);
    this.addToPendingAccounts(account, nodeWebsocket);
  }

  private getNewBlocks(allBlocks: string[]): string[] {
    if (allBlocks.length === 0) return [];

    if (!config.identify_new_blocks) {
      return allBlocks;
    }
    // Comment out this debug log
    // log.debug(`Getting new blocks data for ${allBlocks.length} blocks`);

    try {
      const CHUNK_SIZE = config.new_blocks_batch_size; // better-sqlite3 also has a limit on variables
      const existingBlocksSet = new Set<string>();

      for (let i = 0; i < allBlocks.length; i += CHUNK_SIZE) {
        const chunk = allBlocks.slice(i, i + CHUNK_SIZE);

        // We build the query with placeholders for each item in the chunk
        const query = `
          SELECT hash FROM blocks 
          WHERE hash IN (${chunk.map(() => "?").join(",")})
        `;
        const stmt = this.db.prepare(query);
        const existingBlocksChunk = stmt.all(...chunk) as Array<
          { hash: string }
        >;
        existingBlocksChunk.forEach((row) => existingBlocksSet.add(row.hash));
      }

      const newBlocks = allBlocks.filter((hash) =>
        !existingBlocksSet.has(hash)
      );
      // Comment out this debug log
      // log.debug(
      //   `Found ${newBlocks.length} new blocks from ${allBlocks.length} total blocks`,
      // );
      return newBlocks;
    } catch (error) {
      log.error(
        `Failed to query existing blocks: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error; // Re-throw to be handled by caller
    }
  }

  private async processBlocksQueue(): Promise<void> {
    try {
      // Get up to 50k blocks from the queue
      let rows: Array<{ hash: string }>;
      const stmt = this.db.prepare(
        `SELECT bq.hash FROM blocks_queue bq LIMIT ${config.block_queue_select_batch_size}`,
      );
      rows = stmt.all() as Array<{ hash: string }>;

      if (rows.length === 0) {
        // No blocks to process, schedule next run
        setTimeout(() => this.processBlocksQueue(), 10);
        return;
      }

      // Extract block hashes
      const blockHashes = rows.map((row) => row.hash);

      // Process blocks in batches to avoid overwhelming the node
      const BATCH_SIZE = config.blocks_info_batch_size || 1000;
      for (let i = 0; i < blockHashes.length; i += BATCH_SIZE) {
        if (!this.shouldContinue) {
          log.info(`Shutdown requested. Stopping block queue processing...`);
          return;
        }

        const batchHashes = blockHashes.slice(i, i + BATCH_SIZE);

        // Get block info for this batch
        for await (
          const blocksInfoResponse of this.rpc.getBlocksInfo(batchHashes)
        ) {
          // Save blocks to database
          await this.saveBlocks(blocksInfoResponse.blocks);

          // Update metrics

          for (const info of Object.values(blocksInfoResponse.blocks)) {
            if (!this.shouldContinue) {
              log.info(
                `Shutdown requested. Finishing current account and exiting...`,
              );
              break;
            }
            const newAddress = info.contents.link_as_account ||
              info.contents.destination;
            if (newAddress) {
              this.queueAccount(newAddress);
            }
          }

          // Remove processed blocks from queue in batches to respect SQLite limits
          const deleteStmt = this.db.prepare(
            "DELETE FROM blocks_queue WHERE hash = ?",
          );

          // Get the block hashes to delete
          const hashesToDelete = Object.keys(blocksInfoResponse.blocks);

          // Use a batch size of 999 to stay under SQLite's parameter limit
          const DELETE_BATCH_SIZE = 999;

          // Process deletions in batches
          for (let i = 0; i < hashesToDelete.length; i += DELETE_BATCH_SIZE) {
            const batchHashes = hashesToDelete.slice(
              i,
              i + DELETE_BATCH_SIZE,
            );

            // Use a transaction for each batch for better performance
            const deleteBatch = this.db.transaction((hashes: string[]) => {
              for (const hash of hashes) {
                deleteStmt.run(hash);
              }
            });

            // Execute the transaction for this batch
            await deleteBatch(batchHashes);

            log.debug(
              `Deleted batch of ${batchHashes.length} blocks from queue (${
                i + 1
              }-${
                Math.min(i + DELETE_BATCH_SIZE, hashesToDelete.length)
              } of ${hashesToDelete.length})`,
            );
          }
        }
      }

      // Schedule next run with a small delay
      if (this.shouldContinue) {
        setTimeout(() => this.processBlocksQueue(), 10);
      }
    } catch (error) {
      log.error(
        `Error processing blocks queue: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      // Even on error, continue processing after a delay
      if (this.shouldContinue) {
        setTimeout(() => this.processBlocksQueue(), 1000); // Longer delay on error
      }
    }
  }

  private async processAccount(
    account: string,
    frontier: string,
  ): Promise<void> {
    try {
      let totalBlocks = 0;
      let latestBlockHash = "";
      if (!frontier && account) {
        try {
          const accountInfo = await this.rpc.getAccountInfo(account);
          if (!accountInfo || accountInfo.error) {
            this.removePendingAccount(account);
            this.metrics.addAccount();
            return;
          }
          frontier = accountInfo.open_block;
        } catch (error) {
          log.error(`Failed to fetch account info: ${error}`);
          throw new Error(`Failed to fetch account info: ${error}`);
        }
      }
      log.debug(`Processing account ${account} with frontier ${frontier}`);

      // Process blocks as they come in from the chain
      for await (
        const blockBatch of this.rpc.getSuccessorsGenerator(
          frontier,
        )
      ) {
        if (blockBatch.length === 0) {
          latestBlockHash = frontier;
          break;
        }
        latestBlockHash = blockBatch[blockBatch.length - 1];
        totalBlocks += blockBatch.length;
        this.metrics.addBlocksQueried(blockBatch.length);
        if (!this.shouldContinue) {
          log.info(
            `Shutdown requested. Finishing current account and exiting...`,
          );
          return;
        }

        // Add blocks to queue for processing
        this.addBlocksToQueue(blockBatch);
      }

      this.saveAccount(account, latestBlockHash);
    } catch (error: unknown) {
      log.error(
        `Failed to process account ${account}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw new Error(
        `Failed to process account ${account}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  private getFrontiers(accounts: string[]): Record<string, string> {
    const BATCH_SIZE = 999; // SQLite has a limit of 1000 variables per query
    const frontiers: Record<string, string> = {};

    // Process accounts in batches
    for (let i = 0; i < accounts.length; i += BATCH_SIZE) {
      const batch = accounts.slice(i, i + BATCH_SIZE);

      // Create placeholders for the IN clause (?, ?, ?, etc)
      const placeholders = batch.map(() => "?").join(",");

      const stmt = this.db.prepare(
        `SELECT account, frontier FROM accounts WHERE account IN (${placeholders})`,
      );

      const rows = stmt.all(...batch).reduce((acc, row) => {
        if (row.frontier !== "" && row.frontier !== null) {
          acc[row.account] = row.frontier;
        }
        return acc;
      }, {} as Record<string, string>);

      // Add results to frontiers object
      for (const account of batch) {
        if (account in rows) {
          frontiers[account] = rows[account];
        } else {
          frontiers[account] = "";
        }
      }
    }

    return frontiers;
  }

  private async processBatch(accounts: string[]): Promise<void> {
    try {
      const accountFrontiers = this.getFrontiers(accounts);

      log.debug(
        `Processing frontiers of ${
          Object.keys(accountFrontiers).length
        } accounts`,
      );

      // Process each account sequentially
      for (const [account, frontier] of Object.entries(accountFrontiers)) {
        if (!this.shouldContinue) {
          log.info(
            `Shutdown requested. Finishing current account and exiting...`,
          );
          break;
        }

        try {
          // Comment out this debug log
          // log.debug(`Processing ${account}`);
          const startTime = Date.now();
          await this.processAccount(account, frontier);
          const endTime = Date.now();
          // log.debug(`Processed account ${account} in ${endTime - startTime}ms`);
          this.metrics.addAccountProcessingTime(endTime - startTime);
        } catch (error) {
          log.error(`Error processing account ${account}: ${error}`);
          this.queueAccount(account);
        }
      }
    } catch (error) {
      log.error(`Batch processing failed: ${error}`);
      for (const account of accounts) {
        this.queueAccount(account);
      }
    }
  }

  public async crawl(genesisAccount: string): Promise<void> {
    let lastSignalTime = 0;

    const signalHandler = () => {
      const now = Date.now();
      if (now - lastSignalTime < 1000) {
        log.info("\nForce quitting...");
        Deno.exit(1);
      }

      lastSignalTime = now;
      log.info(
        "\nReceived shutdown signal. Finishing current account and exiting...",
      );
      log.info("Press Ctrl+C again to force quit immediately.");
      this.shouldContinue = false;
    };

    // Handle both SIGINT (Ctrl+C) and SIGTERM
    Deno.addSignalListener("SIGINT", signalHandler);
    Deno.addSignalListener("SIGTERM", signalHandler);

    try {
      // this.accountQueue.push(genesisAccount);

      while (this.shouldContinue) {
        while (this.shouldContinue && this.accountQueue.length > 0) {
          // Process up to 100 accounts at a time
          const batch = this.accountQueue.splice(
            0,
            config.account_processing_batch_size,
          );
          await this.processBatch(batch);
        }
        if (!this.shouldContinue) {
          break;
        }

        // When queue is empty, check pending_accounts
        const pendingAccounts = this.loadPendingAccounts();
        log.debug(
          `Processing batch of ${pendingAccounts.length} pending accounts`,
        );
        if (pendingAccounts.length === 0) {
          log.info(
            "No more pending accounts to process. Waiting for new blocks...",
          );
          await new Promise((resolve) => setTimeout(resolve, 5000));
          continue;
        }

        // Add pending accounts back to queue
        for (const account of pendingAccounts) {
          this.accountQueue.push(account);
          this.metrics.addPendingAccountExtraction();
        }
      }
    } finally {
      // Clean up signal handlers
      Deno.removeSignalListener("SIGINT", signalHandler);
      Deno.removeSignalListener("SIGTERM", signalHandler);
      this.metrics.stop();
    }

    log.info("Crawler stopped.");
  }

  private addBlocksToQueue(blockHashes: string[]): void {
    if (blockHashes.length === 0) return;

    try {
      const insertStmt = this.db.prepare(`
        INSERT OR IGNORE INTO blocks_queue (hash)
        VALUES (?)
      `);

      const insertMany = this.db.transaction((hashes: string[]) => {
        let insertedCount = 0;
        for (const hash of hashes) {
          // First check if block exists in blocks table
          const existsStmt = this.db.prepare(
            "SELECT 1 FROM blocks WHERE hash = ? UNION SELECT 1 FROM blocks_queue WHERE hash = ?",
          );
          const exists = existsStmt.values(hash, hash);

          // Only insert into queue if not already in blocks table
          if (exists.length === 0) {
            insertStmt.run(hash);
            insertedCount += this.db.changes;
          }
        }
        return insertedCount;
      });

      // Execute the transaction with all block hashes
      const inserted = insertMany(blockHashes);
      this.metrics.addQueueInserted(inserted);
    } catch (error) {
      log.error(
        `Failed to add blocks to queue: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error;
    }
  }
}
