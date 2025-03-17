import { NanoRPC } from "./nano-rpc.ts";
import { AccountInfoResponse, BlockInfo } from "./types.ts";
import { log } from "./logger.ts";
import { Database } from "jsr:@db/sqlite@0.12";
import { CrawlerMetrics } from "./metrics.ts";
import { config } from "./config_loader.ts";
import { Logger } from "jsr:@std/log/get-logger";
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
    this.processBlocksQueue();
  }

  private async isAccountProcessed(
    account: string,
    frontier: string,
  ): Promise<string> {
    // better-sqlite3: use .get(...) to fetch a single row
    const stmt = this.db.prepare(
      "SELECT * FROM accounts WHERE account = ?",
    );
    const row: { account: string; frontier: string } | undefined = stmt.get(
      account,
    );
    if (row) {
      return row.frontier === frontier ? "" : row.frontier;
    }
    return "";
  }

  private async saveAccount(account: string, frontier: string): Promise<void> {
    try {
      // Create a transaction for both operations
      const transaction = this.db.transaction((account: string) => {
        // Insert into accounts
        const insertStmt = this.db.prepare(
          "INSERT OR IGNORE INTO accounts (account, frontier) VALUES (?, ?)",
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
        for (const row of batch) {
          stmt.run(...row);
        }
      });

      // Process all values in batches
      for (let i = 0; i < values.length; i += batchSize) {
        const batch = values.slice(i, i + batchSize);
        log.debug(
          `Inserting batch of ${batch.length} blocks (${i + 1}-${
            Math.min(i + batchSize, values.length)
          } of ${values.length})`,
        );
        insertBatch(batch);
      }
      stmt.finalize();

      log.debug(
        `Successfully inserted ${values.length} blocks in batches of up to ${batchSize}`,
      );
    } catch (error) {
      log.error(
        `Failed to save blocks: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error; // Re-throw to be handled by caller
    }
  }

  private async addToPendingAccounts(
    account: string,
    nodeWebsocket: boolean = false,
  ): Promise<void> {
    try {
      const insertStmt = this.db.prepare(
        "INSERT OR IGNORE INTO pending_accounts (account) VALUES (?)",
      );
      insertStmt.run(account);
    } catch (error) {
      log.error(
        `Failed to add account ${account} to pending: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error; // Re-throw to be handled by caller
    }
  }

  private async removeFromPendingAccounts(account: string): Promise<void> {
    const stmt = this.db.prepare(
      "DELETE FROM pending_accounts WHERE account = ?",
    );
    stmt.run(account);
  }

  private async removeAccounts(accounts: string[]): Promise<void> {
    // Skip if no accounts to remove
    if (accounts.length === 0) return;

    try {
      // Create and execute transaction
      const transaction = this.db.transaction((accounts: string[]) => {
        const stmt = this.db.prepare("DELETE FROM accounts WHERE account = ?");
        for (const account of accounts) {
          stmt.run(account);
        }
      });

      transaction(accounts);
    } catch (error) {
      log.error(
        `Failed to remove accounts: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error;
    }
  }

  private async loadPendingAccounts(batchSize: number = -1): Promise<string[]> {
    if (batchSize === -1) {
      batchSize = config.pending_accounts_batch_size;
    }
    const stmt = this.db.prepare(
      "SELECT account FROM pending_accounts ORDER BY id LIMIT ?",
    );
    const rows = stmt.all(batchSize) as Array<{ account: string }>;
    return rows.map((row) => row.account);
  }

  public async queueAccount(
    account: string,
    nodeWebsocket: boolean = false,
  ): Promise<void> {
    // this.accountQueue.push(account);
    await this.addToPendingAccounts(account, nodeWebsocket);
  }

  private async getNewBlocks(allBlocks: string[]): Promise<string[]> {
    if (allBlocks.length === 0) return [];

    if (!config.identify_new_blocks) {
      return allBlocks;
    }
    log.debug(`Getting new blocks data for ${allBlocks.length} blocks`);

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
        const existingBlocksChunk = await stmt.all(chunk) as Array<
          { hash: string }
        >;

        existingBlocksChunk.forEach((row) => existingBlocksSet.add(row.hash));
      }

      const newBlocks = allBlocks.filter((hash) =>
        !existingBlocksSet.has(hash)
      );
      log.debug(
        `Found ${newBlocks.length} new blocks from ${allBlocks.length} total blocks`,
      );
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
      const stmt = this.db.prepare("SELECT hash FROM blocks_queue LIMIT 50000");
      const rows = stmt.all() as Array<{ hash: string }>;

      if (rows.length === 0) {
        // No blocks to process, schedule next run
        setTimeout(() => this.processBlocksQueue(), 10);
        return;
      }

      log.debug(`Processing ${rows.length} blocks from queue`);

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
          this.metrics.addBlocks(Object.keys(blocksInfoResponse.blocks).length);

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
              await this.queueAccount(newAddress);
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
            const batchHashes = hashesToDelete.slice(i, i + DELETE_BATCH_SIZE);

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
          deleteStmt.finalize();
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

      // Process blocks as they come in from the chain
      for await (
        const blockBatch of this.rpc.getSuccessorsGenerator(
          frontier,
        )
      ) {
        latestBlockHash = blockBatch[blockBatch.length - 1];
        totalBlocks += blockBatch.length;
        if (!this.shouldContinue) {
          log.info(
            `Shutdown requested. Finishing current account and exiting...`,
          );
          return;
        }

        // Add blocks to queue for processing
        await this.addBlocksToQueue(blockBatch);
      }

      if (totalBlocks === 0) {
        log.debug(`No blocks found for account ${account}`);
      }

      // Mark account as processed and remove from pending
      if (this.shouldContinue) {
        await this.saveAccount(account, latestBlockHash);
        this.metrics.addAccount();
      }
    } catch (error: unknown) {
      throw new Error(
        `Failed to process account ${account}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  private getFrontiers(
    accounts: string[],
  ): Record<string, string> {
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

      const rows = stmt.all(batch).reduce((acc, row) => {
        acc[row.account] = row.frontier;
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

  /**
   * Fetches account information and determines the appropriate frontier hash for each account
   * that needs processing.
   *
   * @param accounts Array of account addresses to process
   * @returns A map of account addresses to their frontier hashes for processing
   */
  private async getAccountsToProcess(
    accounts: string[],
  ): Promise<Record<string, string>> {
    // Step 1: Fetch account info for all accounts in parallel
    const ledgerPromises = accounts.map((account) =>
      this.rpc.getAccountInfo(account)
    );

    // Wait for all promises to settle (some may fail)
    const promises = await Promise.allSettled(ledgerPromises);

    // Step 2: Process the results, correlating each result with its account
    const accountLedgerPairs = accounts.map((account, index) => ({
      account,
      ledgerPromise: promises[index],
    }));

    // Filter out failed requests and extract account info
    const fetchedAccounts = accountLedgerPairs
      .map((pair) => {
        if (pair.ledgerPromise.status !== "fulfilled") {
          log.debug(`Failed to fetch account info for ${pair.account}`);
          return undefined;
        }
        const promiseResult = pair.ledgerPromise.value;
        promiseResult.account = pair.account; // Ensure account field is set
        return promiseResult;
      })
      .filter((account): account is AccountInfoResponse =>
        account !== undefined
      );

    // Step 3: Get current frontiers from database for these accounts
    const accountFrontiers = this.getFrontiers(
      fetchedAccounts.map((account) => account.account),
    );

    // Step 4: For accounts with no frontier in DB, use their open_block as starting point
    for (const accountData of fetchedAccounts) {
      const account = accountData.account;
      if (account in accountFrontiers && accountFrontiers[account] === "") {
        accountFrontiers[account] = accountData.open_block;
      }
    }

    // Step 5: Filter accounts that need processing (frontier != confirmed_frontier)
    const accountsToProcess = fetchedAccounts
      .filter((account) =>
        account.account in accountFrontiers &&
        accountFrontiers[account.account] !== account.confirmed_frontier
      )
      .map((account) => account.account);

    // Step 6: Create final map of accounts to their frontiers
    const finalFrontiers = accountsToProcess.reduce((acc, account) => {
      acc[account] = accountFrontiers[account];
      return acc;
    }, {} as Record<string, string>);

    log.debug(
      `Found ${
        Object.keys(finalFrontiers).length
      } accounts to process out of ${accounts.length} total`,
    );

    return finalFrontiers;
  }

  private async processBatch(accounts: string[]): Promise<void> {
    try {
      // Get accounts that need processing with their frontiers
      const accountFrontiers = await this.getAccountsToProcess(accounts);

      // Remove accounts that don't need processing from pending
      const accountsToRemove = accounts.filter((account) =>
        !(account in accountFrontiers)
      );
      for (const account of accountsToRemove) {
        await this.removeFromPendingAccounts(account);
        this.metrics.addAccount();
      }

      // Process each account sequentially
      for (const [account, frontier] of Object.entries(accountFrontiers)) {
        if (!this.shouldContinue) {
          log.info(
            `Shutdown requested. Finishing current account and exiting...`,
          );
          break;
        }

        try {
          log.debug(`Processing ${account}`);
          await this.processAccount(account, frontier);
        } catch (error) {
          log.error(`Error processing account ${account}: ${error}`);
          await this.queueAccount(account);
        }
      }
    } catch (error) {
      log.error(`Batch processing failed: ${error}`);
      for (const account of accounts) {
        await this.queueAccount(account);
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
      this.accountQueue.push(genesisAccount);

      while (this.shouldContinue) {
        while (this.shouldContinue && this.accountQueue.length > 0) {
          // Process up to 100 accounts at a time
          const batch = this.accountQueue.splice(0, 100);
          await this.processBatch(batch);
        }
        if (!this.shouldContinue) {
          break;
        }

        // When queue is empty, check pending_accounts
        const pendingAccounts = await this.loadPendingAccounts();
        if (pendingAccounts.length === 0) {
          log.info(
            "No more pending accounts to process. Waiting for new blocks...",
          );
          await new Promise((resolve) => setTimeout(resolve, 5000));
        } else {
          log.debug(
            `Processing batch of ${pendingAccounts.length} pending accounts`,
          );

          // This is done as a precaution for cases where accounts were added to pending
          // and not remove from the list of processed accounts.
          this.removeAccounts(pendingAccounts);
        }

        // Add pending accounts back to queue
        for (const account of pendingAccounts) {
          this.accountQueue.push(account);
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

  private async addBlocksToQueue(blockHashes: string[]): Promise<void> {
    if (blockHashes.length === 0) return;

    try {
      log.debug(`Adding ${blockHashes.length} blocks to queue`);

      // Use a transaction for better performance with multiple inserts
      const insertStmt = this.db.prepare(
        "INSERT OR IGNORE INTO blocks_queue (hash) VALUES (?)",
      );

      const insertMany = this.db.transaction((hashes: string[]) => {
        for (const hash of hashes) {
          insertStmt.run(hash);
        }
      });

      // Execute the transaction with all block hashes
      insertMany(blockHashes);

      log.debug(`Successfully added ${blockHashes.length} blocks to queue`);
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
