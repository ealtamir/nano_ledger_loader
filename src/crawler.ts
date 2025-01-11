import { NanoRPC } from "./nano-rpc.ts";
import { BlockInfo } from "./types.ts";
import { log } from "./logger.ts";
import { Database } from "jsr:@db/sqlite@0.12";
import { CrawlerMetrics } from "./metrics.ts";

export class NanoCrawler {
  private rpc: NanoRPC;
  private accountQueue: string[];
  private db: Database;  // <-- better-sqlite3 Database instance
  private metrics: CrawlerMetrics;  // Add this line

  constructor(rpcUrl: string, db: Database) {
    this.rpc = new NanoRPC(rpcUrl);
    this.accountQueue = [];
    this.db = db;
    this.metrics = new CrawlerMetrics(1000 * 30);  // Print logs every 30 seconds
  }

  private async isAccountProcessed(account: string): Promise<boolean> {
    // better-sqlite3: use .get(...) to fetch a single row
    const stmt = this.db.prepare("SELECT account FROM accounts WHERE account = ?");
    const row = stmt.get(account);
    return row !== undefined;
  }

  private async saveAccount(account: string): Promise<void> {
    // Use .run(...) for INSERT/UPDATE/DELETE statements
    const stmt = this.db.prepare("INSERT OR IGNORE INTO accounts (account) VALUES (?)");
    stmt.run(account);
  }

  private async saveBlocks(blocks: { [key: string]: BlockInfo }): Promise<void> {
    if (Object.keys(blocks).length === 0) return;

    // Build bulk insert query
    const values: any[] = [];

    const entries = Object.entries(blocks)
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
        info.confirmed === 'true' ? 1 : 0,
        info.successor || null,
        info.amount || null,
        info.local_timestamp || null
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
        info.local_timestamp || null
      ]);
        }
    }
    if (values.length === 0) return;

    // If no valid blocks to insert, return early
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
      // better-sqlite3 can insert all rows in one go as long as you match placeholders with values
      const info = this.db.prepare(query);

      const insertMany = this.db.transaction((rows: any[]) => {
        for (const row of rows) {
          info.run(row);
        }
      });
      await insertMany(values);
      info.finalize();
    } catch (error) {
      log.error(
        `Failed to save blocks: ${error instanceof Error ? error.message : String(error)}`
      );
      throw error; // Re-throw to be handled by caller
    }
  }

  private async addToPendingAccounts(account: string): Promise<void> {
    // Remove from processed accounts first
    const removeStmt = this.db.prepare("DELETE FROM accounts WHERE account = ?");
    removeStmt.run(account);

    // Then add to pending accounts
    const insertStmt = this.db.prepare(
      "INSERT OR IGNORE INTO pending_accounts (account) VALUES (?)"
    );
    insertStmt.run(account);
  }

  private async removeFromPendingAccounts(account: string): Promise<void> {
    const stmt = this.db.prepare("DELETE FROM pending_accounts WHERE account = ?");
    stmt.run(account);
  }

  private async loadPendingAccounts(): Promise<string[]> {
    const stmt = this.db.prepare(
      "SELECT account FROM pending_accounts ORDER BY id"
    );
    const rows = stmt.all() as Array<{ account: string }>;
    return rows.map((row) => row.account);
  }

  public async queueAccount(account: string): Promise<void> {
    this.accountQueue.push(account);
    await this.addToPendingAccounts(account);
  }

  private async getNewBlocks(allBlocks: string[]): Promise<string[]> {
    if (allBlocks.length === 0) return [];

    try {
      const CHUNK_SIZE = 500; // better-sqlite3 also has a limit on variables
      const existingBlocksSet = new Set<string>();

      for (let i = 0; i < allBlocks.length; i += CHUNK_SIZE) {
        const chunk = allBlocks.slice(i, i + CHUNK_SIZE);

        // We build the query with placeholders for each item in the chunk
        const query = `
          SELECT hash FROM blocks 
          WHERE hash IN (${chunk.map(() => "?").join(",")})
        `;
        const stmt = this.db.prepare(query);
        const existingBlocksChunk = await stmt.all(chunk) as Array<{ hash: string }>;

        existingBlocksChunk.forEach((row) => existingBlocksSet.add(row.hash));
      }

      const newBlocks = allBlocks.filter((hash) => !existingBlocksSet.has(hash));
      log.debug(
        `Found ${newBlocks.length} new blocks from ${allBlocks.length} total blocks`
      );
      return newBlocks;
    } catch (error) {
      log.error(
        `Failed to query existing blocks: ${error instanceof Error ? error.message : String(error)}`
      );
      throw error; // Re-throw to be handled by caller
    }
  }

  private async processAccount(account: string): Promise<void> {
    if (await this.isAccountProcessed(account)) {
      return;
    }

    try {
      // Get account info and all its blocks
      const ledgerResponse = await this.rpc.getLedger(account);

      if (!ledgerResponse.accounts || !ledgerResponse.accounts[account]) {
        log.debug(`No ledger data found for account ${account}`);
        await this.removeFromPendingAccounts(account);
        this.metrics.addAccount();
        return;
      }

      const accountInfo = ledgerResponse.accounts[account];
      let totalBlocks = 0;
      let processedChunks = 0;

      // Process blocks as they come in from the chain
      for await (const blockBatch of this.rpc.getChainGenerator(accountInfo.frontier)) {
        const newBlocks = await this.getNewBlocks(blockBatch);
        totalBlocks += newBlocks.length;

        if (newBlocks.length === 0) {
          break;
        }
        
        // Process each batch of blocks
        for await (const blocksInfoResponse of this.rpc.getBlocksInfo(newBlocks)) {
          await this.saveBlocks(blocksInfoResponse.blocks);
          this.metrics.addBlocks(Object.keys(blocksInfoResponse.blocks).length);

          // Queue new accounts found in blocks
          for (const info of Object.values(blocksInfoResponse.blocks)) {
            const newAddress = info.contents.link_as_account || info.contents.destination;
            if (newAddress) {
              await this.queueAccount(newAddress);
            }
          }

          // Log progress every 25 chunks
          processedChunks++;
          if (processedChunks % 25 === 0) {  // 10k blocks
            const keysQuantity = Object.keys(blocksInfoResponse.blocks).length;
            const processedBlocks = Math.min(
              processedChunks * keysQuantity,
              totalBlocks
            );
            log.debug(
              `Processed ${processedBlocks} out of ${totalBlocks} blocks`
            );
          }
        }
      }

      if (totalBlocks === 0) {
        log.debug(`No blocks found for account ${account}`);
      }

      // Mark account as processed and remove from pending
      await this.saveAccount(account);
      await this.removeFromPendingAccounts(account);
      this.metrics.addAccount();
    } catch (error: unknown) {
      throw new Error(
        `Failed to process account ${account}: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  }

  public async crawl(genesisAccount: string): Promise<void> {
    let shouldContinue = true;
    let lastSignalTime = 0;

    const signalHandler = () => {
      const now = Date.now();
      if (now - lastSignalTime < 1000) {
        log.info("\nForce quitting...");
        Deno.exit(1);
      }

      lastSignalTime = now;
      log.info("\nReceived shutdown signal. Finishing current account and exiting...");
      log.info("Press Ctrl+C again to force quit immediately.");
      shouldContinue = false;
    };

    // Handle both SIGINT (Ctrl+C) and SIGTERM
    Deno.addSignalListener("SIGINT", signalHandler);
    Deno.addSignalListener("SIGTERM", signalHandler);

    try {
      // Start with genesis account
      await this.queueAccount(genesisAccount);

      while (shouldContinue) {
        // Process accounts in queue
        while (shouldContinue && this.accountQueue.length > 0) {
          const account = this.accountQueue.shift()!;
          try {
            await this.processAccount(account);
          } catch (error) {
            console.error(`Error processing account ${account}:`, error);
            // Add back to queue to retry later
            await this.queueAccount(account);
            // Wait before retrying
            await new Promise((resolve) => setTimeout(resolve, 1000));
          }
        }

        if (!shouldContinue) break;

        // When queue is empty, check pending_accounts
        const pendingAccounts = await this.loadPendingAccounts();
        if (pendingAccounts.length === 0) {
          log.info("No more pending accounts to process. Waiting for new blocks...");
          await new Promise((resolve) => setTimeout(resolve, 5000));
        } else {
          log.info(`Processing ${pendingAccounts.length} pending accounts`);
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
    }

    log.info("Crawler stopped.");
  }
}
