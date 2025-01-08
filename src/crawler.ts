import { NanoRPC } from './nano-rpc.ts';
import { BlockInfo } from './types.ts';
import { DB } from "sqlite";
import { log } from "./logger.ts";

export class NanoCrawler {
  private rpc: NanoRPC;
  private accountQueue: string[];
  private db: DB;

  constructor(rpcUrl: string, db: DB) {
    this.rpc = new NanoRPC(rpcUrl);
    this.accountQueue = [];
    this.db = db;
  }

  private async isAccountProcessed(account: string): Promise<boolean> {
    const result = this.db.query<[string]>(
      "SELECT account FROM accounts WHERE account = ?",
      [account]
    );
    return result.length > 0;
  }

  private async saveAccount(account: string): Promise<void> {
    this.db.query("INSERT OR IGNORE INTO accounts (account) VALUES (?)", [account]);
  }

  private async saveBlocks(blocks: {[key: string]: BlockInfo}): Promise<void> {
    if (Object.keys(blocks).length === 0) return;

    // Build bulk insert query
    const placeholders: string[] = [];
    const values: any[] = [];
    
    for (const [hash, info] of Object.entries(blocks)) {
        // if (info.contents.confirmed !== undefined && !info.contents.confirmed) {
        //     continue;
        // }
        placeholders.push("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        values.push(
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
            info.subtype || null,
            info.height,
            info.confirmed ? 1 : 0,
            info.successor || null,
            info.amount || null,
            info.local_timestamp || null
        );
    }

    // If no valid blocks to insert, return early
    if (placeholders.length === 0) return;

    const query = `
        INSERT INTO blocks (
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
        ) VALUES ${placeholders.join(", ")}
    `.trim();  // Remove any trailing whitespace

    try {
      await this.db.query(query, values);
    } catch (error) {
      log.error(`Failed to save blocks: ${error instanceof Error ? error.message : String(error)}`);
      throw error; // Re-throw to be handled by caller
    }
  }

  private async addToPendingAccounts(account: string): Promise<void> {
    this.db.query(
      "INSERT OR IGNORE INTO pending_accounts (account) VALUES (?)",
      [account]
    );
  }

  private async removeFromPendingAccounts(account: string): Promise<void> {
    this.db.query(
      "DELETE FROM pending_accounts WHERE account = ?",
      [account]
    );
  }

  private async loadPendingAccounts(): Promise<string[]> {
    const results = this.db.query<[string]>(
      "SELECT account FROM pending_accounts ORDER BY id"
    );
    return results.map(row => row[0]);
  }

  public async queueAccount(account: string): Promise<void> {
    this.accountQueue.push(account);
    await this.addToPendingAccounts(account);
  }

  private async getNewBlocks(allBlocks: string[]): Promise<string[]> {
    if (allBlocks.length === 0) return [];
    
    try {
        const CHUNK_SIZE = 500; // SQLite typically has a limit of 1000 variables
        const existingBlocksSet = new Set<string>();

        // Process blocks in chunks
        for (let i = 0; i < allBlocks.length; i += CHUNK_SIZE) {
            const chunk = allBlocks.slice(i, i + CHUNK_SIZE);
            const existingBlocksChunk = await this.db.query<[string]>(
                "SELECT hash FROM blocks WHERE hash IN (" + chunk.map(() => "?").join(",") + ")",
                chunk
            );
            existingBlocksChunk.forEach(row => existingBlocksSet.add(row[0]));
        }

        const newBlocks = allBlocks.filter(hash => !existingBlocksSet.has(hash));
        log.info(`Found ${newBlocks.length} new blocks from ${allBlocks.length} total blocks`);
        return newBlocks;
    } catch (error) {
        log.error(`Failed to query existing blocks: ${error instanceof Error ? error.message : String(error)}`);
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
        log.warn(`No ledger data found for account ${account}`);
        await this.removeFromPendingAccounts(account);
        return;
      }

      const accountInfo = ledgerResponse.accounts[account];
      const chainResponse = await this.rpc.getChain(accountInfo.frontier);
      log.info(`Processing account ${account} - Found ${chainResponse.blocks?.length || 0} blocks`);

      if (chainResponse.blocks && chainResponse.blocks.length > 0) {
        // Use the new method to filter blocks
        const newBlocks = await this.getNewBlocks(chainResponse.blocks);
        // const newBlocks = chainResponse.blocks;
        
        if (newBlocks.length === 0) {
            log.info(`No new blocks after filtering found for account ${account}`);
        } else {
            let processedChunks = 0;
            for await (const blocksInfoResponse of this.rpc.getBlocksInfo(newBlocks)) {
                // Save all blocks in one transaction
                this.db.transaction(async () => {
                    await this.saveBlocks(blocksInfoResponse.blocks);
                    
                    // Queue new accounts found in blocks
                    for (const info of Object.values(blocksInfoResponse.blocks)) {
                        const new_address = info.contents.link_as_account || info.contents.destination;
                        if (new_address) {
                            await this.queueAccount(new_address);
                        }
                    }
                });

      
                // Log progress every 5 chunks
                processedChunks++;
                if (processedChunks % 10 === 0) {
                    const keysQuantity = Object.keys(blocksInfoResponse.blocks).length;
                    const processedBlocks = Math.min(processedChunks * keysQuantity, newBlocks.length);
                    log.info(`Processed ${processedBlocks} out of ${newBlocks.length} blocks`);
                }
            }
        }
      }

      // Mark account as processed and remove from pending
      this.db.transaction(async () => {
        await this.saveAccount(account);
        await this.removeFromPendingAccounts(account);
      });
      
    } catch (error: unknown) {
      throw new Error(`Failed to process account ${account}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  async crawl(genesisAccount: string): Promise<void> {

    // Flag to track if we should continue processing
    let shouldContinue = true;

    let forceQuit = false;
    let lastSignalTime = 0;
    
    const signalHandler = () => {
      const now = Date.now();
      if (now - lastSignalTime < 1000) { // If second press within 1 second
        log.info('\nForce quitting...');
        Deno.exit(1);
      }
      
      lastSignalTime = now;
      log.info('\nReceived shutdown signal. Finishing current account and exiting...');
      log.info('Press Ctrl+C again to force quit immediately.');
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
            await new Promise(resolve => setTimeout(resolve, 1000));
          }
        }

        if (!shouldContinue) break;

        // When queue is empty, check pending_accounts
        const pendingAccounts = await this.loadPendingAccounts();
        if (pendingAccounts.length === 0) {
          break; // No more accounts to process
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

    log.info('Crawler stopped.');
  }
} 