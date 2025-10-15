import { NanoRPC } from "./nano-rpc.ts";
import { BlockInfo } from "./types.ts";
import { log } from "./logger.ts";
import { Client } from "postgres";
import { CrawlerMetrics } from "./metrics.ts";
import { config } from "./config_loader.ts";
import {
  addBlocksToQueue,
  addToPendingAccounts,
  getCurrentLedgerPosition,
  getFrontiers,
  getHashesFromBlocksQueue,
  getNewBlocks,
  loadPendingAccounts,
  removeHashesFromBlocksQueue,
  removePendingAccount,
  saveAccount,
  saveBlocks,
  updateLedgerPosition,
} from "./pg_db.ts";

export class NanoCrawler {
  private rpc: NanoRPC;
  private accountQueue: string[];
  private client: Client; // <-- postgres client instance
  private metrics: CrawlerMetrics; // Add this line

  private shouldContinue: boolean = true;

  constructor(rpcUrl: string, client: Client) {
    this.rpc = new NanoRPC(rpcUrl);
    this.accountQueue = [];
    this.client = client;
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
      let lastProcessedAccount = await getCurrentLedgerPosition(
        this.client,
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
        const dbFrontiers = await getFrontiers(this.client, accounts);

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
          await updateLedgerPosition(this.client, lastProcessedAccount);
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

        await this.addToPendingAccounts(accountsToProcess);
        lastProcessedAccount = accounts[totalAccounts - 1];
        await updateLedgerPosition(this.client, lastProcessedAccount);

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

  private async removePendingAccount(account: string): Promise<void> {
    await removePendingAccount(this.client, account);
  }

  private async saveAccount(account: string, frontier: string): Promise<void> {
    try {
      // Create a transaction for both operations
      log.debug(`Saving account ${account} with frontier ${frontier}`);
      if (frontier === "" || frontier === null) {
        log.error(`Frontier is empty for account ${account}`);
        Deno.exit(1);
      }
      await saveAccount(this.client, account, frontier);
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

    try {
      const inserted = await saveBlocks(this.client, blocks);
      log.debug(
        `Successfully inserted ${inserted} new blocks out of ${
          Object.keys(blocks).length
        } attempted.`,
      );
      this.metrics.addBlocks(inserted);
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
    account: string | string[],
    _nodeWebsocket: boolean = false,
  ): Promise<void> {
    try {
      const accounts = Array.isArray(account) ? account : [account];
      await addToPendingAccounts(this.client, accounts);
    } catch (error) {
      log.error(
        `Failed to add account ${account} to pending: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error; // Re-throw to be handled by caller
    }
  }

  private async loadPendingAccounts(batchSize: number = -1): Promise<string[]> {
    if (batchSize === -1) {
      batchSize = config.pending_accounts_batch_size;
    }
    return await loadPendingAccounts(this.client, batchSize);
  }

  public async queueAccount(
    account: string,
    nodeWebsocket: boolean = false,
  ): Promise<void> {
    await this.addToPendingAccounts(account, nodeWebsocket);
  }

  private async getNewBlocks(allBlocks: string[]): Promise<string[]> {
    if (allBlocks.length === 0) return [];
    if (!config.identify_new_blocks) {
      return allBlocks;
    }

    try {
      return await getNewBlocks(this.client, allBlocks);
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
      const blockHashes = await getHashesFromBlocksQueue(
        this.client,
        config.block_queue_select_batch_size,
      );

      if (blockHashes.length === 0) {
        // No blocks to process, schedule next run
        setTimeout(() => this.processBlocksQueue(), 10);
        return;
      }

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
              await this.queueAccount(newAddress);
            }
          }

          // Get the block hashes to delete
          const hashesToDelete = Object.keys(blocksInfoResponse.blocks);
          await removeHashesFromBlocksQueue(this.client, hashesToDelete);
          log.debug(
            `Deleted batch of ${hashesToDelete.length} blocks from queue.`,
          );
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
            await this.removePendingAccount(account);
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
        await this.addBlocksToQueue(blockBatch);
      }

      await this.saveAccount(account, latestBlockHash);
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

  private async getFrontiers(
    accounts: string[],
  ): Promise<Record<string, string>> {
    return await getFrontiers(this.client, accounts);
  }

  private async processBatch(accounts: string[]): Promise<void> {
    try {
      const accountFrontiers = await this.getFrontiers(accounts);

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
        const pendingAccounts = await this.loadPendingAccounts();
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

  private async addBlocksToQueue(blockHashes: string[]): Promise<void> {
    if (blockHashes.length === 0) return;

    try {
      const inserted = await addBlocksToQueue(this.client, blockHashes);
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
