import {
  AccountInfoResponse,
  BlocksInfoResponse,
  ChainResponse,
  LedgerResponse,
} from "./types.ts";
import { config } from "./config_loader.ts";
import { log } from "./logger.ts";

export class NanoRPC {
  private rpcUrl: string;

  constructor(rpcUrl: string) {
    this.rpcUrl = rpcUrl;
  }

  private async makeRPCCall<T>(
    action: string,
    params: Record<string, unknown>,
  ): Promise<T> {
    const maxRetries = config.rpc_call_max_retries;
    const timeout = config.rpc_call_timeout_ms;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeout);
        const body = JSON.stringify({
          action,
          ...params,
        });

        const response = await fetch(this.rpcUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body,
          signal: controller.signal,
        });

        clearTimeout(timeoutId);

        if (!response.ok) {
          throw new Error(`RPC call failed: ${response.statusText}`);
        }

        return await response.json() as T;
      } catch (error: unknown) {
        const isLastAttempt = attempt === maxRetries - 1;
        if (isLastAttempt) {
          throw new Error(
            `Failed after ${maxRetries} attempts: ${
              error instanceof Error ? error.message : String(error)
            }`,
          );
        }

        // Exponential backoff: 1s, 2s, 4s...
        const backoffTime = Math.pow(2, attempt) * 1000;
        await new Promise((resolve) => setTimeout(resolve, backoffTime));
      }
    }

    // TypeScript needs this even though it's unreachable
    throw new Error("Unexpected end of retry loop");
  }

  async getLedger(
    account: string,
    count: number = -1,
  ): Promise<LedgerResponse> {
    if (count === -1) {
      count = config.account_processing_batch_size;
    }
    return await this.makeRPCCall<LedgerResponse>("ledger", {
      account,
      count: count.toString(),
      representative: "true",
      weight: "true",
      pending: "true",
    });
  }

  async getChain(block: string, count: number = -1): Promise<ChainResponse> {
    const CHAIN_QUERY_BATCH = config.chain_query_batch_size;
    let currentBlock = block;
    let allBlocks: string[] = [];
    let shouldContinue = true;

    while (shouldContinue) {
      const response = await this.makeRPCCall<ChainResponse>("chain", {
        block: currentBlock,
        count: count === -1 ? CHAIN_QUERY_BATCH : count,
      });

      // If this is a single query (count !== -1), return the response directly
      if (count !== -1) {
        return response;
      }

      allBlocks = allBlocks.concat(response.blocks || []);

      // Stop conditions:
      // 1. Empty blocks list
      // 2. Less blocks than batch size (reached the end)
      // 3. Single block that's the same as our query (no more history)
      if (
        !response.blocks?.length ||
        response.blocks.length < CHAIN_QUERY_BATCH ||
        (response.blocks.length === 1 && response.blocks[0] === currentBlock)
      ) {
        shouldContinue = false;
      } else {
        // Get the last (oldest) hash for the next query
        currentBlock = response.blocks[response.blocks.length - 1];
      }
    }

    return { blocks: allBlocks };
  }

  async *getSuccessorsGenerator(
    block: string | undefined,
    count: number = -1,
    account: string | undefined,
  ): AsyncGenerator<string[]> {
    let currentBlock = block;
    if (!currentBlock && account) {
      try {
        const accountInfo = await this.getAccountInfo(account);
        if (accountInfo.error) {
          yield [];
          return;
        }
        currentBlock = accountInfo.open_block;
      } catch (error) {
        log.error(`Failed to fetch account info: ${error}`);
        throw new Error(`Failed to fetch account info: ${error}`);
      }
    }

    if (!currentBlock) {
      throw new Error(
        "No block provided and no account specified to fetch frontier block",
      );
    }

    const CHAIN_QUERY_BATCH = config.chain_query_batch_size;
    let shouldContinue = true;

    while (shouldContinue) {
      const response: ChainResponse = await this.makeRPCCall<ChainResponse>(
        "successors",
        {
          block: currentBlock,
          count: count === -1 ? CHAIN_QUERY_BATCH : count,
        },
      );

      // If this is a single query (count !== -1), yield and return
      if (count !== -1) {
        yield response.blocks || [];
        return;
      }

      // Yield the current batch of blocks
      if (response.blocks?.length) {
        yield response.blocks;
      }

      // Stop conditions:
      // 1. Empty blocks list
      // 2. Less blocks than batch size (reached the end)
      // 3. Single block that's the same as our query (no more history)
      if (
        !response.blocks?.length ||
        response.blocks.length < CHAIN_QUERY_BATCH ||
        (response.blocks.length === 1 && response.blocks[0] === currentBlock)
      ) {
        shouldContinue = false;
      } else {
        // Get the last (oldest) hash for the next query
        currentBlock = response.blocks[response.blocks.length - 1];
      }
    }
  }

  async *getBlocksInfo(blocks: string[]): AsyncGenerator<BlocksInfoResponse> {
    const MAX_BLOCKS_PER_CALL = config.blocks_info_batch_size;

    for (let i = 0; i < blocks.length; i += MAX_BLOCKS_PER_CALL) {
      const blockChunk = blocks.slice(i, i + MAX_BLOCKS_PER_CALL);
      const response = await this.makeRPCCall<BlocksInfoResponse>(
        "blocks_info",
        {
          json_block: "true",
          hashes: blockChunk,
        },
      );

      yield response;
    }
  }

  public getAccountInfo(account: string): Promise<AccountInfoResponse> {
    return this.makeRPCCall<AccountInfoResponse>("account_info", {
      account,
      include_confirmed: true,
    });
  }
}
