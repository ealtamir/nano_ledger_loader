import { BlockInfo, BlocksInfoResponse, ChainResponse, LedgerResponse } from "./types.ts";
import { log } from "./logger.ts";

export class NanoRPC {
  private rpcUrl: string;

  constructor(rpcUrl: string) {
    this.rpcUrl = rpcUrl;
  }

  private async makeRPCCall<T>(action: string, params: Record<string, unknown>): Promise<T> {
    const maxRetries = 3;
    const timeout = 30000; // 30 seconds

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeout);
        const body = JSON.stringify({
            action,
            ...params,
          });

        const response = await fetch(this.rpcUrl, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
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
          throw new Error(`Failed after ${maxRetries} attempts: ${error instanceof Error ? error.message : String(error)}`);
        }

        // Exponential backoff: 1s, 2s, 4s...
        const backoffTime = Math.pow(2, attempt) * 1000;
        await new Promise(resolve => setTimeout(resolve, backoffTime));
      }
    }

    // TypeScript needs this even though it's unreachable
    throw new Error('Unexpected end of retry loop');
  }

  async getLedger(account: string, count: number = 1000): Promise<LedgerResponse> {
    return await this.makeRPCCall<LedgerResponse>('ledger', {
      account,
      count: count.toString(),
      representative: "true",
      weight: "true",
      pending: "true",
    });
  }

  async getChain(block: string, count: number = -1): Promise<ChainResponse> {
    return await this.makeRPCCall<ChainResponse>('chain', {
      block,
      count: count,
    });
  }

  async *getBlocksInfo(blocks: string[]): AsyncGenerator<BlocksInfoResponse> {
    const MAX_BLOCKS_PER_CALL = 100;
    log.info(`Getting blocks info for ${blocks.length} blocks`);

    let processedChunks = 0;
    
    for (let i = 0; i < blocks.length; i += MAX_BLOCKS_PER_CALL) {
      const blockChunk = blocks.slice(i, i + MAX_BLOCKS_PER_CALL);
      const response = await this.makeRPCCall<BlocksInfoResponse>('blocks_info', {
        json_block: 'true',
        hashes: blockChunk,
      });
      
      yield response;
    }
  }
} 