import { Block } from "./types.ts";
import { NanoCrawler } from "./pg_crawler.ts";
import { log } from "./logger.ts";

interface ConfirmationMessage {
  topic: "confirmation";
  message: {
    account: string;
    amount: string;
    hash: string;
    confirmation_type: "active_quorum" | "active_confirmation_height";
    block: Block;
  };
  options?: {
    confirmation_type:
      | "active"
      | "active_quorum"
      | "active_confirmation_height";
  };
}

export class NanoWebSocket {
  private ws!: WebSocket;
  private reconnectAttempts: number = 0;
  private readonly maxReconnectAttempts: number = 5;
  private readonly reconnectDelay: number = 1000;
  private pingInterval: number = 30000; // 30 seconds
  private pingTimeout: number = 5000; // 5 seconds
  private pingTimer?: number;
  private pongReceived: boolean = false;
  private shouldContinue: boolean = true;
  constructor(
    private readonly wsAddress: string,
    private readonly crawler: NanoCrawler,
  ) {
    this.connect();
  }

  private connect() {
    this.ws = new WebSocket(this.wsAddress);

    this.ws.addEventListener("open", () => {
      log.info("Connected to Nano node");
      this.reconnectAttempts = 0;
      this.subscribeToConfirmations();
      this.startPingInterval();
    });

    this.ws.addEventListener("message", (event: MessageEvent) => {
      try {
        const message = JSON.parse(event.data);

        if (message.ack === "pong") {
          this.pongReceived = true;
          return;
        }

        if (message.topic === "confirmation") {
          this.handleNewBlock(message as ConfirmationMessage);
        }
      } catch (error) {
        log.error("Error processing message:", error);
      }
    });

    this.ws.addEventListener("close", () => {
      log.debug("Connection closed");
      this.handleReconnect();
    });

    this.ws.addEventListener("error", (event: Event) => {
      log.error("WebSocket error:", event);
      this.handleReconnect();
    });
  }

  private handleReconnect() {
    if (!this.shouldContinue) {
      log.debug("Not reconnecting as connection was intentionally closed");
      return;
    }

    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      log.warn(
        `Attempting to reconnect (${this.reconnectAttempts}/${this.maxReconnectAttempts})...`,
      );
      setTimeout(
        () => this.connect(),
        this.reconnectDelay * this.reconnectAttempts,
      );
    } else {
      log.error("Max reconnection attempts reached");
    }
  }

  private subscribeToConfirmations() {
    if (this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        action: "subscribe",
        topic: "confirmation",
        options: {
          confirmation_type: "active",
        },
      }));
    }
  }

  private async handleNewBlock(message: ConfirmationMessage) {
    const block = message.message.block;

    // Extract relevant accounts from the block
    const accounts = new Set<string>();

    // Add sender account
    accounts.add(block.account);

    // Add recipient account (could be in link_as_account or destination)
    if (block.link_as_account) {
      accounts.add(block.link_as_account);
    }
    if (block.destination) {
      accounts.add(block.destination);
    }

    // Queue all relevant accounts for processing
    for (const account of accounts) {
      try {
        await this.crawler.queueAccount(account, true);
        log.debug(
          `Queued account ${account} from new block ${message.message.hash}`,
        );
      } catch (error) {
        log.error(
          `Failed to queue account ${account}: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
  }

  private startPingInterval() {
    this.stopPingInterval(); // Clear any existing interval
    this.pingTimer = setInterval(() => this.ping(), this.pingInterval);
  }

  private stopPingInterval() {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = undefined;
    }
  }

  private ping() {
    if (this.ws.readyState === WebSocket.OPEN) {
      this.pongReceived = false;
      this.ws.send(JSON.stringify({ action: "ping" }));

      setTimeout(() => {
        if (!this.pongReceived) {
          log.error("Ping timeout - no pong received");
          this.ws.close(); // This will trigger reconnection through the 'close' event
        }
      }, this.pingTimeout);
    }
  }

  public close() {
    this.stopPingInterval();
    if (this.ws) {
      this.ws.close();
      this.shouldContinue = false;
    }
  }

  public isConnected(): boolean {
    return this.ws.readyState === WebSocket.OPEN;
  }
}
