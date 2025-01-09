import { NanoCrawler } from "./crawler.ts";
import { initializeDatabase } from "./db.ts";
import { log } from "./logger.ts";
import { NanoWebSocket } from "./nano-websocket.ts";

if (import.meta.main) {
  const GENESIS_ACCOUNT = "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3";
  const hostname = Deno.args.find(arg => !arg.startsWith('--')) || "127.0.0.1";
  const RPC_ENDPOINT = `http://${hostname}:7076`;
  const WS_ENDPOINT = `ws://${hostname}:7078`;
  const DATA_DIR = "./nano_data";

  const db = initializeDatabase();
  const crawler = new NanoCrawler(RPC_ENDPOINT, db);
  const wsClient = new NanoWebSocket(WS_ENDPOINT, crawler);
  
  try {
    await crawler.crawl(GENESIS_ACCOUNT);
    log.info("Exploration completed successfully!");
  } catch (error) {
    console.error("Exploration failed:", error);
    Deno.exit(1);
  }
}
