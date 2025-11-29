import { NanoCrawler } from "./crawler.ts";
import { initializeDatabase as initializeSqliteDb } from "./db.ts";
import { log } from "./logger.ts";
import { NanoWebSocket } from "./nano-websocket.ts";
import { initializeDatabase as initializePostgresDb } from "./pg_db.ts";
import { Syncer } from "./syncer.ts";
import { config } from "./config_loader.ts";

if (import.meta.main) {
  const GENESIS_ACCOUNT =
    "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3";
  const hostname = Deno.args.find((arg) => !arg.startsWith("--")) ||
    "127.0.0.1";
  const RPC_ENDPOINT = `http://${hostname}:7076`;
  const WS_ENDPOINT = `ws://${hostname}:7078`;

  log.info(`Starting Nano Crawler with RPC endpoint ${RPC_ENDPOINT}`);
  log.info(`Starting Nano Crawler with WS endpoint ${WS_ENDPOINT}`);

  // Initialize databases
  const sqliteDb = initializeSqliteDb();
  const pgPool = await initializePostgresDb();

  // Initialize the syncer (SQLite -> PostgreSQL)
  let syncer: Syncer | null = null;
  if (config.syncer_enabled) {
    syncer = new Syncer(sqliteDb, pgPool);
    syncer.start();
    log.info("Syncer started for SQLite -> PostgreSQL synchronization");
  }

  const crawler = new NanoCrawler(RPC_ENDPOINT, sqliteDb);
  // const wsClient = new NanoWebSocket(WS_ENDPOINT, crawler);

  try {
    await crawler.crawl(GENESIS_ACCOUNT);
    log.info("Exploration completed successfully!");
    // wsClient.close();

    // Stop syncer gracefully
    if (syncer) {
      syncer.stop();
      const stats = syncer.getStats();
      log.info(
        `Syncer stats: ${stats.totalRuns} runs, ${stats.totalBlocksInserted} blocks inserted total`,
      );
    }

    pgPool.end();
  } catch (error) {
    console.error("Exploration failed:", error);
    if (syncer) {
      syncer.stop();
    }
    Deno.exit(1);
  }
}
