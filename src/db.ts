import { config } from "./config_loader.ts";
import { Database } from "jsr:@db/sqlite@0.12";

let db: Database | null = null;

export function initializeDatabase(): Database {
  if (db) {
    return db;
  }

  // By default, 'fileMustExist: false' means the file will be created if it doesn't exist.
  // If you specifically need to create only if not existing, you can adjust the options as needed.
  db = new Database("./nano.db");
  // Executing pragma statements
  db.exec("pragma journal_mode = WAL");         // WAL mode for better concurrency and write performance
  db.exec("pragma synchronous = NORMAL");      // Balance durability and performance
  db.exec("pragma temp_store = MEMORY");       // Temporary data stored in memory
  db.exec("pragma cache_size = -1048576");     // ~1GB for cache (negative for KB)
  db.exec("pragma locking_mode = NORMAL");     // Normal locking mode for safe multi-process access
  db.exec("pragma mmap_size = 2147483648");    // 2GB memory-mapped I/O
  db.exec("pragma foreign_keys = OFF");        // Disable foreign key checks for inserts
  db.exec("pragma page_size = 65536");         // 64KB page size (requires rebuilding the database)

  // Create accounts table
  db.exec(`
    CREATE TABLE IF NOT EXISTS accounts (
      account TEXT PRIMARY KEY
    )
  `);

  // Create pending_accounts table
  db.exec(`
    CREATE TABLE IF NOT EXISTS pending_accounts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account TEXT NOT NULL UNIQUE
    )
  `);
  if (config.indexes_enabled) {
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_pending_account 
      ON pending_accounts(account)
    `);
  }

  // Create blocks table
  db.exec(`
    CREATE TABLE IF NOT EXISTS blocks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      hash TEXT UNIQUE,
      type TEXT,
      account TEXT,
      previous TEXT,
      representative TEXT,
      balance INTEGER,
      link TEXT,
      link_as_account TEXT,
      destination TEXT,
      signature TEXT,
      work TEXT,
      subtype TEXT,
      height INTEGER,
      confirmed BOOLEAN,
      successor TEXT,
      amount INTEGER,
      local_timestamp INTEGER
    )
  `);
  if (config.indexes_enabled) {
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_block_hash 
      ON blocks(hash)
    `);
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_block_local_timestamp 
      ON blocks(local_timestamp)
    `);
  }

  return db;
}

export function getDatabase(): Database {
  if (!db) {
    throw new Error("Database not initialized. Call initializeDatabase() first.");
  }
  return db;
}

export function closeDatabase(): void {
  if (db) {
    db.close();
    db = null;
  }
}
