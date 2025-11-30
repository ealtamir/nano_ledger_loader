import { config } from "./config_loader.ts";
import { Database } from "sqlite";

let db: Database | null = null;

export function initializeDatabase(): Database {
  if (db) {
    return db;
  }

  // By default, 'fileMustExist: false' means the file will be created if it doesn't exist.
  // If you specifically need to create only if not existing, you can adjust the options as needed.
  db = new Database("./nano.db", { unsafeConcurrency: true });
  // Executing pragma statements
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = FULL");
  db.exec("PRAGMA wal_autocheckpoint = 1000"); // auto-checkpoint after 1000 pages
  db.exec("pragma temp_store = MEMORY"); // Temporary data stored in memory
  db.exec("pragma cache_size = -1048576"); // ~1GB for cache (negative for KB)
  db.exec("pragma locking_mode = NORMAL"); // Normal locking mode for safe multi-process access
  db.exec("pragma mmap_size = 2147483648"); // 2GB memory-mapped I/O
  db.exec("pragma foreign_keys = OFF"); // Disable foreign key checks for inserts
  db.exec("pragma page_size = 65536"); // 64KB page size (requires rebuilding the database)

  // Create accounts table
  db.exec(`
    CREATE TABLE IF NOT EXISTS accounts (
      account TEXT PRIMARY KEY,
      frontier TEXT
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS blocks_queue (
      hash TEXT PRIMARY KEY
    )
  `);

  // Create pending_accounts table
  db.exec(`
    CREATE TABLE IF NOT EXISTS pending_accounts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account TEXT NOT NULL UNIQUE
    )
  `);
  // if (config.indexes_enabled) {
  //   db.exec(`
  //     CREATE INDEX IF NOT EXISTS idx_pending_account
  //     ON pending_accounts(account)
  //   `);
  // }

  // Create blocks table
  db.exec(`
    CREATE TABLE IF NOT EXISTS blocks (
      hash PRIMARY KEY,
      type TEXT,
      account TEXT,
      previous TEXT,
      representative TEXT,
      balance TEXT,
      link TEXT,
      link_as_account TEXT,
      destination TEXT,
      signature TEXT,
      work TEXT,
      subtype TEXT,
      height INTEGER,
      confirmed BOOLEAN,
      successor TEXT,
      amount TEXT,
      local_timestamp INTEGER
    )
  `);
  if (config.indexes_enabled) {
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_block_local_timestamp 
      ON blocks(local_timestamp)
    `);
  }

  db.exec(`
    CREATE TABLE IF NOT EXISTS ledger_positions (
      id INTEGER PRIMARY KEY,
      account TEXT
    )
  `);

  return db;
}

export function getDatabase(): Database {
  if (!db) {
    throw new Error(
      "Database not initialized. Call initializeDatabase() first.",
    );
  }
  return db;
}

export function closeDatabase(): void {
  if (db) {
    db.close();
    db = null;
  }
}

/**
 * Gets the current ledger position account.
 * If no ledger position exists in the database, returns the genesis account.
 * @param genesisAccount The genesis account to use as fallback
 * @returns The current ledger position account
 */
export function getCurrentLedgerPosition(genesisAccount: string): string {
  const database = getDatabase();

  // Try to get the ledger position from the database
  const result = database.prepare(
    "SELECT account FROM ledger_positions WHERE id = 1",
  ).get();

  // If a ledger position exists, return it; otherwise, return the genesis account
  return result?.account || genesisAccount;
}

/**
 * Updates the current ledger position in the database.
 * @param account The account to set as the current ledger position
 */
export function updateLedgerPosition(account: string): void {
  const database = getDatabase();

  const transaction = database.transaction(() => {
    const exists = database.prepare(
      "SELECT 1 FROM ledger_positions WHERE id = 1",
    ).get();
    if (exists) {
      database.prepare("UPDATE ledger_positions SET account = ? WHERE id = 1")
        .run(account);
    } else {
      database.prepare(
        "INSERT INTO ledger_positions (id, account) VALUES (1, ?)",
      ).run(account);
    }
  });

  transaction();
}
