
import { Database } from "jsr:@db/sqlite@0.12";

let db: Database | null = null;

export function initializeDatabase(): Database {
  if (db) {
    return db;
  }

  // By default, 'fileMustExist: false' means the file will be created if it doesn't exist.
  // If you specifically need to create only if not existing, you can adjust the options as needed.
  db = new Database("./nano_A.db");
  // Executing pragma statements
    db.exec("pragma journal_mode = WAL");
    db.exec("pragma synchronous = normal");
    db.exec("pragma temp_store = memory");

  // Create accounts table
  db.exec(`
    CREATE TABLE IF NOT EXISTS accounts (
      account TEXT PRIMARY KEY
    )
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_account 
    ON accounts(account)
  `);

  // Create pending_accounts table
  db.exec(`
    CREATE TABLE IF NOT EXISTS pending_accounts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account TEXT NOT NULL UNIQUE
    )
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_pending_account 
    ON pending_accounts(account)
  `);

  // Create blocks table
  db.exec(`
    CREATE TABLE IF NOT EXISTS blocks (
      hash TEXT PRIMARY KEY,
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
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_blocks_account 
    ON blocks(account)
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_blocks_hash 
    ON blocks(hash)
  `);

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
