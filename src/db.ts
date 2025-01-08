import { DB } from "sqlite";

let db: DB | null = null;

export function initializeDatabase(): DB {
    if (db) return db;

    db = new DB("./nano.db", {mode: "create"});

    // Create accounts table
    db.execute(`
        CREATE TABLE IF NOT EXISTS accounts (
            account TEXT PRIMARY KEY
        )
    `);
    db.execute(`CREATE INDEX IF NOT EXISTS idx_account ON accounts(account)`);

    db.execute(`
        CREATE TABLE IF NOT EXISTS pending_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account TEXT NOT NULL UNIQUE
        )
    `);
    db.execute(`CREATE INDEX IF NOT EXISTS idx_pending_account ON pending_accounts(account)`);

    // Create blocks table
    db.execute(`
        CREATE TABLE IF NOT EXISTS blocks (
            hash TEXT PRIMARY KEY,
            type TEXT NOT NULL,
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
            local_timestamp TIMESTAMP
        )
    `);
    db.execute(`CREATE INDEX IF NOT EXISTS idx_blocks_account ON blocks(account)`);
    db.execute(`CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(hash)`);

    return db;
}

export function getDatabase(): DB {
    if (!db) {
        throw new Error('Database not initialized. Call initializeDatabase() first.');
    }
    return db;
}

export function closeDatabase(): void {
    if (db) {
        db.close();
        db = null;
    }
}
