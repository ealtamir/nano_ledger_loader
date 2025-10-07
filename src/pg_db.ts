import { config } from "./config_loader.ts";
import postgres from "npm:postgres";

// Default PostgreSQL configuration
const DEFAULT_PG_CONFIG = {
  user: Deno.env.get("POSTGRES_USER") || "postgres",
  password: Deno.env.get("POSTGRES_PASSWORD") || "postgres",
  database: Deno.env.get("POSTGRES_DB") || "postgres",
  hostname: Deno.env.get("POSTGRES_HOST") || "127.0.0.1",
  port: Number(Deno.env.get("POSTGRES_PORT")) || 5432,
  // Connection pool settings
  max: 3,
  idle_timeout: 30,
};

let sql: ReturnType<typeof postgres> | null = null;

export function initializeDatabase() {
  if (sql) {
    return sql;
  }

  // Create a connection pool
  sql = postgres({
    host: DEFAULT_PG_CONFIG.hostname,
    port: DEFAULT_PG_CONFIG.port,
    database: DEFAULT_PG_CONFIG.database,
    username: DEFAULT_PG_CONFIG.user,
    password: DEFAULT_PG_CONFIG.password,
    max: DEFAULT_PG_CONFIG.max,
    idle_timeout: DEFAULT_PG_CONFIG.idle_timeout,
    types: {
      bigint: postgres.BigInt,
    },
  });

  // Initialize database schema
  initializeSchema().catch((err) => {
    console.error("Failed to initialize database schema:", err);
    throw err;
  });

  return sql;
}

async function initializeSchema() {
  if (!sql) {
    throw new Error("SQL client not initialized");
  }

  // Set some PostgreSQL specific optimizations
  await sql`SET synchronous_commit TO OFF`; // Improves write performance

  // Create accounts table
  await sql`
    CREATE TABLE IF NOT EXISTS accounts (
      account TEXT PRIMARY KEY,
      frontier TEXT
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS blocks_queue (
      hash TEXT PRIMARY KEY
    )
  `;

  // Create pending_accounts table
  await sql`
    CREATE TABLE IF NOT EXISTS pending_accounts (
      id SERIAL PRIMARY KEY,
      account TEXT NOT NULL UNIQUE
    )
  `;

  if (config.indexes_enabled) {
    await sql`
      CREATE INDEX IF NOT EXISTS idx_pending_account 
      ON pending_accounts(account)
    `;
  }

  // Create blocks table
  await sql`
    CREATE TABLE IF NOT EXISTS blocks (
      hash TEXT PRIMARY KEY,
      type TEXT,
      account TEXT,
      previous TEXT,
      representative TEXT,
      balance BIGINT,
      link TEXT,
      link_as_account TEXT,
      destination TEXT,
      signature TEXT,
      work TEXT,
      subtype TEXT,
      height INTEGER,
      confirmed BOOLEAN,
      successor TEXT,
      amount BIGINT,
      local_timestamp BIGINT
    )
  `;

  if (config.indexes_enabled) {
    await sql`
      CREATE INDEX IF NOT EXISTS idx_block_local_timestamp 
      ON blocks(local_timestamp)
    `;
  }
}

export function getDatabase() {
  if (!sql) {
    throw new Error(
      "Database not initialized. Call initializeDatabase() first.",
    );
  }
  return sql;
}

export async function closeDatabase() {
  if (sql) {
    await sql.end();
    sql = null;
  }
}

// Batch insert helper function
// export async function batchInsert(
//   tableName: string,
//   columns: string[],
//   values: any[][],
// ) {
//   if (values.length === 0) return;

//   const sql = getDatabase();

//   // Construct the column names part of the query
//   const columnsPart = columns.join(", ");

//   // Create a values array for the SQL template literal
//   const flatValues = values.flat();

//   // Create placeholders for the values
//   const placeholdersArray = [];
//   for (let i = 0; i < values.length; i++) {
//     const rowPlaceholders = [];
//     for (let j = 0; j < columns.length; j++) {
//       rowPlaceholders.push(`$${i * columns.length + j + 1}`);
//     }
//     placeholdersArray.push(`(${rowPlaceholders.join(", ")})`);
//   }

//   // Join all placeholders
//   const placeholders = placeholdersArray.join(", ");

//   // Build and execute the query using tagged template literals
//   // We need to use dynamic SQL here since we can't use template literals directly with table names
//   await sql.unsafe(
//     `
//     INSERT INTO ${tableName} (${columnsPart})
//     VALUES ${placeholders}
//     ON CONFLICT DO NOTHING
//   `,
//     flatValues,
//   );
// }

// // Transaction helper
// export async function transaction<T>(
//   callback: (sql: ReturnType<typeof postgres>) => Promise<T>,
// ): Promise<T> {
//   const db = getDatabase();
//   return db.begin(async (sql) => {
//     return await callback(sql);
//   });
// }
