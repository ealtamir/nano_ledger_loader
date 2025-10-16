import { Pool } from "postgres";
import type { BlockInfo } from "./types.ts";
import { config } from "./config_loader.ts";
import { log } from "./logger.ts";

// Default PostgreSQL configuration
const DEFAULT_PG_CONFIG = {
  user: Deno.env.get("POSTGRES_USER") || "postgres",
  password: Deno.env.get("POSTGRES_PASSWORD") || "your_password",
  database: Deno.env.get("POSTGRES_DB") || "postgres",
  hostname: Deno.env.get("POSTGRES_HOST") || "127.0.0.1",
  port: Number(Deno.env.get("POSTGRES_PORT")) || 5432,
};

export async function initializeDatabase(): Promise<Pool> {
  const pool = new Pool(
    {
      hostname: DEFAULT_PG_CONFIG.hostname,
      port: DEFAULT_PG_CONFIG.port,
      database: DEFAULT_PG_CONFIG.database,
      user: DEFAULT_PG_CONFIG.user,
      password: DEFAULT_PG_CONFIG.password,
    },
    10,
    true,
  );
  await pool.connect();
  log.info(
    `Connected to PostgreSQL at ${DEFAULT_PG_CONFIG.hostname}:${DEFAULT_PG_CONFIG.port}`,
  );
  return pool;
}

// Helper function for running queries with proper connection management
async function runQuery<T = Record<string, unknown>>(
  pool: Pool,
  query: string,
  params?: unknown[],
): Promise<T[]> {
  using client = await pool.connect();
  const result = await client.queryObject<T>(query, params);
  return result.rows;
}

// Helper function for running queries that return row count
async function runQueryWithCount(
  pool: Pool,
  query: string,
  params?: unknown[],
): Promise<number> {
  using client = await pool.connect();
  const result = await client.queryArray(query, params);
  return result.rowCount ?? 0;
}

export async function getCurrentLedgerPosition(
  pool: Pool,
  defaultPosition: string,
): Promise<string> {
  const rows = await runQuery<{ value: string }>(
    pool,
    "SELECT value FROM ledger_loader_metadata WHERE key = 'ledger_position'",
  );
  if (rows.length > 0) {
    return rows[0].value;
  } else {
    await updateLedgerPosition(pool, defaultPosition);
    return defaultPosition;
  }
}

export async function updateLedgerPosition(
  pool: Pool,
  position: string,
): Promise<void> {
  using client = await pool.connect();
  await client.queryArray`
    INSERT INTO ledger_loader_metadata (key, value) VALUES ('ledger_position', ${position})
    ON CONFLICT (key) DO UPDATE SET value = ${position};
  `;
}

export async function removePendingAccount(
  pool: Pool,
  account: string,
): Promise<void> {
  using client = await pool.connect();
  await client
    .queryArray`DELETE FROM ledger_loader_pending_accounts WHERE account = ${account}`;
}

export async function saveAccount(
  pool: Pool,
  account: string,
  frontier: string,
): Promise<void> {
  using client = await pool.connect();
  const transaction = client.createTransaction("save_account");
  await transaction.begin();
  try {
    await transaction.queryArray`
      INSERT INTO ledger_loader_accounts (account, frontier) VALUES (${account}, ${frontier})
      ON CONFLICT (account) DO UPDATE SET frontier = ${frontier};
    `;
    await transaction.queryArray`
      DELETE FROM ledger_loader_pending_accounts WHERE account = ${account};
    `;
    await transaction.commit();
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
}

export async function saveBlocks(
  pool: Pool,
  blocks: { [key: string]: BlockInfo },
): Promise<number> {
  if (Object.keys(blocks).length === 0) return 0;

  const BATCH_SIZE = 500; // Process blocks in chunks to avoid large memory usage
  const blockHashes = Object.keys(blocks);
  let totalInserted = 0;

  for (let i = 0; i < blockHashes.length; i += BATCH_SIZE) {
    const batchHashes = blockHashes.slice(i, i + BATCH_SIZE);
    const batchBlocks = batchHashes.map((hash) =>
      [hash, blocks[hash]] as [string, BlockInfo]
    );

    const columns = [
      "hash",
      "type",
      "account",
      "previous",
      "representative",
      "balance",
      "link",
      "link_as_account",
      "destination",
      "signature",
      "work",
      "subtype",
      "height",
      "confirmed",
      "successor",
      "amount",
      "local_timestamp",
    ];

    // Transpose the data into columnar arrays for unnest
    const columnarData: unknown[][] = columns.map(() => []);
    const dataTypes = [
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "numeric",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "integer",
      "boolean",
      "varchar",
      "numeric",
      "timestamptz",
    ];

    for (const [, info] of batchBlocks) {
      const hash = blockHashes.find((h) => blocks[h] === info);
      let row: unknown[];
      if (info.contents.type !== "state") {
        row = [
          hash,
          info.contents.type,
          info.block_account,
          info.contents.previous || null,
          info.contents.representative || null,
          info.balance ? info.balance : null,
          info.contents.link || null,
          info.contents.link_as_account || info.contents.destination || null,
          null,
          info.contents.signature,
          info.contents.work,
          info.contents.type || null,
          info.height,
          info.confirmed === "true",
          info.successor || null,
          info.amount ? info.amount : null,
          info.local_timestamp
            ? new Date(Number(BigInt(info.local_timestamp)) * 1000)
              .toISOString()
            : null,
        ];
      } else {
        row = [
          hash,
          info.contents.type,
          info.contents.account,
          info.contents.previous || null,
          info.contents.representative || null,
          info.contents.balance ? info.contents.balance : null,
          info.contents.link || null,
          info.contents.link_as_account || null,
          info.contents.destination || null,
          info.contents.signature,
          info.contents.work,
          info.contents.subtype || info.subtype || null,
          info.height,
          !!info.confirmed,
          info.successor || null,
          info.amount ? info.amount : null,
          info.local_timestamp
            ? new Date(Number(BigInt(info.local_timestamp)) * 1000)
              .toISOString()
            : null,
        ];
      }
      row.forEach((val, i) => columnarData[i].push(val));
    }

    const unnestClauses = columns
      .map((_, i) => `unnest($${i + 1}::${dataTypes[i]}[])`)
      .join(", ");

    const query = `
      INSERT INTO block_confirmations (${columns.join(", ")})
      SELECT ${unnestClauses}
      ON CONFLICT (hash, local_timestamp) DO NOTHING;
    `;

    try {
      const inserted = await runQueryWithCount(pool, query, columnarData);
      totalInserted += inserted;
    } catch (e) {
      log.error(
        `Error during bulk block insert: ${
          e instanceof Error ? e.message : String(e)
        }`,
      );
      throw e;
    }
  }

  return totalInserted;
}

export async function addToPendingAccounts(
  pool: Pool,
  accounts: string | string[],
): Promise<void> {
  const accs = Array.isArray(accounts) ? accounts : [accounts];
  if (accs.length === 0) return;

  // Use VALUES clause for bulk insert, which is more portable and less ambiguous
  // for drivers than unnest with array parameters.
  const query = `
    INSERT INTO ledger_loader_pending_accounts (account)
    SELECT UNNEST($1::varchar[])
    ON CONFLICT (account) DO NOTHING;
  `;
  using client = await pool.connect();
  await client.queryArray(query, [accs]);
}

export async function loadPendingAccounts(
  pool: Pool,
  batchSize: number,
): Promise<string[]> {
  const rows = await runQuery<{ account: string }>(
    pool,
    `SELECT account FROM ledger_loader_pending_accounts ORDER BY id LIMIT ${batchSize}`,
  );
  return rows.map((row) => row.account);
}

export async function getNewBlocks(
  pool: Pool,
  allBlocks: string[],
): Promise<string[]> {
  if (allBlocks.length === 0) return [];
  if (!config.identify_new_blocks) {
    return allBlocks;
  }

  const existingBlocks = new Set<string>();
  const CHUNK_SIZE = config.new_blocks_batch_size;

  for (let i = 0; i < allBlocks.length; i += CHUNK_SIZE) {
    const chunk = allBlocks.slice(i, i + CHUNK_SIZE);
    const rows = await runQuery<{ hash: string }>(
      pool,
      "SELECT hash FROM block_confirmations WHERE hash = ANY($1)",
      [chunk],
    );
    rows.forEach((row) => existingBlocks.add(row.hash));
  }

  return allBlocks.filter((hash) => !existingBlocks.has(hash));
}

export async function getHashesFromBlocksQueue(
  pool: Pool,
  limit: number,
): Promise<string[]> {
  const rows = await runQuery<{ hash: string }>(
    pool,
    `SELECT hash FROM ledger_loader_blocks_queue LIMIT ${limit}`,
  );
  return rows.map((row) => row.hash);
}

export async function removeHashesFromBlocksQueue(
  pool: Pool,
  hashes: string[],
): Promise<void> {
  if (hashes.length === 0) return;
  using client = await pool.connect();
  await client.queryArray(
    "DELETE FROM ledger_loader_blocks_queue WHERE hash = ANY($1)",
    [hashes],
  );
}

export async function getFrontiers(
  pool: Pool,
  accounts: string[],
): Promise<Record<string, string>> {
  if (accounts.length === 0) return {};
  const rows = await runQuery<{ account: string; frontier: string }>(
    pool,
    "SELECT account, frontier FROM ledger_loader_accounts WHERE account = ANY($1)",
    [accounts],
  );
  const frontiers = rows.reduce((acc, row) => {
    acc[row.account] = row.frontier;
    return acc;
  }, {} as Record<string, string>);

  for (const account of accounts) {
    if (!frontiers[account]) {
      frontiers[account] = "";
    }
  }
  return frontiers;
}

export async function addBlocksToQueue(
  pool: Pool,
  blockHashes: string[],
): Promise<number> {
  if (blockHashes.length === 0) return 0;

  const query = `
    WITH hashes(hash) AS (
      SELECT * FROM unnest($1::varchar[])
    )
    INSERT INTO ledger_loader_blocks_queue (hash)
    SELECT h.hash FROM hashes h
    WHERE 
      NOT EXISTS (SELECT 1 FROM block_confirmations b WHERE b.hash = h.hash) AND
      NOT EXISTS (SELECT 1 FROM ledger_loader_blocks_queue bq WHERE bq.hash = h.hash)
    ON CONFLICT (hash) DO NOTHING;
  `;
  return await runQueryWithCount(pool, query, [blockHashes]);
}
