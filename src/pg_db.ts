import { Client, Pool } from "postgres";
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
  const client = new Pool(
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
  await client.connect();
  log.info(
    `Connected to PostgreSQL at ${DEFAULT_PG_CONFIG.hostname}:${DEFAULT_PG_CONFIG.port}`,
  );
  return client;
}

export async function getCurrentLedgerPosition(
  client: Client,
  defaultPosition: string,
): Promise<string> {
  const result = await client.queryObject<{ value: string }>(
    "SELECT value FROM ledger_loader_metadata WHERE key = 'ledger_position'",
  );
  if (result.rows.length > 0) {
    return result.rows[0].value;
  } else {
    await updateLedgerPosition(client, defaultPosition);
    return defaultPosition;
  }
}

export async function updateLedgerPosition(
  client: Client,
  position: string,
): Promise<void> {
  await client.queryArray`
    INSERT INTO ledger_loader_metadata (key, value) VALUES ('ledger_position', ${position})
    ON CONFLICT (key) DO UPDATE SET value = ${position};
  `;
}

export async function removePendingAccount(
  client: Client,
  account: string,
): Promise<void> {
  await client
    .queryArray`DELETE FROM ledger_loader_pending_accounts WHERE account = ${account}`;
}

export async function saveAccount(
  client: Client,
  account: string,
  frontier: string,
): Promise<void> {
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
  client: Client,
  blocks: { [key: string]: BlockInfo },
): Promise<number> {
  if (Object.keys(blocks).length === 0) return 0;

  const BATCH_SIZE = 500; // Process blocks in chunks to avoid large memory usage
  const blockHashes = Object.keys(blocks);
  let totalInserted = 0;

  for (let i = 0; i < blockHashes.length; i += BATCH_SIZE) {
    const batchHashes = blockHashes.slice(i, i + BATCH_SIZE);
    const batchBlocks = batchHashes.map((hash) => [hash, blocks[hash]]);

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
    const columnarData: any[][] = columns.map(() => []);
    const dataTypes = [
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "bigint",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "varchar",
      "integer",
      "boolean",
      "varchar",
      "bigint",
      "bigint",
    ];

    for (const [, info] of batchBlocks) {
      const hash = blockHashes.find((h) => blocks[h] === info);
      let row: any[];
      if (info.contents.type !== "state") {
        row = [
          hash,
          info.contents.type,
          info.block_account,
          info.contents.previous || null,
          info.contents.representative || null,
          info.balance ? BigInt(info.balance) : null,
          info.contents.link || null,
          info.contents.link_as_account || info.contents.destination || null,
          null,
          info.contents.signature,
          info.contents.work,
          info.contents.type || null,
          info.height,
          info.confirmed === "true",
          info.successor || null,
          info.amount ? BigInt(info.amount) : null,
          info.local_timestamp ? BigInt(info.local_timestamp) : null,
        ];
      } else {
        row = [
          hash,
          info.contents.type,
          info.contents.account,
          info.contents.previous || null,
          info.contents.representative || null,
          info.contents.balance ? BigInt(info.contents.balance) : null,
          info.contents.link || null,
          info.contents.link_as_account || null,
          info.contents.destination || null,
          info.contents.signature,
          info.contents.work,
          info.contents.subtype || info.subtype || null,
          info.height,
          !!info.confirmed,
          info.successor || null,
          info.amount ? BigInt(info.amount) : null,
          info.local_timestamp ? BigInt(info.local_timestamp) : null,
        ];
      }
      row.forEach((val, i) => columnarData[i].push(val));
    }

    const unnestClauses = columns
      .map((_, i) => `unnest($${i + 1}::${dataTypes[i]}[])`)
      .join(", ");

    const query = `
      INSERT INTO ledger_loader_blocks (${columns.join(", ")})
      SELECT ${unnestClauses}
      ON CONFLICT (hash) DO NOTHING;
    `;

    try {
      const result = await client.queryArray(query, columnarData);
      totalInserted += result.rowCount ?? 0;
    } catch (e) {
      log.error(`Error during bulk block insert: ${e.message}`);
      throw e;
    }
  }

  return totalInserted;
}

export async function addToPendingAccounts(
  client: Client,
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
  await client.queryArray(query, [accs]);
}

export async function loadPendingAccounts(
  client: Client,
  batchSize: number,
): Promise<string[]> {
  const result = await client.queryObject<{ account: string }>(
    `SELECT account FROM ledger_loader_pending_accounts ORDER BY id LIMIT ${batchSize}`,
  );
  return result.rows.map((row) => row.account);
}

export async function getNewBlocks(
  client: Client,
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
    const result = await client.queryObject<{ hash: string }>(
      "SELECT hash FROM ledger_loader_blocks WHERE hash = ANY($1)",
      [chunk],
    );
    result.rows.forEach((row) => existingBlocks.add(row.hash));
  }

  return allBlocks.filter((hash) => !existingBlocks.has(hash));
}

export async function getHashesFromBlocksQueue(
  client: Client,
  limit: number,
): Promise<string[]> {
  const result = await client.queryObject<{ hash: string }>(
    `SELECT hash FROM ledger_loader_blocks_queue LIMIT ${limit}`,
  );
  return result.rows.map((row) => row.hash);
}

export async function removeHashesFromBlocksQueue(
  client: Client,
  hashes: string[],
): Promise<void> {
  if (hashes.length === 0) return;
  await client.queryArray(
    "DELETE FROM ledger_loader_blocks_queue WHERE hash = ANY($1)",
    [hashes],
  );
}

export async function getFrontiers(
  client: Client,
  accounts: string[],
): Promise<Record<string, string>> {
  if (accounts.length === 0) return {};
  const result = await client.queryObject<
    { account: string; frontier: string }
  >(
    "SELECT account, frontier FROM ledger_loader_accounts WHERE account = ANY($1)",
    [accounts],
  );
  const frontiers = result.rows.reduce((acc, row) => {
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
  client: Client,
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
      NOT EXISTS (SELECT 1 FROM ledger_loader_blocks b WHERE b.hash = h.hash) AND
      NOT EXISTS (SELECT 1 FROM ledger_loader_blocks_queue bq WHERE bq.hash = h.hash)
    ON CONFLICT (hash) DO NOTHING;
  `;
  const result = await client.queryArray(query, [blockHashes]);
  return result.rowCount ?? 0;
}
