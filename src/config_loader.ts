import { z } from "https://deno.land/x/zod@v3.23.8/mod.ts";
import { config as configTemplate } from "../config.ts";

// Define the configuration schema
const ConfigSchema = z.object({
  metric_report_time_ms: z.number().default(30_000),
  pending_accounts_batch_size: z.number().default(1000),
  new_blocks_batch_size: z.number().default(500),
  per_batch_delay_ms: z.number().default(1_000),
  chain_query_batch_size: z.number().default(50_000),
  blocks_info_batch_size: z.number().default(40),
  account_processing_batch_size: z.number().default(100),
  rpc_call_timeout_ms: z.number().default(30_000),
  rpc_call_max_retries: z.number().default(3),
  identify_new_blocks: z.boolean().default(true),
  indexes_enabled: z.boolean().default(false),
  block_insert_batch_size: z.number().default(50),
  block_queue_select_batch_size: z.number().default(10_000),
  ledger_parse_batch_size: z.number().default(1_000),
  ledger_parse_interval: z.number().default(60_000),
  genesis_account: z.string(),
  burn_address: z.string(),
});

// Create a type from the schema
type Config = z.infer<typeof ConfigSchema>;

// Validate the imported config against the schema
export const config: Config = ConfigSchema.parse(configTemplate);
