export const config = {
  // How often to report metrics
  metric_report_time_ms: 30_000,

  // How many accounts to pull from the pending queue at a time
  pending_accounts_batch_size: 10_000,

  // Batch size to query for new blocks
  new_blocks_batch_size: 999,
  new_block_filter_enabled: true,

  // Delay between processing accounts
  per_batch_delay_ms: 100,

  // Batch size for the chain RPC call
  chain_query_batch_size: 50_000,

  // Batch size for the blocks info RPC call
  blocks_info_batch_size: 10_000,

  // How many blocks to insert at a time
  block_insert_batch_size: 57,

  block_queue_select_batch_size: 10_000,

  // How many accounts are retrieved from the pending queue at a time
  // this is different to how many accounts are retrieved from the DB
  // to populate the pending queue
  account_processing_batch_size: 100,

  // Timeout for RPC calls
  rpc_call_timeout_ms: 30_000,

  // Maximum number of retries for RPC calls
  rpc_call_max_retries: 3,

  // How many accounts to pull from the ledger at a time
  ledger_parse_batch_size: 1_000,
  ledger_parse_interval: 60_000,
  genesis_account:
    "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
  burn_address:
    "nano_1111111111111111111111111111111111111111111111111111hifc8npp",

  // Whether to identify new blocks
  identify_new_blocks: false,
  indexes_enabled: true,
};
