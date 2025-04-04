// RPC Response Types

export interface BlocksInfoResponse {
  blocks: {
    [hash: string]: BlockInfo;
  };
}

export interface Block {
  type: "state" | "send" | "receive" | "open" | "change";
  account: string;
  previous: string;
  representative: string;
  balance: string;
  link: string;
  link_as_account: string;
  destination: string;
  signature: string;
  work: string;
  subtype?: "send" | "receive" | "change" | "epoch";
  height?: number;
  confirmed?: boolean;
  successor?: string;
}

export interface BlockInfo {
  block_account: string;
  amount: string;
  balance: string;
  height: number;
  local_timestamp: string;
  confirmed: boolean | "true" | "false";
  subtype: string;
  successor: string;
  contents: Block;
}

export interface LedgerResponse {
  accounts: {
    [account: string]: {
      frontier: string;
      open_block: string;
      representative_block: string;
      balance: string;
      modified_timestamp: string;
      block_count: string;
    };
  };
}

export interface ChainResponse {
  blocks: string[];
}

export interface AccountInfoResponse {
  // Base fields (always available)
  account: string;
  frontier: string;
  open_block: string;
  representative_block: string;
  balance: string;
  modified_timestamp: string;
  block_count: string;
  account_version: string;

  // Available in v19.0+
  confirmation_height?: string;

  // Available in v21.0+
  confirmation_height_frontier?: string;

  // Optional fields when include_confirmed=true (v22.0+)
  confirmed_balance?: string;
  confirmed_height?: string;
  confirmed_frontier?: string;
  confirmed_representative?: string; // Only when representative=true
  confirmed_receivable?: string; // Only when receivable=true

  // Keeping these from original interface as they might be used elsewhere
  representative?: string;
  pending?: string;
}
