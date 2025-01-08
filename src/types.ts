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
  confirmed: boolean;
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