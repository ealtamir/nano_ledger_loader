# Nano Node SQLite Synchronizer

This tool creates and maintains a SQLite database that mirrors the blockchain data from a Nano node. It establishes a WebSocket connection to the node and continuously listens for new blocks, ensuring the local database stays synchronized with the node's state.

## Features

- Creates a SQLite database matching the Nano node's blockchain data
- Real-time synchronization through WebSocket connection 
- Persistent storage of blockchain data for local querying
- Automatic handling of chain reorganizations
- Efficient block processing with batch operations

## Prerequisites

- Deno runtime installed
- Access to a Nano node
- Network connectivity between this application and the Nano node

## Installation

Clone this repository:
```bash
git clone [repository-url]
cd [repository-name]
```

## Usage

Run the application by providing the IP address of your Nano node as a parameter:
```bash
deno --allow-all src/main.ts <node-ip-address>
```

## Configuration
The application will connect to the specified Nano node using the following default ports:
RPC: 7076
WebSocket: 7078

Make sure your Nano node has both RPC and WebSocket functionality enabled.