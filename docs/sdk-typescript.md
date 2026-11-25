# TypeScript SDK

The **@soroscan/sdk** is the official TypeScript/JavaScript client for SoroScan. It is designed to work in both server-side (Node.js) and client-side (Browser, React, Vite) environments.

## Installation

```bash
npm install @soroscan/sdk
# or
yarn add @soroscan/sdk
```

## Basic Usage

### Client Setup

```typescript
import { SoroScanClient } from "@soroscan/sdk";

const client = new SoroScanClient({
  baseUrl: "https://api.soroscan.io",
  apiKey: "your-api-key", // optional for public endpoints
});
```

### Fetching Events

```typescript
const events = await client.getEvents({
  contractId: "CCAAA...",
  eventType: "transfer",
  first: 50,
});

for (const event of events.items) {
  console.log(`[${event.ledger}] ${event.type}: ${event.txHash}`);
}
```

## Advanced Features

### Webhook Subscriptions

```typescript
const webhook = await client.subscribeWebhook({
  url: "https://your-app.com/api/webhook",
  triggers: ["event.created", "transaction.success"],
  contractId: "CCAAA...",
});

console.log(`Webhook secret for verification: ${webhook.secret}`);
```

### Async Iteration (Pagination)

```typescript
let after: string | null = null;

do {
  const page = await client.getEvents({ first: 100, after });
  // Process events...
  after = page.pageInfo.hasNextPage ? page.pageInfo.endCursor : null;
} while (after);
```
### Tagged Events (SC-24)

You can submit events classified by up to 4 tags to support efficient event category indexing:

```typescript
const response = await client.recordTaggedEvent({
  contractId: "CCAAA...",
  eventType: "transfer",
  payloadHash: "a".repeat(64),
  tags: ["defi", "token"],
});

console.log(`Status: ${response.status}, tags: ${response.tags.join(", ")}`);
```

### Structured Events (SC-38)

Submit a versioned, deduplicated event. The `correlationId` makes retries safe — the contract rejects a repeated ID without publishing a second event:

```typescript
const result = await client.recordStructuredEvent({
  contractId: "CCAAA...",
  eventType: "escrow",
  payloadHash: "a".repeat(64),
  schemaVersion: 1,
  correlationId: "b".repeat(64), // unique 32-byte hex id
});

console.log(result.status); // "submitted" | "failed"
```

### Batch Event Recording (SC-29)

Record up to 25 events in a single transaction:

```typescript
const result = await client.recordEventsBatch({
  events: [
    { contractId: "CCAAA...", eventType: "transfer", payloadHash: "a".repeat(64) },
    { contractId: "CCAAA...", eventType: "swap",     payloadHash: "b".repeat(64) },
  ],
});

console.log("New total:", result.totalEvents);
```

### Contract Events (SC-16)

Fetch up to `limit` recent events for a specific contract (default 100):

```typescript
const events = await client.getContractEvents("CCAAA...", 20);

for (const event of events) {
  console.log(event.ledger, event.type, event.txHash);
}
```

### Contract Recent Events (SC-30)

Fetch the newest events for a contract, newest first. The limit is clamped to the server-side maximum of 20:

```typescript
const recent = await client.getContractRecentEvents("CCAAA...", 10);

for (const event of recent) {
  console.log(event.ledger, event.type);
}
```

### Contract Health (SC-16)

Check the indexing health of a tracked contract:

```typescript
const health = await client.getContractHealth("CCAAA...");

console.log("Status:", health.status);            // "healthy" | "unhealthy"
console.log("Failures:", health.consecutiveFailures);
console.log("Last event:", health.lastEventTime);
```

### Contract Pause Status (SC-28)

Check whether the SoroScan contract is currently paused:

```typescript
const status = await client.getContractStatus();

if (status.paused) {
  console.log("Event recording is paused. Admin:", status.admin);
} else {
  console.log("Active. Total events:", status.totalEvents);
}
```

### Indexer Authorization (SC-9 / SC-15)

Authorize a new indexer address, check whether an address is a registered indexer, and retrieve the current contract admin:

```typescript
// Authorize an indexer
const added = await client.addIndexer({ indexerAddress: "GABC..." });
console.log(added.status); // "submitted"

// Check registration
const { is_indexer } = await client.isIndexer("GABC...");
console.log("Is indexer:", is_indexer);

// Retrieve the admin address
const { admin_address } = await client.getAdmin();
console.log("Admin:", admin_address);
```

### Indexer Statistics (SC-13)

Retrieve how many events a specific indexer has recorded:

```typescript
const stats = await client.getIndexerStats("GABC...");

console.log("Indexer:", stats.indexer);
console.log("Events recorded:", stats.eventsRecorded);
```

### Test Webhook Delivery

Trigger a test delivery to verify a webhook endpoint is reachable:

```typescript
const result = await client.testWebhook("wh_001");

console.log(result.status); // "test_webhook_queued"
```

## Features


- **Strict Typing**: Full TypeScript support for all request and response shapes.
- **Zero Dependencies**: Uses native `fetch` API for a minimal footprint.
- **Dual Build**: Supports both ESM and CJS modules.
- **React/Vite Friendly**: Ready for modern frontend frameworks.

## Error Handling

```typescript
import { SoroScanError } from "@soroscan/sdk";

try {
  await client.getContract({ contractId: "INVALID" });
} catch (err) {
  if (err instanceof SoroScanError) {
    console.error(`API Error: ${err.message} (Status: ${err.statusCode})`);
  }
}
```

For more details, see the [TypeScript SDK README](https://github.com/Harbduls/soroscan/tree/main/sdk/typescript).

If you want to add or maintain SDK methods, use the [SDK Development Guide](./contributing/sdk-development.md) and the comprehensive [SDK & Library Development Guide](./sdk/sdk-development-guide.md).
