# Super Perps

An in-memory perpetual trading platform built with Bun, TypeScript, Express,
Redis Streams, Prisma, PostgreSQL, WebSockets, and Zod.

The project is structured as a small event-driven monorepo. The API server
accepts user requests, the engine owns trading state and matching, the backend
poller persists engine events to PostgreSQL, and the liquidation checker streams
Binance mark prices into the engine.

![Super Perp - An scalable perpetual trading platform](./perp.png)

## Features

- User sign-up and sign-in with JWT authentication
- Admin market creation
- Collateral on-ramping
- Market and limit order placement
- Order cancellation with unused margin release
- In-memory order books and position tracking
- Trade fill persistence
- Available and locked equity queries
- Open and closed position queries
- Snapshot-based engine recovery
- Mark-price-driven liquidation checks

## Tech Stack

- Bun
- TypeScript
- Express 5
- Redis Streams
- PostgreSQL
- Prisma 7
- Zod
- bcrypt
- jsonwebtoken
- websocket - ws
- Turborepo workspaces

## Monorepo Layout

```text
apps/
  backend/               HTTP API and request loopback to the engine
  engine/                In-memory matching engine, positions, snapshots
  backend-poller/        Persists engine order/fill events into PostgreSQL
  liquidation-checker/   Binance mark price subscriber

packages/
  commons/               Shared message and payload types
  db/                    Prisma client and schema
  typescript-config/     Shared TypeScript config
```

## Architecture

### System Overview

```mermaid
flowchart TB
  client[Client / Trader]

  subgraph api[Backend API]
    backend[Express server<br/>Auth, validation, request loopback]
  end

  subgraph redis[Redis Streams]
    toEngine[[to-engine-queue<br/>Commands into engine]]
    toBackend[[to-backend-queue<br/>Engine responses and events]]
  end

  subgraph trading[Engine Layer]
    engine[Trading Engine<br/>Matching, balances, positions, liquidations]
    snapshot[(Snapshot file<br/>engine-snapshot.json)]
  end

  subgraph persistence[Persistence Layer]
    poller[Backend Poller<br/>Consumes engine events]
    postgres[(PostgreSQL<br/>Users, markets, orders, fills)]
  end

  subgraph risk[Market Data / Risk]
    binance[Binance Futures<br/>Mark price streams]
    checker[Liquidation Checker<br/>Publishes mark_price updates]
  end

  client -->|HTTP /api/v1| backend
  backend -->|trading commands and queries| toEngine
  toEngine -->|consumer group read| engine

  engine -->|order results, fills, equity,<br/>positions, liquidation events| toBackend
  toBackend -->|loopback response| backend
  backend -->|HTTP response| client

  toBackend -->|persist durable events| poller
  poller -->|upsert orders and fills| postgres

  backend -->|signup, signin,<br/>market admin| postgres
  checker -->|load market symbols| postgres
  binance -->|websocket mark prices| checker
  checker -->|mark_price messages| toEngine

  engine <-->|write and restore| snapshot
  toEngine -. replay after lastProcessedId .-> engine
```

The engine is the source of truth for live trading state. It keeps balances,
order books, order index, and positions in memory. It periodically writes a
snapshot to disk and restores from that snapshot on restart, then replays Redis
stream messages after the snapshot's last processed id.

### Backend API

```mermaid
flowchart LR
  client[Client]
  backend[Backend API<br/>apps/backend]
  auth[JWT middleware<br/>Protects trading routes]
  validators[Request validation<br/>Zod schemas and controllers]
  loopback[Loopback map<br/>Waits for matching loopBackId]
  toEngine[[Redis<br/>to-engine-queue]]
  toBackend[[Redis<br/>to-backend-queue]]
  postgres[(PostgreSQL)]

  client -->|HTTP requests| backend
  backend --> auth
  backend --> validators
  validators -->|signup/signin/admin market| postgres
  validators -->|create_order, cancel_order,<br/>onramp, equity, positions| loopback
  loopback -->|xAdd with loopBackId| toEngine
  toBackend -->|engine response with same loopBackId| loopback
  loopback -->|JSON response| backend
  backend --> client
```

The backend is intentionally thin. It handles HTTP concerns, authentication,
input validation, and request/response coordination over Redis. It does not own
matching or live position state.

### Trading Engine

```mermaid
flowchart TB
  toEngine[[Redis<br/>to-engine-queue]]
  toBackend[[Redis<br/>to-backend-queue]]
  snapshot[(Snapshot file)]

  subgraph engine[Trading Engine - apps/engine]
    recovery[Startup recovery<br/>restore snapshot + replay stream]
    router[Message router<br/>applyMessage]
    balances[User balances<br/>available and locked]
    books[Order books<br/>bids, asks, price levels]
    matching[Matcher<br/>market and limit orders]
    positions[Positions<br/>qty, entry value, margin]
    liquidation[Liquidation checks<br/>mark price vs liquidation price]
  end

  toEngine -->|commands and mark_price| router
  recovery --> router
  snapshot <-->|periodic write and restore| recovery
  toEngine -. replay after lastProcessedId .-> recovery

  router --> balances
  router --> matching
  router --> liquidation
  matching --> books
  matching --> balances
  matching --> positions
  liquidation --> positions
  liquidation --> balances

  router -->|responses and events| toBackend
  matching -->|orders and fills| toBackend
  liquidation -->|liquidation event| toBackend
```

The engine consumes commands sequentially from Redis, mutates in-memory state,
and emits results. Snapshot recovery makes the in-memory state restartable
without asking PostgreSQL to rebuild the order book.

### Backend Poller

```mermaid
flowchart LR
  toBackend[[Redis<br/>to-backend-queue]]
  poller[Backend Poller<br/>apps/backend-poller]
  parser[Parse engine payloads<br/>orders and fills]
  orders[(Order table)]
  fills[(Fill table)]

  toBackend -->|consumer group read| poller
  poller -->|create_order success| parser
  poller -->|cancel_order success| parser
  parser -->|upsert order snapshot| orders
  parser -->|upsert fills| fills
  poller -->|xAck after successful persistence| toBackend
```

The backend poller is the persistence bridge. It listens to engine output and
writes durable order and fill records to PostgreSQL after the engine has already
accepted and processed the command.

### Liquidation Checker

```mermaid
flowchart LR
  postgres[(PostgreSQL<br/>Market table)]
  binance[Binance Futures<br/>combined websocket stream]
  checker[Liquidation Checker<br/>apps/liquidation-checker]
  dedupe[Last price cache<br/>skip duplicate mark prices]
  toEngine[[Redis<br/>to-engine-queue]]
  engine[Trading Engine<br/>runs liquidation checks]

  postgres -->|load market ids and symbols| checker
  checker -->|subscribe to symbolusdt@markprice| binance
  binance -->|mark price + index price| checker
  checker --> dedupe
  dedupe -->|mark_price message| toEngine
  toEngine --> engine
```

The liquidation checker does not liquidate positions itself. It only turns
external Binance mark prices into engine messages. The engine remains the only
component that mutates positions and balances.

## Prerequisites

- Bun 1.3+
- PostgreSQL
- Redis
- Network access to Binance futures websocket streams for liquidation checks

## Installation

```bash
bun install
```

## Environment Variables

Create a `.env` file in the directory where you run the services, usually the
repo root during local development.

```env
DATABASE_URL="postgres://user:password@localhost:5432/super_perps"
JWT_SECRET="replace-me"
ADMIN_SECRET="replace-me"
```

Optional engine variables:

```env
SNAPSHOT_PATH="./engine-snapshot.json"
SNAPSHOT_INTERVAL_MS="5000"
MAINTENANCE_MARGIN_RATE="0.005"
```

Notes:

- `DATABASE_URL` is used by the Prisma package.
- `JWT_SECRET` signs and verifies user sessions.
- `ADMIN_SECRET` authorizes admin market creation.
- `SNAPSHOT_PATH` controls where the engine writes its local snapshot.
- Runtime files such as `.env` and `engine-snapshot.json` should not be
  committed.

## Database Setup

Generate the Prisma client:

```bash
cd packages/db
bun --bun run prisma generate
```

Apply migrations:

```bash
cd packages/db
bun --bun run prisma migrate deploy
```

For local development, you can use Prisma's dev migration command instead:

```bash
cd packages/db
bun --bun run prisma migrate dev
```

## Running Locally

Start Redis and PostgreSQL first.

Run each service in a separate terminal:

```bash
cd apps/engine
bun run index.ts
```

```bash
cd apps/backend
bun run index.ts
```

```bash
cd apps/backend-poller
bun run index.ts
```

```bash
cd apps/liquidation-checker
bun run index.ts
```

The backend API listens on port `3000`.

Recommended startup order:

1. Redis
2. PostgreSQL
3. Engine
4. Backend
5. Backend poller
6. Liquidation checker

## Core Services

### Backend

Location: `apps/backend`

The backend exposes HTTP routes, validates user requests, authenticates JWTs,
and sends trading commands to Redis stream `to-engine-queue`. For request/response
workflows, it waits for matching responses from `to-backend-queue`.

### Engine

Location: `apps/engine`

The engine consumes `to-engine-queue`, applies trading commands, matches orders,
updates balances and positions, and emits results to `to-backend-queue`.

It handles:

- Market creation messages
- On-ramp balance updates
- Equity queries
- Position queries
- Order creation
- Order cancellation
- Mark price updates
- Liquidation checks
- Snapshot and replay recovery

### Backend Poller

Location: `apps/backend-poller`

The backend poller consumes `to-backend-queue` and persists successful order and
fill events to PostgreSQL.

### Liquidation Checker

Location: `apps/liquidation-checker`

The liquidation checker loads markets from PostgreSQL, subscribes to Binance
futures mark price streams, and publishes `mark_price` messages into
`to-engine-queue`. The engine uses these messages to check whether open
positions should be liquidated.

## Redis Streams

### `to-engine-queue`

Commands sent to the engine.

Message types:

- `create_market`
- `onramp`
- `get_equity`
- `get_positions`
- `create_order`
- `cancel_order`
- `mark_price`

### `to-backend-queue`

Responses and events emitted by the engine.

Message types include:

- `create_market`
- `onramp`
- `get_equity`
- `get_positions`
- `create_order`
- `cancel_order`
- `liquidation`

## Engine Snapshots

The engine stores all live trading state in memory, so snapshots are used for
restart recovery.

Snapshot contents:

- Users and balances
- Order books
- Order index
- Positions
- Last processed Redis stream id

On startup, the engine:

1. Reads the snapshot file if it exists.
2. Restores in-memory Maps.
3. Replays stream entries after `lastProcessedId`.
4. Suppresses backend side effects during replay.
5. Starts consuming live messages.

Snapshots are written atomically by writing a temporary file and renaming it over
the target path.

## Liquidation Model

The engine uses isolated margin accounting.

For each position:

- Long positions have positive quantity.
- Short positions have negative quantity.
- Entry value is stored as signed notional.
- Margin is tracked on the position.

Liquidation price is calculated from:

```text
margin + (markPrice - entryPrice) * qty = maintenanceMarginRequirement
```

where maintenance margin is charged on entry notional:

```text
maintenanceMarginRequirement = MMR * abs(qty) * entryPrice
```

A long is liquidated when mark price is less than or equal to its liquidation
price. A short is liquidated when mark price is greater than or equal to its
liquidation price.

Liquidation currently flattens the position in engine state, removes seized
margin from locked balance, and emits a liquidation event. It does not yet route
the liquidated exposure through an insurance fund, ADL system, or order-book
auction.

## API Reference

### Health

```http
GET /health
```

Returns:

```json
{
  "success": true,
  "message": "The server is up"
}
```

### Sign Up

```http
POST /api/v1/signup
```

Request:

```json
{
  "username": "alice",
  "password": "password123"
}
```

### Sign In

```http
POST /api/v1/signin
```

Request:

```json
{
  "username": "alice",
  "password": "password123"
}
```

Returns a JWT token.

### Create Market

```http
POST /api/v1/admin/market
```

Headers:

```http
token: <ADMIN_SECRET>
```

Request:

```json
{
  "symbol": "BTC",
  "imageUrl": "https://example.com/btc.png"
}
```

The `symbol` is also used by the liquidation checker to build Binance stream
names such as `btcusdt@markprice`.

### On-Ramp Collateral

```http
POST /api/v1/onramp
Authorization: Bearer <jwt>
```

Request:

```json
{
  "amount": 1000
}
```

### Get Available Equity

```http
GET /api/v1/equity/available
Authorization: Bearer <jwt>
```

Returns the user's available and locked balances from the engine.

### Create Order

```http
POST /api/v1/order
Authorization: Bearer <jwt>
```

Request:

```json
{
  "price": 65000,
  "qty": 0.1,
  "equity": 500,
  "type": "long",
  "marketId": "market-id",
  "orderType": "limit"
}
```

Fields:

- `type`: `long` or `short`
- `orderType`: `market` or `limit`
- `equity`: isolated margin to lock for the order

### Cancel Order

```http
DELETE /api/v1/order
Authorization: Bearer <jwt>
```

Request:

```json
{
  "orderId": "order-id"
}
```

Cancels an open or partially filled order and releases unused margin.

### Get Orders For Market

```http
GET /api/v1/orders/:marketId
Authorization: Bearer <jwt>
```

Returns all persisted user orders for a market.

### Get Open Orders For Market

```http
GET /api/v1/orders/open/:marketId
Authorization: Bearer <jwt>
```

Returns open and partially filled persisted user orders for a market.

### Get Open Positions

```http
GET /api/v1/positions/open/:marketId
Authorization: Bearer <jwt>
```

Returns open positions from the engine.

### Get Closed Positions

```http
GET /api/v1/positions/closed/:marketId
Authorization: Bearer <jwt>
```

Returns closed positions from the engine.

### Get Fills

```http
GET /api/v1/fills
Authorization: Bearer <jwt>
```

Returns persisted fills where the user was either maker or taker.

## Order Lifecycle

1. User submits an order to the backend.
2. Backend publishes a `create_order` message to `to-engine-queue`.
3. Engine validates balances, locks margin, and attempts to match the order.
4. Matching creates fills and updates maker/taker positions.
5. Unfilled limit quantity rests on the book.
6. Unfilled market quantity is cancelled and unused margin is released.
7. Engine emits the order and fills to `to-backend-queue`.
8. Backend resolves the HTTP request.
9. Backend poller persists the order and fills.

## Development Checks

Typecheck the engine:

```bash
bunx tsc -p apps/engine/tsconfig.json
```

Typecheck the liquidation checker:

```bash
bunx tsc -p apps/liquidation-checker/tsconfig.json
```

Format files:

```bash
bunx prettier --write "**/*.{ts,tsx,md}"
```

## Current Limitations

- Liquidation settlement is simplified and does not yet include insurance fund,
  ADL, or auction mechanics.
- There are no automated integration tests yet for full order matching,
  recovery, and liquidation flows.
