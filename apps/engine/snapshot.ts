import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import type { OrderSnapshot } from "commons";

// The engine keeps everything in Maps, which are not JSON serializable as is.
// This module converts the live state to a plain object, writes it atomically,
// and restores it on boot. It also carries lastProcessedId, the Redis stream id
// of the last to-engine-queue entry that was applied, so recovery can replay
// every command that landed after the snapshot was taken.

export type OpenOrder = {
  userId: string;
  orderId: string;
  qty: number;
  filledQty: number;
};

export type PriceLevel = {
  availableQty: number;
  openOrders: OpenOrder[];
};

export type OrderBook = {
  marketId: string;
  bids: Map<string, PriceLevel>;
  asks: Map<string, PriceLevel>;
  lastTradedPrice: number;
  markPrice: number;
  indexPrice: number;
};

export type EngineUser = {
  balance: {
    available: string;
    locked: string;
  };
};

export type Position = {
  id: string;
  userId: string;
  market_id: string;
  qty: number;
  entryValue: number;
  margin: number;
  createdAt: string;
  updatedAt: string;
};

export type OrderIndexEntry = {
  userId: string;
  order: OrderSnapshot;
};

// The four Maps that make up engine state, passed in by reference so restore
// can repopulate them in place.
export type EngineState = {
  users: Map<string, EngineUser>;
  orderBooks: Map<string, OrderBook>;
  orderIndex: Map<string, OrderIndexEntry>;
  positions: Map<string, Map<string, Position>>;
};

type SerializableOrderBook = Omit<OrderBook, "bids" | "asks"> & {
  bids: [string, PriceLevel][];
  asks: [string, PriceLevel][];
};

export type SnapshotFile = {
  version: 1;
  lastProcessedId: string;
  takenAt: string;
  users: [string, EngineUser][];
  orderBooks: [string, SerializableOrderBook][];
  orderIndex: [string, OrderIndexEntry][];
  positions: [string, [string, Position][]][];
};

export function buildSnapshot(
  state: EngineState,
  lastProcessedId: string,
): SnapshotFile {
  const orderBooks: [string, SerializableOrderBook][] = [];
  for (const [marketId, book] of state.orderBooks.entries()) {
    orderBooks.push([
      marketId,
      {
        marketId: book.marketId,
        lastTradedPrice: book.lastTradedPrice,
        markPrice: book.markPrice,
        indexPrice: book.indexPrice,
        bids: Array.from(book.bids.entries()),
        asks: Array.from(book.asks.entries()),
      },
    ]);
  }

  const positions: [string, [string, Position][]][] = [];
  for (const [userId, byMarket] of state.positions.entries()) {
    positions.push([userId, Array.from(byMarket.entries())]);
  }

  return {
    version: 1,
    lastProcessedId,
    takenAt: new Date().toISOString(),
    users: Array.from(state.users.entries()),
    orderBooks,
    orderIndex: Array.from(state.orderIndex.entries()),
    positions,
  };
}

export function restoreSnapshot(state: EngineState, snapshot: SnapshotFile) {
  state.users.clear();
  state.orderBooks.clear();
  state.orderIndex.clear();
  state.positions.clear();

  for (const [userId, user] of snapshot.users) {
    state.users.set(userId, user);
  }

  for (const [marketId, book] of snapshot.orderBooks) {
    state.orderBooks.set(marketId, {
      marketId: book.marketId,
      lastTradedPrice: book.lastTradedPrice,
      markPrice: book.markPrice ?? 0,
      indexPrice: book.indexPrice ?? 0,
      bids: new Map(book.bids),
      asks: new Map(book.asks),
    });
  }

  for (const [orderId, entry] of snapshot.orderIndex) {
    state.orderIndex.set(orderId, entry);
  }

  for (const [userId, byMarket] of snapshot.positions) {
    state.positions.set(userId, new Map(byMarket));
  }
}

// Atomic write: write to a temp file in the same directory, then rename over
// the target. rename is atomic on the same filesystem, so a crash mid-write
// never leaves a half-written snapshot.
export async function writeSnapshotFile(path: string, snapshot: SnapshotFile) {
  await mkdir(dirname(path), { recursive: true });
  const tmp = `${path}.tmp`;
  await writeFile(tmp, JSON.stringify(snapshot), "utf8");
  await rename(tmp, path);
}

export async function readSnapshotFile(
  path: string,
): Promise<SnapshotFile | null> {
  try {
    const raw = await readFile(path, "utf8");
    return JSON.parse(raw) as SnapshotFile;
  } catch (err: any) {
    if (err?.code === "ENOENT") {
      return null;
    }
    throw err;
  }
}