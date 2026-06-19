import { createClient } from "redis";
import type { FillSnapshot, OrderSnapshot, ToEngine } from "commons";
import {
  buildSnapshot,
  readSnapshotFile,
  restoreSnapshot,
  writeSnapshotFile,
  type EngineState,
} from "./snapshot";

const STREAM = "to-engine-queue";
const CONSUMER_GROUP = "engine-queue-consumer";
const BACKEND_STREAM = "to-backend-queue";

const client = createClient();
const publisher = createClient();

client.on("error", () => {
  console.log("Client Connection Error");
});

publisher.on("error", () => {
  console.log("Publisher Connection Error");
});

await client.connect();
await publisher.connect();

// Snapshot + recovery configuration.
const SNAPSHOT_PATH = process.env.SNAPSHOT_PATH ?? "./engine-snapshot.json";
const RAW_SNAPSHOT_INTERVAL_MS = Number(
  process.env.SNAPSHOT_INTERVAL_MS ?? 5000,
);
const SNAPSHOT_INTERVAL_MS =
  Number.isFinite(RAW_SNAPSHOT_INTERVAL_MS) && RAW_SNAPSHOT_INTERVAL_MS > 0
    ? RAW_SNAPSHOT_INTERVAL_MS
    : 5000;

// Risk configuration for liquidation. Isolated margin, maintenance margin
// charged on entry notional. See computeLiquidationPrice for the formula.
const RAW_MAINTENANCE_MARGIN_RATE = Number(
  process.env.MAINTENANCE_MARGIN_RATE ?? 0.005,
);
const MAINTENANCE_MARGIN_RATE =
  Number.isFinite(RAW_MAINTENANCE_MARGIN_RATE) &&
  RAW_MAINTENANCE_MARGIN_RATE >= 0
    ? RAW_MAINTENANCE_MARGIN_RATE
    : 0.005;

// Redis stream id of the last to-engine-queue entry that was applied to state.
// Persisted inside the snapshot so recovery knows where to resume the replay.
let lastProcessedId = "0-0";

// While true, all side effects to the backend stream are suppressed. Used
// during recovery replay so the backend and the db poller do not see (and
// re-persist) responses they already handled before the crash.
let replaying = false;

function publishToBackend(fields: Record<string, string>) {
  if (replaying) {
    return Promise.resolve(undefined);
  }
  return publisher.xAdd(BACKEND_STREAM, "*", fields);
}

// Redis stream ids look like "<ms>-<seq>". Compare them numerically so we can
// skip any message at or below lastProcessedId, which makes applying a message
// idempotent no matter which path (replay or live group read) delivered it.
function streamIdLte(a: string, b: string): boolean {
  const aParts = a.split("-");
  const bParts = b.split("-");
  const aMs = Number(aParts[0] ?? 0);
  const aSeq = Number(aParts[1] ?? 0);
  const bMs = Number(bParts[0] ?? 0);
  const bSeq = Number(bParts[1] ?? 0);
  if (aMs !== bMs) {
    return aMs < bMs;
  }
  return aSeq <= bSeq;
}

type Balance = {
  available: string;
  locked: string;
};

type EngineUser = {
  balance: Balance;
};

type OpenOrder = {
  userId: string;
  orderId: string;
  qty: number;
  filledQty: number;
};

type CancellationResult = {
  order: OrderSnapshot;
  cancelledQty: number;
  marginReleased: number;
};

type PriceLevel = {
  availableQty: number;
  openOrders: OpenOrder[];
};

type OrderBook = {
  marketId: string;
  bids: Map<string, PriceLevel>;
  asks: Map<string, PriceLevel>;
  lastTradedPrice: number;
  markPrice: number;
  indexPrice: number;
};

const users = new Map<string, EngineUser>();
const orderBooks = new Map<string, OrderBook>();
const orderIndex = new Map<string, { userId: string; order: OrderSnapshot }>();
const positions = new Map<
  string,
  Map<
    string,
    {
      id: string;
      userId: string;
      market_id: string;
      qty: number;
      entryValue: number; // sum(price * qtySigned)
      margin: number; // isolated margin attributed to this position
      createdAt: string;
      updatedAt: string;
    }
  >
>();

// Single object holding references to all four state Maps, handed to the
// snapshot helpers so they can read and repopulate state in place.
const engineState: EngineState = {
  users,
  orderBooks,
  orderIndex,
  positions,
};

function now() {
  return new Date().toISOString();
}

function isPositiveFinite(value: number) {
  return Number.isFinite(value) && value > 0;
}

function isNonNegativeFinite(value: number) {
  return Number.isFinite(value) && value >= 0;
}

function numberFromBalance(value: string) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function moveLockedToAvailable(
  user: EngineUser,
  lockedDelta: number,
  availableDelta: number,
) {
  user.balance.locked = String(
    Math.max(0, numberFromBalance(user.balance.locked) - lockedDelta),
  );
  user.balance.available = String(
    numberFromBalance(user.balance.available) + availableDelta,
  );
}

function ensureUser(userId: string): EngineUser {
  const existingUser = users.get(userId);
  if (existingUser) {
    return existingUser;
  }

  const newUser: EngineUser = {
    balance: {
      available: "0",
      locked: "0",
    },
  };

  users.set(userId, newUser);
  return newUser;
}

function ensureMarket(marketId: string): OrderBook {
  const existingMarket = orderBooks.get(marketId);
  if (existingMarket) {
    return existingMarket;
  }

  const market: OrderBook = {
    marketId,
    bids: new Map(),
    asks: new Map(),
    lastTradedPrice: 0,
    markPrice: 0,
    indexPrice: 0,
  };

  orderBooks.set(marketId, market);
  return market;
}

function toSide(input: "long" | "short") {
  return input === "long" ? "Bid" : "Ask";
}

function toOrderType(input: "market" | "limit") {
  return input === "market" ? "Market" : "Limit";
}

function createOrderSnapshot(params: {
  id: string;
  userId: string;
  marketId: string;
  side: "Bid" | "Ask";
  orderType: "Market" | "Limit";
  price: number;
  qty: number;
  initialMargin: number;
  filledQty: number;
  status: OrderSnapshot["status"];
}): OrderSnapshot {
  const timestamp = now();

  return {
    id: params.id,
    userId: params.userId,
    market_id: params.marketId,
    orderType: params.orderType,
    side: params.side,
    price: String(params.price),
    qty: String(params.qty),
    initialMargin: String(params.initialMargin),
    filledQty: String(params.filledQty),
    status: params.status,
    createdAt: timestamp,
    updatedAt: timestamp,
  };
}

function createFillSnapshot(params: {
  makerId: string;
  takerId: string;
  makerOrderId: string;
  takerOrderId: string;
  marketId: string;
  qty: number;
  price: number;
}): FillSnapshot {
  return {
    id: crypto.randomUUID(),
    maker_id: params.makerId,
    taker_id: params.takerId,
    qty: String(params.qty),
    pirce: String(params.price),
    maker_order_id: params.makerOrderId,
    taker_order_id: params.takerOrderId,
    market_id: params.marketId,
    createdAt: now(),
  };
}

function ensurePosition(userId: string, marketId: string) {
  let userPositions = positions.get(userId);
  if (!userPositions) {
    userPositions = new Map();
    positions.set(userId, userPositions);
  }

  let pos = userPositions.get(marketId);
  if (!pos) {
    pos = {
      id: crypto.randomUUID(),
      userId,
      market_id: marketId,
      qty: 0,
      entryValue: 0,
      margin: 0,
      createdAt: now(),
      updatedAt: now(),
    };
    userPositions.set(marketId, pos);
  }

  return pos;
}

function updatePositionForFill(
  userId: string,
  marketId: string,
  side: "Bid" | "Ask",
  qty: number,
  price: number,
  marginDelta: number,
) {
  const pos = ensurePosition(userId, marketId);
  const user = ensureUser(userId);

  // side Bid == buy -> positive qty, Ask == sell -> negative qty
  const signedQty = side === "Bid" ? qty : -qty;
  const existingQty = pos.qty;

  if (existingQty === 0 || Math.sign(existingQty) === Math.sign(signedQty)) {
    pos.entryValue += signedQty * price;
    pos.qty += signedQty;
    pos.margin += marginDelta;
    pos.updatedAt = now();
    return;
  }

  const fillAbsQty = Math.abs(signedQty);
  const existingAbsQty = Math.abs(existingQty);
  const closedAbsQty = Math.min(fillAbsQty, existingAbsQty);
  const avgEntryPrice = Math.abs(pos.entryValue / existingQty);
  const oldMarginReleased =
    existingAbsQty === 0 ? 0 : pos.margin * (closedAbsQty / existingAbsQty);
  const newMarginReleased =
    fillAbsQty === 0 ? 0 : marginDelta * (closedAbsQty / fillAbsQty);
  const realizedPnl =
    (price - avgEntryPrice) * Math.sign(existingQty) * closedAbsQty;

  moveLockedToAvailable(
    user,
    oldMarginReleased + newMarginReleased,
    oldMarginReleased + newMarginReleased + realizedPnl,
  );

  const remainingExistingAbsQty = existingAbsQty - closedAbsQty;
  const openingAbsQty = fillAbsQty - closedAbsQty;

  if (remainingExistingAbsQty > 0) {
    const remainingQty = Math.sign(existingQty) * remainingExistingAbsQty;
    pos.qty = remainingQty;
    pos.entryValue =
      Math.sign(existingQty) * avgEntryPrice * remainingExistingAbsQty;
    pos.margin = Math.max(0, pos.margin - oldMarginReleased);
    pos.updatedAt = now();
    return;
  }

  if (openingAbsQty > 0) {
    const openingSign = Math.sign(signedQty);
    const openingMargin =
      fillAbsQty === 0 ? 0 : marginDelta * (openingAbsQty / fillAbsQty);
    pos.qty = openingSign * openingAbsQty;
    pos.entryValue = openingSign * price * openingAbsQty;
    pos.margin = openingMargin;
    pos.updatedAt = now();
    return;
  }

  pos.qty = 0;
  pos.entryValue = 0;
  pos.margin = 0;
  pos.updatedAt = now();
}

type Position = {
  id: string;
  userId: string;
  market_id: string;
  qty: number;
  entryValue: number;
  margin: number;
  createdAt: string;
  updatedAt: string;
};

// Isolated margin liquidation price. The position is liquidated when account
// equity falls to the maintenance margin requirement, charged here on entry
// notional for simplicity:
//   margin + (P - entryPrice) * qty = MMR * absQty * entryPrice
// Solving for P, with qty signed (positive long, negative short):
//   P = entryPrice + (MMR * absQty * entryPrice - margin) / qty
// A long liquidates as price falls to this level, a short as price rises to it.
function computeLiquidationPrice(pos: Position): number | null {
  if (pos.qty === 0) {
    return null;
  }
  const absQty = Math.abs(pos.qty);
  const entryPrice = Math.abs(pos.entryValue / pos.qty);
  return (
    entryPrice +
    (MAINTENANCE_MARGIN_RATE * absQty * entryPrice - pos.margin) / pos.qty
  );
}

// Direct, in-process liquidation. This replaces the HTTP /liquidate hop from
// the reference: the engine already owns the position, so it settles it here.
// This is an accounting settlement at mark price: the position is flattened,
// the attributed isolated margin is seized from the user's locked balance, and
// a liquidation event is emitted for audit. It does not yet trade against the
// book to transfer the exposure to a counterparty (insurance fund / ADL), which
// is a separate policy decision.
async function liquidatePosition(
  userId: string,
  pos: Position,
  markPrice: number,
) {
  const user = ensureUser(userId);
  const entryPrice = pos.qty === 0 ? 0 : Math.abs(pos.entryValue / pos.qty);
  const closedQty = pos.qty; // signed
  const realizedPnl = (markPrice - entryPrice) * closedQty;
  const seizedMargin = pos.margin;

  user.balance.locked = String(
    Math.max(0, numberFromBalance(user.balance.locked) - seizedMargin),
  );

  pos.qty = 0;
  pos.entryValue = 0;
  pos.margin = 0;
  pos.updatedAt = now();

  await publishToBackend({
    messageType: "liquidation",
    userId,
    marketId: pos.market_id,
    qty: String(Math.abs(closedQty)),
    side: closedQty > 0 ? "long" : "short",
    entryPrice: String(entryPrice),
    markPrice: String(markPrice),
    realizedPnl: String(realizedPnl),
    seizedMargin: String(seizedMargin),
  });
}

// Walk every open position in this market and liquidate the ones whose
// liquidation price has been crossed by the latest mark price.
async function runLiquidationChecks(marketId: string, markPrice: number) {
  for (const [userId, byMarket] of positions.entries()) {
    const pos = byMarket.get(marketId);
    if (!pos || pos.qty === 0) {
      continue;
    }

    const liquidationPrice = computeLiquidationPrice(pos);
    if (liquidationPrice === null) {
      continue;
    }

    if (pos.qty > 0) {
      // LONG: safe while mark is above the liquidation price
      if (markPrice > liquidationPrice) {
        continue;
      }
    } else {
      // SHORT: safe while mark is below the liquidation price
      if (markPrice < liquidationPrice) {
        continue;
      }
    }

    await liquidatePosition(userId, pos, markPrice);
  }
}

async function processMarkPrice(
  message: Extract<ToEngine, { messageType: "mark_price" }>,
) {
  const markPrice = Number(message.markPrice);
  const indexPrice = Number(message.indexPrice);

  if (!isPositiveFinite(markPrice) || !isPositiveFinite(indexPrice)) {
    return;
  }

  const book = ensureMarket(message.marketId);
  book.markPrice = markPrice;
  book.indexPrice = indexPrice;
  await runLiquidationChecks(message.marketId, markPrice);
}

function updateOrderState(orderId: string, patch: Partial<OrderSnapshot>) {
  const existing = orderIndex.get(orderId);
  if (!existing) {
    return;
  }

  Object.assign(existing.order, patch, { updatedAt: now() });
}

function addRestingOrder(params: {
  market: OrderBook;
  order: OrderSnapshot;
  remainingQty: number;
}) {
  if (params.remainingQty <= 0) {
    return;
  }

  const priceKey = params.order.price;
  const targetBook =
    params.order.side === "Bid" ? params.market.bids : params.market.asks;
  const existingLevel = targetBook.get(priceKey);

  const openOrder: OpenOrder = {
    userId: params.order.userId,
    orderId: params.order.id,
    qty: Number(params.order.qty),
    filledQty: Number(params.order.filledQty),
  };

  if (existingLevel) {
    existingLevel.availableQty += params.remainingQty;
    existingLevel.openOrders.push(openOrder);
    return;
  }

  targetBook.set(priceKey, {
    availableQty: params.remainingQty,
    openOrders: [openOrder],
  });
}

function markOrderAfterMatch(params: {
  order: OrderSnapshot;
  totalQty: number;
  filledQty: number;
  remainder: number;
  orderType: "market" | "limit";
}) {
  params.order.filledQty = String(params.filledQty);

  if (params.filledQty === params.totalQty) {
    params.order.status = "Filled";
    return;
  }

  if (params.filledQty > 0) {
    params.order.status = "PartiallyFilled";
    return;
  }

  params.order.status = params.orderType === "market" ? "Cancelled" : "Open";
}

function getOppositeBookForSide(market: OrderBook, side: "Bid" | "Ask") {
  return side === "Bid" ? market.asks : market.bids;
}

function cancelOrderInBook(params: {
  market: OrderBook;
  order: OrderSnapshot;
}): CancellationResult | null {
  const targetBook =
    params.order.side === "Bid" ? params.market.bids : params.market.asks;
  const level = targetBook.get(params.order.price);

  if (!level) {
    return null;
  }

  const existingOrder = level.openOrders.find(
    (open) => open.orderId === params.order.id,
  );
  if (!existingOrder) {
    return null;
  }

  const cancelledQty = existingOrder.qty - existingOrder.filledQty;
  if (cancelledQty < 0) {
    return null;
  }

  level.openOrders = level.openOrders.filter(
    (open) => open.orderId !== params.order.id,
  );
  level.availableQty -= cancelledQty;

  if (level.availableQty <= 0) {
    targetBook.delete(params.order.price);
  }

  const totalQty = Number(params.order.qty);
  const marginReleased =
    totalQty === 0
      ? 0
      : Number(params.order.initialMargin) * (cancelledQty / totalQty);

  if (params.order.status === "Open") {
    params.order.status = "Cancelled";
  } else if (params.order.status === "PartiallyFilled") {
    params.order.status = "Cancelled";
  }

  updateOrderState(params.order.id, {
    status: params.order.status,
    filledQty: params.order.filledQty,
  });

  const user = ensureUser(params.order.userId);
  moveLockedToAvailable(user, marginReleased, marginReleased);

  return {
    order: params.order,
    cancelledQty,
    marginReleased,
  };
}

async function processCreateMarket(
  message: Extract<ToEngine, { messageType: "create_market" }> & {
    loopBackId: string;
  },
) {
  ensureMarket(message.marketId);

  await publishToBackend({
    loopBackId: message.loopBackId,
    messageType: "create_market",
    marketId: message.marketId,
    success: "true",
  });
}

async function processOnramp(
  message: Extract<ToEngine, { messageType: "onramp" }> & {
    loopBackId: string;
  },
) {
  const user = ensureUser(message.userId);
  const amount = Number(message.amount);

  if (!isPositiveFinite(amount)) {
    await publishToBackend({
      loopBackId: message.loopBackId,
      messageType: "onramp",
      availableBalance: user.balance.available,
      lockedBalance: user.balance.locked,
    });
    return;
  }

  const nextAvailable = numberFromBalance(user.balance.available) + amount;

  user.balance.available = String(nextAvailable);

  await publishToBackend({
    loopBackId: message.loopBackId,
    messageType: "onramp",
    availableBalance: user.balance.available,
    lockedBalance: user.balance.locked,
  });
}

async function processCreateOrder(
  message: Extract<ToEngine, { messageType: "create_order" }> & {
    loopBackId: string;
  },
) {
  const user = ensureUser(message.userId);
  const market = ensureMarket(message.marketId);

  const equityRequired = Number(message.equity);
  const requestedQty = Number(message.qty);
  const requestedPrice = Number(message.price);

  if (
    !isPositiveFinite(equityRequired) ||
    !isPositiveFinite(requestedQty) ||
    !isNonNegativeFinite(requestedPrice) ||
    (message.type === "limit" && requestedPrice === 0)
  ) {
    await publishToBackend({
      loopBackId: message.loopBackId,
      messageType: "create_order",
      success: "false",
      error: "Invalid order",
      fills: JSON.stringify([]),
    });
    return;
  }

  const availableBalance = numberFromBalance(user.balance.available);

  if (availableBalance < equityRequired) {
    await publishToBackend({
      loopBackId: message.loopBackId,
      messageType: "create_order",
      success: "false",
      error: "Insufficient funds",
      fills: JSON.stringify([]),
    });
    return;
  }

  user.balance.available = String(availableBalance - equityRequired);
  user.balance.locked = String(
    numberFromBalance(user.balance.locked) + equityRequired,
  );

  const side = toSide(message.side);
  const orderType = toOrderType(message.type);
  const takerOrderId = message.orderId;

  const takerOrder = createOrderSnapshot({
    id: takerOrderId,
    userId: message.userId,
    marketId: message.marketId,
    side,
    orderType,
    price: requestedPrice,
    qty: requestedQty,
    initialMargin: equityRequired,
    filledQty: 0,
    status: "Open",
  });

  orderIndex.set(takerOrder.id, { userId: message.userId, order: takerOrder });

  const fills: FillSnapshot[] = [];
  let remainingQty = requestedQty;
  let matchedQty = 0;

  const oppositeBook = getOppositeBookForSide(market, side);
  const sortedPrices = Array.from(oppositeBook.keys()).sort((a, b) =>
    side === "Bid" ? Number(a) - Number(b) : Number(b) - Number(a),
  );

  for (const priceKey of sortedPrices) {
    if (remainingQty <= 0) {
      break;
    }

    const level = oppositeBook.get(priceKey);
    if (!level) {
      continue;
    }

    const bestPrice = Number(priceKey);
    const priceCrosses =
      orderType === "Market" ||
      (side === "Bid"
        ? bestPrice <= requestedPrice
        : bestPrice >= requestedPrice);

    if (!priceCrosses) {
      break;
    }

    for (const openOrder of [...level.openOrders]) {
      if (remainingQty <= 0) {
        break;
      }

      const makerFillable = openOrder.qty - openOrder.filledQty;
      if (makerFillable <= 0) {
        continue;
      }

      const fillQty = Math.min(remainingQty, makerFillable);
      remainingQty -= fillQty;
      matchedQty += fillQty;
      openOrder.filledQty += fillQty;
      level.availableQty -= fillQty;
      market.lastTradedPrice = bestPrice;

      const makerUserId = openOrder.userId;
      const makerOrderEntry = orderIndex.get(openOrder.orderId);

      const fill = createFillSnapshot({
        makerId: makerUserId,
        takerId: message.userId,
        makerOrderId: openOrder.orderId,
        takerOrderId: takerOrder.id,
        marketId: message.marketId,
        qty: fillQty,
        price: bestPrice,
      });

      fills.push(fill);

      // update positions for maker and taker
      if (makerOrderEntry) {
        const makerOrderQty = Number(makerOrderEntry.order.qty);
        const makerMarginShare =
          makerOrderQty === 0
            ? 0
            : Number(makerOrderEntry.order.initialMargin) *
              (fillQty / makerOrderQty);
        updatePositionForFill(
          makerUserId,
          message.marketId,
          makerOrderEntry.order.side,
          fillQty,
          bestPrice,
          makerMarginShare,
        );
      }

      const takerMarginShare =
        requestedQty === 0 ? 0 : equityRequired * (fillQty / requestedQty);
      updatePositionForFill(
        message.userId,
        message.marketId,
        takerOrder.side,
        fillQty,
        bestPrice,
        takerMarginShare,
      );

      if (makerOrderEntry) {
        makerOrderEntry.order.filledQty = String(
          Number(makerOrderEntry.order.filledQty) + fillQty,
        );
        makerOrderEntry.order.updatedAt = now();

        if (
          Number(makerOrderEntry.order.filledQty) ===
          Number(makerOrderEntry.order.qty)
        ) {
          makerOrderEntry.order.status = "Filled";
        } else {
          makerOrderEntry.order.status = "PartiallyFilled";
        }
      }

      takerOrder.filledQty = String(Number(takerOrder.filledQty) + fillQty);
      updateOrderState(takerOrder.id, {
        filledQty: takerOrder.filledQty,
      });
    }

    if (level.availableQty <= 0) {
      oppositeBook.delete(priceKey);
    } else {
      level.openOrders = level.openOrders.filter(
        (open) => open.qty > open.filledQty,
      );
    }
  }

  markOrderAfterMatch({
    order: takerOrder,
    totalQty: requestedQty,
    filledQty: matchedQty,
    remainder: remainingQty,
    orderType: message.type,
  });

  if (message.type === "limit" && remainingQty > 0) {
    addRestingOrder({
      market,
      order: takerOrder,
      remainingQty,
    });
  }

  if (message.type === "market" && remainingQty > 0) {
    const unusedMargin = equityRequired * (remainingQty / requestedQty);
    moveLockedToAvailable(user, unusedMargin, unusedMargin);
  }

  if (Number(takerOrder.filledQty) > 0 && takerOrder.status !== "Cancelled") {
    takerOrder.status =
      Number(takerOrder.filledQty) === requestedQty
        ? "Filled"
        : "PartiallyFilled";
  }

  await publishToBackend({
    loopBackId: message.loopBackId,
    messageType: "create_order",
    success: "true",
    order: JSON.stringify(takerOrder),
    fills: JSON.stringify(fills),
  });
}

async function processCancelOrder(
  message: Extract<ToEngine, { messageType: "cancel_order" }> & {
    loopBackId: string;
  },
) {
  const existing = orderIndex.get(message.orderId);
  if (!existing) {
    await publishToBackend({
      loopBackId: message.loopBackId,
      messageType: "cancel_order",
      success: "false",
      orderId: message.orderId,
      userId: message.userId,
      error: "Order not found",
    });
    return;
  }

  if (existing.userId !== message.userId) {
    await publishToBackend({
      loopBackId: message.loopBackId,
      messageType: "cancel_order",
      success: "false",
      orderId: message.orderId,
      userId: message.userId,
      error: "Not authorized to cancel this order",
    });
    return;
  }

  if (
    existing.order.status === "Filled" ||
    existing.order.status === "Cancelled"
  ) {
    await publishToBackend({
      loopBackId: message.loopBackId,
      messageType: "cancel_order",
      success: "false",
      orderId: message.orderId,
      userId: message.userId,
      error: `The order is ${existing.order.status}`,
    });
    return;
  }

  const market = ensureMarket(existing.order.market_id);
  const result = cancelOrderInBook({ market, order: existing.order });

  if (!result) {
    await publishToBackend({
      loopBackId: message.loopBackId,
      messageType: "cancel_order",
      success: "false",
      orderId: message.orderId,
      userId: message.userId,
      error: "Order not found in order book",
    });
    return;
  }

  await publishToBackend({
    loopBackId: message.loopBackId,
    messageType: "cancel_order",
    success: "true",
    orderId: message.orderId,
    userId: message.userId,
    cancelledQty: String(result.cancelledQty),
    marginReleased: String(result.marginReleased),
    order: JSON.stringify(result.order),
  });
}

async function ensureConsumerGroup() {
  try {
    await client.xGroupCreate(STREAM, CONSUMER_GROUP, "$", {
      MKSTREAM: true,
    });
  } catch (err: any) {
    if (!err.message.includes("BUSYGROUP")) {
      throw err;
    }
  }
}

await ensureConsumerGroup();

// Route a single decoded message to the right processor. Shared by the recovery
// replay and the live loop so that applying a message is defined once and
// behaves identically in both paths.
async function applyMessage(message: { loopBackId: string } & ToEngine) {
  if (message.messageType === "create_market") {
    await processCreateMarket(message);
  }

  if (message.messageType === "onramp") {
    await processOnramp(message);
  }

  if (message.messageType === "create_order") {
    await processCreateOrder(message);
  }

  if (message.messageType === "cancel_order") {
    await processCancelOrder(message);
  }

  if (message.messageType === "mark_price") {
    await processMarkPrice(message);
  }

  if (message.messageType === "get_equity") {
    const user = ensureUser(message.userId);
    await publishToBackend({
      loopBackId: message.loopBackId,
      messageType: "get_equity",
      availableBalance: user.balance.available,
      lockedBalance: user.balance.locked,
    });
  }

  if (message.messageType === "get_positions") {
    const userPositions = positions.get(message.userId) ?? new Map();
    const result: Array<{
      id: string;
      userId: string;
      market_id: string;
      qty: string;
      avgEntryPrice: string;
      status: "open" | "closed";
      createdAt: string;
      updatedAt: string;
    }> = [];

    for (const [marketId, pos] of userPositions.entries()) {
      if (message.marketId && message.marketId !== marketId) continue;
      const status = pos.qty === 0 ? "closed" : "open";
      if (message.status && message.status !== status) continue;

      const qty = String(pos.qty);
      const avgEntryPrice =
        pos.qty === 0 ? "0" : String(Math.abs(pos.entryValue / pos.qty));

      result.push({
        id: pos.id,
        userId: pos.userId,
        market_id: pos.market_id,
        qty,
        avgEntryPrice,
        status,
        createdAt: pos.createdAt,
        updatedAt: pos.updatedAt,
      });
    }

    await publishToBackend({
      loopBackId: message.loopBackId,
      messageType: "get_positions",
      positions: JSON.stringify(result),
    });
  }
}

// Every command on to-engine-queue carries a loopBackId. Anything missing one
// is not a valid command and is ignored.
function decode(rawMessage: Record<string, string> | undefined) {
  if (!rawMessage?.messageType || !rawMessage.loopBackId) {
    return null;
  }
  return rawMessage as { loopBackId: string } & ToEngine;
}

async function flushSnapshot() {
  try {
    const snapshot = buildSnapshot(engineState, lastProcessedId);
    await writeSnapshotFile(SNAPSHOT_PATH, snapshot);
  } catch (err) {
    console.error("Failed to write snapshot", err);
  }
}

// Recovery: load the snapshot, then replay every stream entry that arrived
// after the snapshot was taken. Replay is side-effect free toward the backend
// (replaying = true), so the backend and the db poller never see responses they
// already handled before the crash. State is rebuilt purely from the snapshot
// plus the replayed commands, so nothing in the gap is lost.
async function recover() {
  const snapshot = await readSnapshotFile(SNAPSHOT_PATH);
  if (snapshot) {
    restoreSnapshot(engineState, snapshot);
    lastProcessedId = snapshot.lastProcessedId;
    console.log(
      `Restored snapshot taken at ${snapshot.takenAt}, resuming after ${lastProcessedId}`,
    );
  } else {
    console.log("No snapshot found, starting from an empty state");
  }

  replaying = true;
  let replayed = 0;

  while (true) {
    const response = await client.xRead(
      [{ key: STREAM, id: lastProcessedId }],
      { COUNT: 500 },
    );

    if (!response?.length || !response[0]?.messages?.length) {
      break;
    }

    const messages = response[0]?.messages ?? [];
    for (const entry of messages) {
      const message = decode(entry.message as Record<string, string>);
      if (message) {
        await applyMessage(message);
        replayed += 1;
      }
      lastProcessedId = entry.id;
    }
  }

  replaying = false;

  if (replayed > 0) {
    console.log(`Replayed ${replayed} message(s) after the snapshot`);
    await flushSnapshot();
  }
}

async function main() {
  await recover();

  // Periodic snapshot flush. Data loss is bounded to one interval, and recovery
  // replays the stream from the snapshot's id so that gap is filled on restart.
  setInterval(() => {
    flushSnapshot();
  }, SNAPSHOT_INTERVAL_MS);

  while (true) {
    const response = await client.xReadGroup(
      CONSUMER_GROUP,
      CONSUMER_GROUP,
      [
        {
          key: STREAM,
          id: ">",
        },
      ],
      {
        BLOCK: 0,
        COUNT: 1,
      },
    );

    if (!response?.length || !response[0]?.messages?.length) {
      continue;
    }

    const entry = response[0]?.messages?.[0];
    if (!entry) {
      continue;
    }

    // Idempotency guard: replay may already have applied this id. Skip and ack
    // so the same command is never applied twice.
    if (streamIdLte(entry.id, lastProcessedId)) {
      await client.xAck(STREAM, CONSUMER_GROUP, entry.id);
      continue;
    }

    const message = decode(entry.message as Record<string, string>);
    if (message) {
      await applyMessage(message);
    }

    lastProcessedId = entry.id;
    await client.xAck(STREAM, CONSUMER_GROUP, entry.id);
  }
}

main();
