import { createClient } from "redis";
import type {
  FillSnapshot,
  OrderSnapshot,
  ToEngine,
} from "commons";

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

type PriceLevel = {
  availableQty: number;
  openOrders: OpenOrder[];
};

type OrderBook = {
  marketId: string;
  bids: Map<string, PriceLevel>;
  asks: Map<string, PriceLevel>;
  lastTradedPrice: number;
};

const users = new Map<string, EngineUser>();
const orderBooks = new Map<string, OrderBook>();
const orderIndex = new Map<string, { userId: string; order: OrderSnapshot }>();

function now() {
  return new Date().toISOString();
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
  const targetBook = params.order.side === "Bid" ? params.market.bids : params.market.asks;
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

async function processCreateMarket(message: Extract<ToEngine, { messageType: "create_market" }> & { loopBackId: string }) {
  ensureMarket(message.marketId);

  await publisher.xAdd(BACKEND_STREAM, "*", {
    loopBackId: message.loopBackId,
    messageType: "create_market",
    marketId: message.marketId,
    success: "true",
  });
}

async function processOnramp(message: Extract<ToEngine, { messageType: "onramp" }> & { loopBackId: string }) {
  const user = ensureUser(message.userId);
  const nextAvailable = Number(user.balance.available) + Number(message.amount);

  user.balance.available = String(nextAvailable);

  await publisher.xAdd(BACKEND_STREAM, "*", {
    loopBackId: message.loopBackId,
    messageType: "onramp",
    availableBalance: user.balance.available,
    lockedBalance: user.balance.locked,
  });
}

async function processCreateOrder(message: Extract<ToEngine, { messageType: "create_order" }> & { loopBackId: string }) {
  const user = ensureUser(message.userId);
  const market = ensureMarket(message.marketId);

  const equityRequired = Number(message.equity);
  const availableBalance = Number(user.balance.available);

  if (availableBalance < equityRequired) {
    await publisher.xAdd(BACKEND_STREAM, "*", {
      loopBackId: message.loopBackId,
      messageType: "create_order",
      success: "false",
      error: "Insufficient funds",
      fills: JSON.stringify([]),
    });
    return;
  }

  user.balance.available = String(availableBalance - equityRequired);
  user.balance.locked = String(Number(user.balance.locked) + equityRequired);

  const side = toSide(message.side);
  const orderType = toOrderType(message.type);
  const requestedQty = Number(message.qty);
  const requestedPrice = Number(message.price);
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

  const oppositeBook = side === "Bid" ? market.asks : market.bids;
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
      (side === "Bid" ? bestPrice <= requestedPrice : bestPrice >= requestedPrice);

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

      if (makerOrderEntry) {
        makerOrderEntry.order.filledQty = String(Number(makerOrderEntry.order.filledQty) + fillQty);
        makerOrderEntry.order.updatedAt = now();

        if (Number(makerOrderEntry.order.filledQty) === Number(makerOrderEntry.order.qty)) {
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

  if (Number(takerOrder.filledQty) > 0 && takerOrder.status !== "Cancelled") {
    takerOrder.status = Number(takerOrder.filledQty) === requestedQty ? "Filled" : "PartiallyFilled";
  }

  await publisher.xAdd(BACKEND_STREAM, "*", {
    loopBackId: message.loopBackId,
    messageType: "create_order",
    success: "true",
    order: JSON.stringify(takerOrder),
    fills: JSON.stringify(fills),
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

async function main() {
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

    const rawMessage = response[0].messages[0]?.message as
      | Record<string, string>
      | undefined;

    if (!rawMessage?.messageType || !rawMessage.loopBackId) {
      continue;
    }

    const message = rawMessage as { loopBackId: string } & ToEngine;

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
      await publisher.xAdd(BACKEND_STREAM, "*", {
        loopBackId: message.loopBackId,
        messageType: "cancel_order",
        orderId: message.orderId,
        userId: message.userId,
      });
    }
  }
}

main();

function liquidationCheck() {}
liquidationCheck();