import type { ToEngine } from "commons";
import { createClient } from "redis";

const BACKEND_STREAM = "to-backend-queue";
const BACKEND_CONSUMER_GROUP = "backend-queue-consumer-1";

const client = createClient();
client.connect();

client.on("error", () => {
  console.log("Client Connection Error");
});

const subscriber = createClient();
subscriber.connect();

subscriber.on("error", () => {
  console.log("Client Connection Error");
});

async function ensureConsumerGroup() {
  try {
    await subscriber.xGroupCreate(BACKEND_STREAM, BACKEND_CONSUMER_GROUP, "$", {
      MKSTREAM: true,
    });
  } catch (err: any) {
    if (!err.message.includes("BUSYGROUP")) {
      throw err;
    }
  }
}

await ensureConsumerGroup();

const loopbackResolves = new Map<string, (value: unknown) => void>();

export async function loopback(message: ToEngine) {
  return new Promise(async (resolve, reject) => {
    const loopBackId: string = crypto.randomUUID();
    const response = await client.xAdd("to-engine-queue", "*", {
      loopBackId,
      ...message,
    });

    loopbackResolves.set(loopBackId, resolve);

    setTimeout(() => {
      if (loopbackResolves.get(loopBackId)) {
        reject();
        loopbackResolves.delete(loopBackId);
      }
    }, 10000);
  });
}

async function main() {
  while (1) {
    const response = await subscriber.xReadGroup(
      BACKEND_CONSUMER_GROUP,
      BACKEND_CONSUMER_GROUP,
      [
        {
          key: BACKEND_STREAM,
          id: ">",
        },
      ],
      {
        BLOCK: 0,
        COUNT: 1,
      },
    );

    const message = response?.[0]?.messages?.[0]?.message as
      | Record<string, string>
      | undefined;

    if (!message) {
      continue;
    }

    const loopBackId = message.loopBackId;

    if (!loopBackId) {
      continue;
    }

    console.log(message);

    if (message.messageType === "onramp") {
      loopbackResolves.get(loopBackId)?.({
        messageType: "onramp",
        loopBackId,
        availableBalance: message.availableBalance,
        lockedBalance: message.lockedBalance,
      });
    }

    if (message.messageType === "create_order") {
      loopbackResolves.get(loopBackId)?.({
        loopBackId,
        messageType: "create_order",
        success: message.success === "true",
        order: message.order ? JSON.parse(message.order) : undefined,
        fills: message.fills ? JSON.parse(message.fills) : undefined,
        error: message.error,
      });
    }

    if (message.messageType === "get_equity") {
      loopbackResolves.get(loopBackId)?.({
        loopBackId,
        messageType: "get_equity",
        availableBalance: message.availableBalance,
        lockedBalance: message.lockedBalance,
      });
    }

    if (message.messageType === "get_positions") {
      loopbackResolves.get(loopBackId)?.({
        loopBackId,
        messageType: "get_positions",
        positions: message.positions ? JSON.parse(message.positions) : [],
      });
    }

    if (message.messageType === "create_market") {
      loopbackResolves.get(loopBackId)?.({
        loopBackId,
        messageType: "create_market",
        marketId: message.marketId,
        success: message.success === "true",
      });
    }

    loopbackResolves.delete(loopBackId);
  }
}

main();
