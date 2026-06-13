import { createClient } from "redis";
import { prisma } from "db";
import type { FillSnapshot, OrderSnapshot } from "commons";

const BACKEND_STREAM = "to-backend-queue";
const DATABASE_CONSUMER_GROUP = "database-queue-consumer-1";

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
    await subscriber.xGroupCreate(
      BACKEND_STREAM,
      DATABASE_CONSUMER_GROUP,
      "$",
      {
        MKSTREAM: true,
      },
    );
  } catch (err: any) {
    if (!err.message.includes("BUSYGROUP")) {
      throw err;
    }
  }
}

await ensureConsumerGroup();

async function main() {
  while (1) {
    const response = await subscriber.xReadGroup(
      DATABASE_CONSUMER_GROUP,
      DATABASE_CONSUMER_GROUP,
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

    if (!response?.length || !response[0]?.messages?.length) {
      continue;
    }

    const entry = response[0].messages[0];
    const message = entry?.message as Record<string, string> | undefined;
    const messageId = entry.id;

    if (!message) {
      // nothing to do
      continue;
    }

    try {
      const messageType = message.messageType;

      if (messageType === "create_order") {
        const success = message.success === "true";

        if (!success) {
          // nothing to persist when engine reports failure, just ack
          await subscriber.xAck(BACKEND_STREAM, DATABASE_CONSUMER_GROUP, messageId);
          continue;
        }

        const rawOrder = message.order;
        const rawFills = message.fills;

        let order: OrderSnapshot | undefined;
        let fills: FillSnapshot[] = [];

        if (rawOrder) {
          try {
            order = JSON.parse(rawOrder) as OrderSnapshot;
          } catch (err) {
            console.error("Failed to parse order payload", err);
          }
        }

        if (rawFills) {
          try {
            fills = JSON.parse(rawFills) as FillSnapshot[];
          } catch (err) {
            console.error("Failed to parse fills payload", err);
          }
        }

        if (order) {
          // Upsert the order into DB (create or update)
          const orderData = {
            id: order.id,
            userId: order.userId,
            market_id: order.market_id,
            orderType: order.orderType as any,
            side: order.side as any,
            price: order.price,
            qty: order.qty,
            initialMargin: order.initialMargin,
            filledQty: order.filledQty,
            status: order.status as any,
            createdAt: new Date(order.createdAt),
            updatedAt: new Date(order.updatedAt),
          };

          await prisma.order.upsert({
            where: { id: order.id },
            create: orderData,
            update: orderData,
          });
        }

        // Persist fills (if any)
        for (const f of fills) {
          try {
            const fillData = {
              id: f.id,
              maker_id: f.maker_id,
              taker_id: f.taker_id,
              qty: f.qty,
              pirce: f.pirce,
              maker_order_id: f.maker_order_id,
              taker_order_id: f.taker_order_id,
              market_id: f.market_id,
              createdAt: new Date(f.createdAt),
            };

            await prisma.fill.upsert({
              where: { id: f.id },
              create: fillData,
              update: fillData,
            });
          } catch (err) {
            console.error("Error persisting fill", err);
          }
        }

        // Acknowledge processed message
        await subscriber.xAck(BACKEND_STREAM, DATABASE_CONSUMER_GROUP, messageId);
        continue;
      }

      if (messageType === "cancel_order") {
        const success = message.success === "true";

        if (!success) {
          await subscriber.xAck(BACKEND_STREAM, DATABASE_CONSUMER_GROUP, messageId);
          continue;
        }

        const rawOrder = message.order;
        let order: OrderSnapshot | undefined;

        if (rawOrder) {
          try {
            order = JSON.parse(rawOrder) as OrderSnapshot;
          } catch (err) {
            console.error("Failed to parse cancelled order payload", err);
          }
        }

        if (order) {
          const orderData = {
            id: order.id,
            userId: order.userId,
            market_id: order.market_id,
            orderType: order.orderType as any,
            side: order.side as any,
            price: order.price,
            qty: order.qty,
            initialMargin: order.initialMargin,
            filledQty: order.filledQty,
            status: "Cancelled" as any,
            createdAt: new Date(order.createdAt),
            updatedAt: new Date(order.updatedAt),
          };

          await prisma.order.upsert({
            where: { id: order.id },
            create: orderData,
            update: {
              ...orderData,
              status: "Cancelled" as any,
            },
          });
        } else if (message.orderId) {
          await prisma.order.updateMany({
            where: { id: message.orderId, userId: message.userId },
            data: {
              status: "Cancelled" as any,
              updatedAt: new Date(),
            },
          });
        }

        await subscriber.xAck(BACKEND_STREAM, DATABASE_CONSUMER_GROUP, messageId);
        continue;
      }

      // For other message types we currently only ack and log
      if (messageType === "onramp" || messageType === "create_market") {
        console.log("Received engine message", messageType, message);
        await subscriber.xAck(BACKEND_STREAM, DATABASE_CONSUMER_GROUP, messageId);
        continue;
      }
    } catch (err) {
      console.error("Error processing engine message", err);
      // do not ack so it can be retried/inspected later
    }
  }
}

main();

