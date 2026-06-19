import { prisma } from "db";
import { createClient } from "redis";
import WebSocket from "ws";
import { z } from "zod";


const ENGINE_STREAM = "to-engine-queue";
const BINANCE_WS_BASE = "wss://fstream.binance.com/stream?streams=";

const publisher = createClient();
publisher.on("error", () => {
  console.log("Redis Publisher Connection Error");
});
await publisher.connect();

const BinanceMarkPriceSchema = z.object({
  stream: z.string(),
  data: z.object({
    // e: z.string(),
    s: z.string(),
    p: z.string(),
    i: z.string(),
  }),
});

type MarketRow = { id: string; slug: string };

function streamNameForSlug(slug: string): string | null {
  const normalized = slug.trim().toLowerCase();
  if (!normalized) {
    return null;
  }
  return `${normalized}usdt@markprice`;
}

async function loadMarketStreams() {
  const markets: MarketRow[] = await prisma.market.findMany({
    select: { id: true, slug: true },
  });

  const streamToMarketId = new Map<string, string>();

  for (const market of markets) {
    const stream = streamNameForSlug(market.slug);
    if (!stream) {
      console.log(`Skipping market ${market.slug}: no Binance mapping`);
      continue;
    }
    streamToMarketId.set(stream, market.id);
  }

  return streamToMarketId;
}

async function publishMarkPrice(
  marketId: string,
  markPrice: string,
  indexPrice: string,
) {
  await publisher.xAdd(
    ENGINE_STREAM,
    "*",
    {
      loopBackId: crypto.randomUUID(),
      messageType: "mark_price",
      marketId,
      markPrice,
      indexPrice,
    },
    {
      TRIM: {
        strategy: "MAXLEN",
        strategyModifier: "~",
        threshold: 100000,
      },
    },
  );
}

async function connect() {
  const streamToMarketId = await loadMarketStreams();

  if (streamToMarketId.size === 0) {
    console.log("No mappable markets found, retrying in 5s");
    setTimeout(connect, 5000);
    return;
  }

  const streams = Array.from(streamToMarketId.keys());
  const url = `${BINANCE_WS_BASE}${streams.join("/")}`;
  const ws = new WebSocket(url);

  const lastPrice = new Map<string, string>();

  ws.on("open", () => {
    console.log(`Subscribed to ${streams.length} Binance mark price stream(s)`);
  });

  ws.on("message", async (raw) => {
    const parsed = BinanceMarkPriceSchema.safeParse(
      JSON.parse(raw.toString()),
    );
    if (!parsed.success) {
      return;
    }

    const { stream, data } = parsed.data;
    const marketId = streamToMarketId.get(stream);
    if (!marketId) {
      return;
    }

    if (lastPrice.get(stream) === data.p) {
      return;
    }
    lastPrice.set(stream, data.p);

    try {
      await publishMarkPrice(marketId, data.p, data.i);
    } catch (err) {
      console.error("Failed to publish mark price", err);
    }
  });

  ws.on("close", () => {
    console.log("Binance socket closed, reconnecting in 5s");
    setTimeout(connect, 5000);
  });

  ws.on("error", (err) => {
    console.error("Binance socket error", err);
  });

  ws.on("ping", () => {
    ws.pong();
  });
}

connect();