import { createClient } from "redis";
import type { ToEngine } from "commons";

const client = createClient();
client.connect();
const STREAM = "to-engine-queue";
const CONSUMER_GROUP = "engine-queue-consumer";

const publisher = createClient();
publisher.connect();

client.on("error", () => {
  console.log("Client Connection Error");
});

type OpenOrder = {
  userId: string;
  originalOrderId: string;
  qty: string;
  filledQty: string;
};

type Bid = {
  availableQty: number;
  openOrders: OpenOrder[];
};

type Ask = {
  availableQty: number;
  openOrders: OpenOrder[];
};

interface OrderBook {
  bids: Map<string, Bid>;
  asks: Map<string, Ask>;
  lastTradedPrice: number;
  marketId: string;
}
const orderBooks: OrderBook[] = [];

const balances: Map<string, { available: string; locked: string }> = new Map();

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

async function matching() {
  while (1) {
    const response = await client.xReadGroup(
      "engine-queue-consumer",
      "engine-1",
      [
        {
          key: "to-engine-queue",
          id: ">",
        },
      ],
      {
        BLOCK: 0,
        COUNT: 1,
      },
    );

    if (!response) {
      console.log("Noting found");
      continue;
    }

    const message: {
      loopBackId: string;
    } & ToEngine = response[0].messages[0].message;

    if (message.messageType === "create_market") {
      orderBooks.push({
        bids: new Map(),
        asks: new Map(),
        lastTradedPrice: -1,
        marketId: message.marketId,
      });

      console.log(orderBooks);

      await publisher.xAdd("to-backend-queue", "*", {
        loopBackId: message.loopBackId,
      });
    }

    if (message.messageType === "onramp") {
      const userBalance = balances.get(message.userId) ?? {
        available: "0",
        locked: "0",
      };

      balances.set(message.userId, {
        ...userBalance,
        available: String(
          Number(userBalance.available) + Number(message.amount),
        ),
      });

      console.log(balances);

      await publisher.xAdd("to-backend-queue", "*", {
        loopBackId: message.loopBackId,
      });
    }

    if (message.messageType === "create_order") {
    }

    if (message.messageType === "cancel_order") {
    }

    console.log(message);
  }
}

function liquidationCheck() {
  // const wss = new WebSocket(
  //     "wss://fstream.binance.com/market/stream?streams=btcusdt@markPrice/ethusdt@markPrice/solusdt@markPrice",
  //   );
  //
  //   let binancePrice: BinancePrice = {
  //     "solusdt@markPrice": "",
  //     "btcusdt@markPrice": "",
  //     "ethusdt@markPrice": "",
  //   };
  //
  //   wss.on("open", () => {
  //     console.log("Hello world from binance");
  //   });
  //
  //   wss.onmessage = async (event) => {
  //     const message = BinancePriceSchema.safeParse(
  //       JSON.parse(event.data.toString()),
  //     );
  //
  //     if (message.success) {
  //       const data = message.data;
  //       if (
  //         binancePrice[data.stream] === "" ||
  //         binancePrice[data.stream] !== data.data.p
  //       ) {
  //         binancePrice[data.stream] = data.data.p;
  //         let sym: "BTC" | "SOL" | "ETH" = data.stream
  //           .slice(0, 3)
  //           .toUpperCase() as "SOL" | "ETH" | "BTC";
  //         globalState.orderBooks[sym].markPrice = Number(data.data.p);
  //         globalState.orderBooks[sym].indexPrice = Number(data.data.i);
  //         await liquidationChecks(sym, Number(data.data.p));
  //       }
  //     }
  //   };
  //
  //   wss.on("close", () => {
  //     setTimeout(() => {
  //       onPriceUpdateFromBinance();
  //     }, 5000);
  //   });
  //
  //   wss.on("error", (e) => {
  //     console.log(e);
  //   });
  //
  //   wss.on("ping", () => {
  //     wss.pong();
  //   });
}

liquidationCheck();

matching();
