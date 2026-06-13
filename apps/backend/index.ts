import express from "express";
import type { Request, Response, NextFunction } from "express";
import { prisma } from "db";
import singUp from "./controller/signUp";
import signIn from "./controller/signIn";
import notFound from "./middleware/notFound";
import errorHandler, { AppError } from "./middleware/errorHandler";
import authMiddleWare from "./middleware/authMiddleware";
import { loopback } from "./loopback";
import onramp from "./controller/onramp";
import order from "./controller/order";
import getAvailableEquity from "./controller/getAvailableEquity";
import positionsOpenMarketId from "./controller/positionsOpenMarketId";
import positionsClosedMarketId from "./controller/positionsClosedMarketId";
import ordersOpenMarketId from "./controller/ordersOpenMarketId";
import ordersMarketId from "./controller/ordersMarketId";
import fills from "./controller/fills";
import deleteOrder from "./controller/deleteOrder";

const app = express();

app.use(express.json());

app.get("/health", (req, res) => {
  res.status(200).json({ success: true, message: "The server is up" });
});
app.post("/api/v1/signup", singUp);
app.post("/api/v1/signin", signIn);

app.post("/admin/market", async (req, res) => {
  const { symbol, imageUrl } = req.body;

  const token = req.headers.token;

  if (token != process.env.ADMIN_SECRET) {
    return res.status(403).json({
      message: "Invalid Token",
    });
  }

  const existingMarket = await prisma.market.findFirst({
    where: {
      slug: symbol,
    },
    select: {
      id: true,
    },
  });

  if (existingMarket) {
    return res.status(409).json({
      message: "Market already exists",
      id: existingMarket.id,
    });
  }

  const response = await prisma.market.create({
    data: {
      slug: symbol,
      imageUrl,
    },
  });

  const queueLoopbackResponse = await loopback({
    messageType: "create_market",
    marketId: response.id,
  });

  res.status(200).json({
    message: "Market Created Succesfully",
    id: response.id,
  });
});

app.post("/api/v1/onramp", authMiddleWare, onramp);
app.post("/api/v1/order", authMiddleWare, order);
app.delete("/order", authMiddleWare, deleteOrder);


app.get("/equity/available", authMiddleWare, getAvailableEquity);
app.get("/positions/open/:marketId", authMiddleWare, positionsOpenMarketId);
app.get("/positions/closed/:marketId", authMiddleWare, positionsClosedMarketId);
app.get("/orders/open/:marketId", authMiddleWare, ordersOpenMarketId);
app.get("/orders/:marketId", authMiddleWare, ordersMarketId);
app.get("/fills", authMiddleWare, fills);

app.use(notFound);
app.use(errorHandler);

app.listen(3000, () => {
  console.log("App is running on port 3000");
});
