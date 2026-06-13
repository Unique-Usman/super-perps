import express from "express";
import { prisma } from "db";
import singUp from "./controller/signUp";
import signIn from "./controller/signIn";
import notFound from "./middleware/notFound";
import errorHandler, { AppError } from "./middleware/errorHandler";
import authMiddleWare from "./middleware/authMiddleware";
import { loopback } from "./loopback";
import onramp from "./controller/onramp";
import order from "./controller/order";

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

app.use(notFound);
app.use(errorHandler);

app.listen(3000, () => {
  console.log("App is running on port 3000");
});
