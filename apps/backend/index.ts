import express from "express";
import { prisma } from "db";
import singUp from "./controller/signUp";
import signIn from "./controller/signIn";
import notFound from "./middleware/notFound";
import errorHandler from "./middleware/errorHandler";
import authMiddleWare from "./middleware/authMiddleware";
import { loopback } from "./loopback";

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
    res.status(403).json({
      message: "Invalid Token",
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

app.post("/api/v1/onramp", authMiddleWare, async (req, res) => {
  const userId: string = req.userId!;
  console.log(userId);
  console.log(req.body);

  const queueLoopbackResponse = await loopback({
    messageType: "onramp",
    userId: userId,
    amount: req.body.amount.toString(),
  });

  console.log(queueLoopbackResponse);

  res.json({ sucess: true, message: queueLoopbackResponse });
});

app.post("api/v1/order", authMiddleWare, (req, res) => {});

app.use(notFound);
app.use(errorHandler);

app.listen(3000, () => {
  console.log("App is running on port 3000");
});
