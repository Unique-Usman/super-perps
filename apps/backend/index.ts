import express from "express";
import singUp from "./controller/signUp";
import signIn from "./controller/signIn";
import notFound from "./middleware/notFound";
import errorHandler from "./middleware/errorHandler";
import authMiddleWare from "./middleware/authMiddleware";
import onramp from "./controller/onramp";
import order from "./controller/order";
import getAvailableEquity from "./controller/getAvailableEquity";
import positionsOpenMarketId from "./controller/positionsOpenMarketId";
import positionsClosedMarketId from "./controller/positionsClosedMarketId";
import ordersOpenMarketId from "./controller/ordersOpenMarketId";
import ordersMarketId from "./controller/ordersMarketId";
import fills from "./controller/fills";
import deleteOrder from "./controller/deleteOrder";
import createMarket from "./controller/createMarket";

const app = express();
const API_PREFIX = "/api/v1";

app.use(express.json());

app.get("/health", (req, res) => {
  res.status(200).json({ success: true, message: "The server is up" });
});
app.post(`${API_PREFIX}/signup`, singUp);
app.post(`${API_PREFIX}/signin`, signIn);

app.post(`${API_PREFIX}/admin/market`, createMarket);

app.post(`${API_PREFIX}/onramp`, authMiddleWare, onramp);
app.post(`${API_PREFIX}/order`, authMiddleWare, order);
app.delete(`${API_PREFIX}/order`, authMiddleWare, deleteOrder);

app.get(`${API_PREFIX}/equity/available`, authMiddleWare, getAvailableEquity);
app.get(
  `${API_PREFIX}/positions/open/:marketId`,
  authMiddleWare,
  positionsOpenMarketId,
);
app.get(
  `${API_PREFIX}/positions/closed/:marketId`,
  authMiddleWare,
  positionsClosedMarketId,
);
app.get(
  `${API_PREFIX}/orders/open/:marketId`,
  authMiddleWare,
  ordersOpenMarketId,
);
app.get(`${API_PREFIX}/orders/:marketId`, authMiddleWare, ordersMarketId);
app.get(`${API_PREFIX}/fills`, authMiddleWare, fills);

app.use(notFound);
app.use(errorHandler);

app.listen(3000, () => {
  console.log("App is running on port 3000");
});
