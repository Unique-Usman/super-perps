import type { Request, Response, NextFunction } from "express";
import { prisma } from "db";
import { AppError } from "../middleware/errorHandler";

const ordersMarketId = async (
  req: Request & { userId?: string },
  res: Response,
  next: NextFunction,
) => {
  try {
    const userId = req.userId!;
    const marketId = String(req.params.marketId);
    const orders = await prisma.order.findMany({
      where: { market_id: marketId, userId },
      orderBy: { createdAt: "desc" },
    });
    return res.status(200).json({ orders });
  } catch (err) {
    return next(new AppError("Failed to fetch orders", 500));
  }
};

export default ordersMarketId;
