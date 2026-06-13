import type { Request, Response, NextFunction } from "express";
import { AppError } from "../middleware/errorHandler";
import { loopback } from "../loopback";

const positionsClosedMarketId = async (
  req: Request & { userId?: string },
  res: Response,
  next: NextFunction,
) => {
  try {
    const userId = req.userId!;
    const marketId = String(req.params.marketId);
    const response = await loopback({
      messageType: "get_positions",
      userId,
      marketId,
      status: "closed",
    });
    return res.status(200).json(response);
  } catch (err) {
    return next(new AppError("Failed to fetch positions", 500));
  }
};

export default positionsClosedMarketId;
