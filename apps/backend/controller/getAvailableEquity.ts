import type { Request, Response, NextFunction } from "express";
import { AppError } from "../middleware/errorHandler";
import { loopback } from "../loopback";

const getAvailableEquity = async (
  req: Request & { userId?: string },
  res: Response,
  next: NextFunction,
) => {
  try {
    const userId = req.userId!;
    const response = await loopback({ messageType: "get_equity", userId });
    return res.status(200).json(response);
  } catch (err) {
    return next(new AppError("Failed to fetch equity", 500));
  }
};

export default getAvailableEquity;
