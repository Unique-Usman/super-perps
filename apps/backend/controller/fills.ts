import type { Request, Response, NextFunction } from "express";
import { prisma } from "db";
import { AppError } from "../middleware/errorHandler";

const fills = async (
  req: Request & { userId?: string },
  res: Response,
  next: NextFunction,
) => {
  try {
    const userId = req.userId!;
    const fills = await prisma.fill.findMany({
      where: { OR: [{ maker_id: userId }, { taker_id: userId }] },
      orderBy: { createdAt: "desc" },
    });
    return res.status(200).json({ fills });
  } catch (err) {
    return next(new AppError("Failed to fetch fills", 500));
  }
};

export default fills;
