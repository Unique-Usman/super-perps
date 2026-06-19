import type { NextFunction, Request, Response } from "express";
import { prisma } from "db";
import { loopback } from "../loopback";
import { AppError } from "../middleware/errorHandler";

const createMarket = async (
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  const { symbol, imageUrl } = req.body;
  const token = req.headers.token;

  if (token !== process.env.ADMIN_SECRET) {
    return next(new AppError("Invalid Token", 403));
  }

  if (!symbol || !imageUrl) {
    return next(new AppError("Invalid Request Body", 400));
  }

  try {
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

    const market = await prisma.market.create({
      data: {
        slug: symbol,
        imageUrl,
      },
    });

    await loopback({
      messageType: "create_market",
      marketId: market.id,
    });

    return res.status(200).json({
      message: "Market Created Successfully",
      id: market.id,
    });
  } catch (error) {
    return next(new AppError("Market creation failed", 500));
  }
};

export default createMarket;
