import type { Request, Response, NextFunction } from "express";
import { OrderSchema } from "commons";
import { AppError } from "../middleware/errorHandler";
import { loopback } from "../loopback";

const order = async (req: Request, res: Response, next: NextFunction) => {
  const result = OrderSchema.safeParse(req.body);

  if (!result.success) {
    return next(new AppError("Invalid Request Body", 400, result.error.issues));
  }

  const userId: string = req.userId!;
  const orderData = result.data;

  try {
    const response = await loopback({
      messageType: "create_order",
      price: orderData.price.toString(),
      qty: orderData.qty.toString(),
      side: orderData.type as "long" | "short",
      marketId: orderData.marketId,
      type: orderData.orderType as "market" | "limit",
      equity: orderData.equity.toString(),
      userId,
      orderId: crypto.randomUUID(),
    });

    res.status(201).json(response);
  } catch (error) {
    return next(new AppError("Order processing failed", 500));
  }
};

export default order;
