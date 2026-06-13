import type { NextFunction, Request, Response } from "express";
import { AppError } from "../middleware/errorHandler";
import { loopback } from "../loopback";

const deleteOrder = async (req: Request, res: Response, next: NextFunction) => {
  const userId = req.userId!;
  const orderId = req.body.orderId;

  if (!orderId) {
    return next(new AppError("Invalid Request Body", 400, "orderId is not presents"));
  }

  try {
    const response = await loopback({
      messageType: "cancel_order",
      orderId: String(orderId),
      userId,
    });

    return res.status(200).json(response);
  } catch (error) {
    return next(new AppError("Order cancellation failed", 500));
  }
};

export default deleteOrder;
