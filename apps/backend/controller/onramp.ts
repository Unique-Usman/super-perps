import type { NextFunction, Request, Response } from "express";
import { loopback } from "../loopback";

const onramp = async (req: Request, res: Response, next: NextFunction) => {
  const userId: string = req.userId!;

  const queueLoopbackResponse = await loopback({
    messageType: "onramp",
    userId: userId,
    amount: req.body.amount.toString(),
  });

  res.json({ sucess: true, message: queueLoopbackResponse });
};

export default onramp;
