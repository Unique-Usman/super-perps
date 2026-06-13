import z from "zod";

export const OrderSchema = z.object({
  price: z.number(),
  qty: z.number(),
  equity: z.number(),
  type: z.enum(["long", "short"]),
  marketId: z.string(),
  orderType: z.enum(["market", "limit"]),
  userId: z.string().optional(),
});
