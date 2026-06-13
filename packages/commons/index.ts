export { OrderSchema } from "./zodSchema.ts";

export type ToEngine =
  | {
      messageType: "onramp";
      userId: string;
      amount: string;
    }
  | {
      messageType: "get_equity";
      userId: string;
    }
  | {
      messageType: "get_positions";
      userId: string;
      marketId?: string;
      status?: "open" | "closed";
    }
  | {
      messageType: "create_order";
      price: string;
      qty: string;
      side: "short" | "long";
      marketId: string;
      type: "limit" | "market";
      equity: string;
      userId: string;
      orderId: string;
    }
  | {
      messageType: "cancel_order";
      orderId: string;
      userId: string;
    }
  | {
      messageType: "create_market";
      marketId: string;
    };

export type EngineOrderStatus = "Open" | "PartiallyFilled" | "Cancelled" | "Filled";

export type EngineSide = "Bid" | "Ask";

export type EngineOrderType = "Market" | "Limit";

export type OrderSnapshot = {
  id: string;
  userId: string;
  market_id: string;
  orderType: EngineOrderType;
  side: EngineSide;
  price: string;
  qty: string;
  initialMargin: string;
  filledQty: string;
  status: EngineOrderStatus;
  createdAt: string;
  updatedAt: string;
};

export type FillSnapshot = {
  id: string;
  maker_id: string;
  taker_id: string;
  qty: string;
  pirce: string;
  maker_order_id: string;
  taker_order_id: string;
  market_id: string;
  createdAt: string;
};

export type PositionSnapshot = {
  id: string;
  userId: string;
  market_id: string;
  qty: string;
  avgEntryPrice: string;
  status: "open" | "closed";
  createdAt: string;
  updatedAt: string;
};

export type UserSnapshot = {
  id: string;
  username?: string;
  password?: string;
  availableBalance: string;
  lockedBalance: string;
};

export type FromEngine =
  | {
      loopBackId: string;
      messageType: "onramp";
      availableBalance: string;
      lockedBalance: string;
    }
  | {
      loopBackId: string;
      messageType: "get_equity";
      availableBalance: string;
      lockedBalance: string;
    }
  | {
      loopBackId: string;
      messageType: "get_positions";
      positions: PositionSnapshot[];
    }
  | {
      loopBackId: string;
      messageType: "create_order";
      success: boolean;
      order?: OrderSnapshot;
      fills?: FillSnapshot[];
      error?: string;
    }
  | {
      loopBackId: string;
      messageType: "cancel_order";
      orderId: string;
      userId: string;
    }
  | {
      loopBackId: string;
      messageType: "create_market";
      marketId: string;
      success?: boolean;
    };
