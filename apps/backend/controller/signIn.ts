import type { Request, Response, NextFunction } from "express";
import { UserSignUpInSchema } from "../schema/zodSchema";
import { prisma } from "db";
import bcrypt from "bcrypt";
import { AppError } from "../middleware/errorHandler";
import jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET;

const signIn = async (req: Request, res: Response, next: NextFunction) => {
  const result = UserSignUpInSchema.safeParse(req.body);

  if (!result.success) {
    return next(new AppError("Invalid Credentials", 401));
  }

  const username = result.data.username;
  const password = result.data.password;

  const foundUser = await prisma.user.findUnique({
    where: {
      username: username,
    },
    select: {
      username: true,
      password: true,
      id: true,
    },
  });

  if (!foundUser) {
    return next(new AppError("Invalid Credentials", 401));
  }

  const isCorrectPassword = await bcrypt.compare(password, foundUser.password);

  if (!isCorrectPassword) {
    return next(new AppError("Invalid Credentials", 401));
  }

  if (!JWT_SECRET) {
    throw new Error("JWT_SECRET is missing");
  }

  const token = jwt.sign({ userId: foundUser.id }, JWT_SECRET, {
    expiresIn: "1d",
  });

  res.status(200).json({
    success: true,
    token,
  });
};

export default signIn;
