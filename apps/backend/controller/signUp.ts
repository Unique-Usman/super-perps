import type { Request, Response, NextFunction } from "express";
import { UserSignUpInSchema } from "../schema/zodSchema";
import { prisma } from "db";
import bcrypt from "bcrypt";
import { AppError } from "../middleware/errorHandler";

const singUp = async (req: Request, res: Response, next: NextFunction) => {
  const result = UserSignUpInSchema.safeParse(req.body);

  if (!result.success) {
    return next(new AppError("Invalid Request Body", 400, result.error.issues));
  }

  const username = result.data.username;
  const password = result.data.password;

  const foundUser = await prisma.user.findUnique({
    where: {
      username: username,
    },
    select: {
      username: true,
    },
  });

  if (foundUser) {
    return next(new AppError("User already exists", 409));
  }

  const hashedPassword = await bcrypt.hash(password, 10);

  const user = await prisma.user.create({
    data: {
      username: username,
      password: hashedPassword,
    },
  });

  res.status(201).json({
    success: true,
    user: {
      id: user.id,
      username: user.username,
    },
  });
};

export default singUp;
