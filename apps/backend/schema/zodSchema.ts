import z from "zod";

export const UserSignUpInSchema = z.object({
  username: z.string(),
  password: z.string().min(6),
});
