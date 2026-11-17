import { z } from "zod";

/**
 * A thread file uploaded to `POST /threads/import`: a `.jsonl.zst` file written
 * by `GET /threads/:id/export` or saved by bb.
 */
export type ThreadImportForm = Record<"file", Blob>;

/**
 * A thread bb saved to a thread file and removed from the database. `saving`
 * and `deleting` mean the save is still running; `saved` means only the file
 * remains. Restoring it brings the thread back with its full history.
 */
export const savedThreadResponseSchema = z.object({
  threadId: z.string().min(1),
  projectId: z.string().min(1),
  title: z.string().nullable(),
  providerId: z.string().min(1),
  state: z.enum(["saving", "saved"]),
  threadCreatedAt: z.number().int(),
  archivedAt: z.number().int().nullable(),
  savedAt: z.number().int().nullable(),
  rows: z.number().int().nonnegative(),
  packedBytes: z.number().int().nonnegative(),
});
export type SavedThreadResponse = z.infer<typeof savedThreadResponseSchema>;

export const savedThreadListQuerySchema = z.object({
  projectId: z.string().min(1).optional(),
  search: z.string().min(1).optional(),
  limit: z.string().regex(/^\d+$/).optional(),
  offset: z.string().regex(/^\d+$/).optional(),
});
export type SavedThreadListQuery = z.infer<typeof savedThreadListQuerySchema>;

export const savedThreadListResponseSchema = z.object({
  savedThreads: z.array(savedThreadResponseSchema),
  hasMore: z.boolean(),
});
export type SavedThreadListResponse = z.infer<
  typeof savedThreadListResponseSchema
>;

/**
 * The result of importing a thread file. `rows` is the number of events in the
 * file; `insertedRows` is how many of them the thread did not already have.
 */
export const threadImportResponseSchema = z.object({
  threadId: z.string().min(1),
  rows: z.number().int().nonnegative(),
  insertedRows: z.number().int().nonnegative(),
});
export type ThreadImportResponse = z.infer<typeof threadImportResponseSchema>;
