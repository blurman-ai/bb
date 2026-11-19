import {
  acquireProjectAttachmentOwnership,
  cancelThreadPurge,
  deleteSavedThread,
  hideThreadForSaving,
  requestThreadPurge,
  restartThreadArchiveClock,
  unhideSavedThread,
  getProject,
  getSavedThread,
  getThread,
  insertHiddenThreadRow,
  insertThreadTableRow,
  listThreadAttachments,
  readThreadEventWindow,
  readThreadTableRowsPage,
  THREAD_FILE_FORMAT_VERSION,
  THREAD_FILE_TABLES,
  updateSavedThreadInState,
  upsertSavedThread,
  type DbConnection,
  type SavedThreadAttachment,
  type ThreadFileTableRow,
} from "@bb/db";
import { ApiError } from "../../../errors.js";
import {
  readAttachment,
  restoreProjectAttachment,
} from "../../projects/attachments.js";
import {
  THREAD_FILE_FRAME_RAW_BYTES,
  ThreadFileError,
  ThreadFileWriter,
  openThreadFile,
  readThreadFileFrame,
  removeSavedThreadFile,
  savedThreadFileExists,
  savedThreadFilePath,
  type OpenThreadFile,
  type ThreadFileHeader,
} from "./thread-file.js";
import {
  holdSavedThread,
  insertInSlices,
  insertThreadFileEvents,
  yieldToEventLoop,
  type SavedThreadDeps,
} from "./saved-thread-files.js";

const THREAD_FILE_WINDOW_ROWS = 128;
const THREAD_FILE_WINDOW_BYTES = 128 * 1024;
const ATTACHMENT_PART_BYTES = 512 * 1024;
const TABLE_ROWS_PER_STEP = 256;

export interface ThreadFileAttachmentContent {
  content: Buffer;
  mimeType: string | null;
  originalName: string;
  storedPath: string;
}

interface AttachmentInProgress {
  content: Buffer;
  nextIndex: number;
  parts: number;
  storedPath: string;
}

export interface ThreadFileSummary {
  packedBytes: number;
  rawBytes: number;
  rows: number;
}

export interface ThreadFileImportResult {
  insertedRows: number;
  rows: number;
  threadId: string;
}

function threadFileHeader(
  db: DbConnection,
  threadId: string,
  exportedAt: number,
): ThreadFileHeader | null {
  const thread = getThread(db, threadId);
  if (thread === null) return null;
  return {
    archivedAt: thread.archivedAt,
    createdAt: thread.createdAt,
    exportedAt,
    projectId: thread.projectId,
    projectName: getProject(db, thread.projectId)?.name ?? null,
    providerId: thread.providerId,
    threadId,
    title: thread.title ?? thread.titleFallback,
  };
}

type ExportPhase = "tables" | "events" | "attachments" | "done";

export class ThreadFileExport {
  private phase: ExportPhase = "tables";
  private tableIndex = 0;
  private tableAfterRowid = 0;
  private afterSequence = 0;
  private attachments: SavedThreadAttachment[] = [];
  private attachment: AttachmentInProgress | null = null;

  private constructor(
    private readonly deps: Pick<SavedThreadDeps, "config" | "db">,
    private readonly writer: ThreadFileWriter,
    readonly threadId: string,
    private readonly projectId: string,
    private readonly throughSequence: number | null,
  ) {}

  static async start(
    deps: Pick<SavedThreadDeps, "config" | "db">,
    args: { path: string; threadId: string; throughSequence: number | null },
  ): Promise<ThreadFileExport | null> {
    const header = threadFileHeader(deps.db, args.threadId, Date.now());
    if (header === null) return null;
    const writer = await ThreadFileWriter.create({ header, path: args.path });
    return new ThreadFileExport(
      deps,
      writer,
      args.threadId,
      header.projectId,
      args.throughSequence,
    );
  }

  private async flushIfFull(): Promise<void> {
    if (this.writer.pendingRawBytes >= THREAD_FILE_FRAME_RAW_BYTES) {
      await this.writer.flushFrameInBackground();
    }
  }

  async step(): Promise<boolean> {
    switch (this.phase) {
      case "tables": {
        const table = THREAD_FILE_TABLES[this.tableIndex];
        if (table === undefined) {
          this.attachments = listThreadAttachments(this.deps.db, this.threadId);
          this.phase = "events";
          return true;
        }
        const page = readThreadTableRowsPage(this.deps.db, {
          afterRowid: this.tableAfterRowid,
          limit: TABLE_ROWS_PER_STEP,
          table,
          threadId: this.threadId,
        });
        this.writer.addTableRows(page.rows);
        if (page.lastRowid === null || page.rows.length < TABLE_ROWS_PER_STEP) {
          this.tableIndex += 1;
          this.tableAfterRowid = 0;
        } else {
          this.tableAfterRowid = page.lastRowid;
        }
        await this.flushIfFull();
        return true;
      }
      case "events": {
        const window = readThreadEventWindow(this.deps.db, {
          afterSequence: this.afterSequence,
          maxBytes: THREAD_FILE_WINDOW_BYTES,
          maxRows: THREAD_FILE_WINDOW_ROWS,
          threadId: this.threadId,
          throughSequence: this.throughSequence,
        });
        if (window.lastSequence === null) {
          this.phase = "attachments";
          return true;
        }
        this.afterSequence = window.lastSequence;
        this.writer.addWindow(window.rows, window.retained);
        await this.flushIfFull();
        return true;
      }
      case "attachments": {
        if (this.attachment !== null) {
          const current = this.attachment;
          const start = current.nextIndex * ATTACHMENT_PART_BYTES;
          this.writer.addAttachmentPart({
            data: current.content.subarray(
              start,
              start + ATTACHMENT_PART_BYTES,
            ),
            index: current.nextIndex,
            storedPath: current.storedPath,
          });
          current.nextIndex += 1;
          if (current.nextIndex >= current.parts) this.attachment = null;
          await this.flushIfFull();
          return true;
        }
        const attachment = this.attachments.shift();
        if (attachment === undefined) {
          this.phase = "done";
          return false;
        }
        const content = await readAttachment(
          this.deps.config.dataDir,
          this.projectId,
          attachment.storedPath,
        ).catch((error: unknown) => {
          if (error instanceof ApiError && error.status === 404) return null;
          throw error;
        });
        if (content === null) return true;
        const parts = Math.ceil(content.content.length / ATTACHMENT_PART_BYTES);
        this.writer.addAttachment({
          mimeType: attachment.mimeType,
          originalName: attachment.originalName,
          parts,
          sizeBytes: content.content.length,
          storedPath: attachment.storedPath,
        });
        if (parts > 0) {
          this.attachment = {
            content: content.content,
            nextIndex: 0,
            parts,
            storedPath: attachment.storedPath,
          };
        }
        return true;
      }
      case "done":
        return false;
    }
  }

  seal(): Promise<ThreadFileSummary> {
    return this.writer.seal();
  }

  abort(): Promise<void> {
    return this.writer.abort();
  }
}

export async function exportThreadFile(
  deps: SavedThreadDeps,
  args: { path: string; threadId: string },
): Promise<ThreadFileSummary> {
  const exporter = await ThreadFileExport.start(deps, {
    path: args.path,
    threadId: args.threadId,
    throughSequence: null,
  });
  if (exporter === null) {
    throw new ApiError(404, "thread_not_found", "Thread not found");
  }
  try {
    while (await exporter.step()) await yieldToEventLoop();
    return await exporter.seal();
  } catch (error) {
    await exporter.abort();
    throw error;
  }
}

async function openForImport(path: string): Promise<OpenThreadFile> {
  try {
    const file = await openThreadFile(path);
    try {
      for (const entry of file.table.frames) {
        await readThreadFileFrame(file, entry);
        await yieldToEventLoop();
      }
    } catch (error) {
      await file.close();
      throw error;
    }
    return file;
  } catch (error) {
    if (error instanceof ThreadFileError) {
      throw new ApiError(400, "invalid_thread_file", error.message);
    }
    throw error;
  }
}

async function readTablesAndAttachments(file: OpenThreadFile): Promise<{
  attachments: ThreadFileAttachmentContent[];
  tables: ThreadFileTableRow[];
}> {
  const tables: ThreadFileTableRow[] = [];
  const headers: Array<
    Awaited<ReturnType<typeof readThreadFileFrame>>["attachments"][number]
  > = [];
  const parts = new Map<string, Buffer[]>();
  for (const entry of file.table.frames) {
    const frame = await readThreadFileFrame(file, entry);
    tables.push(...frame.tables);
    headers.push(...frame.attachments);
    for (const part of frame.attachmentParts) {
      const list = parts.get(part.storedPath) ?? [];
      list[part.index] = part.data;
      parts.set(part.storedPath, list);
    }
  }
  const attachments = headers.map((header) => {
    const list = parts.get(header.storedPath) ?? [];
    if (
      list.length !== header.parts ||
      list.some((part) => part === undefined)
    ) {
      throw new ThreadFileError(
        `Thread file is missing parts of ${header.storedPath}`,
      );
    }
    const content = Buffer.concat(list);
    if (content.length !== header.sizeBytes) {
      throw new ThreadFileError(
        `Thread file has a damaged copy of ${header.storedPath}`,
      );
    }
    return {
      content,
      mimeType: header.mimeType,
      originalName: header.originalName,
      storedPath: header.storedPath,
    };
  });
  return { attachments, tables };
}

const THREAD_LINK_COLUMNS = [
  "parent_thread_id",
  "source_thread_id",
  "lifecycle_owner_thread_id",
] as const;

async function restoreLinkedSavedThreads(
  deps: SavedThreadDeps,
  threadRow: ThreadFileTableRow,
  visited: Set<string>,
): Promise<void> {
  for (const column of THREAD_LINK_COLUMNS) {
    const linked = threadRow.values[column];
    if (typeof linked !== "string" || getThread(deps.db, linked) !== null)
      continue;
    const saved = getSavedThread(deps.db, linked);
    if (saved?.state === "saved" && !visited.has(linked)) {
      await restoreSavedThreadWith(deps, linked, visited);
    }
  }
}

function requireImportProject(
  deps: SavedThreadDeps,
  header: ThreadFileHeader,
): void {
  const project = getProject(deps.db, header.projectId);
  if (project === null || project.deletedAt !== null) {
    throw new ApiError(
      404,
      "project_not_found",
      `Thread ${header.threadId} belongs to a project that isn't on this server. Importing a thread from another server isn't supported yet.`,
    );
  }
}

async function insertThreadFileRows(
  deps: SavedThreadDeps,
  file: OpenThreadFile,
  args: { recreate: boolean; visited: Set<string> },
): Promise<number> {
  const { threadId, projectId } = file.header;
  requireImportProject(deps, file.header);
  const { attachments, tables } = await readTablesAndAttachments(file);
  const threadRow = tables.find((row) => row.table === "threads");
  if (threadRow === undefined) {
    throw new ApiError(
      400,
      "invalid_thread_file",
      "Thread file has no thread row",
    );
  }
  if (args.recreate) {
    await restoreLinkedSavedThreads(deps, threadRow, args.visited);
    insertHiddenThreadRow(deps.db, threadRow, Date.now());
  }
  await insertInSlices(
    deps,
    tables.filter((row) => row.table !== "threads"),
    (tx, row) => (insertThreadTableRow(tx, row) ? 1 : 0),
  );
  for (const attachment of attachments) {
    await restoreProjectAttachment(
      deps.db,
      deps.config.dataDir,
      projectId,
      attachment,
    );
  }
  acquireProjectAttachmentOwnership(
    deps.db,
    threadId,
    attachments.map((attachment) => attachment.storedPath),
    "best-effort",
  );
  return insertThreadFileEvents(deps, file);
}

function removeThreadAgain(deps: SavedThreadDeps, threadId: string): void {
  deps.db.transaction(
    (tx) => {
      if (getThread(tx, threadId) === null) return;
      const now = Date.now();
      hideThreadForSaving(tx, { now, threadId });
      requestThreadPurge(tx, { now, threadId });
    },
    { behavior: "immediate" },
  );
}

function asImportError(error: unknown): unknown {
  return error instanceof ThreadFileError
    ? new ApiError(400, "invalid_thread_file", error.message)
    : error;
}

function notifyThreadReturned(
  deps: SavedThreadDeps,
  header: ThreadFileHeader,
  change: "thread-created" | "history-rewritten",
): void {
  deps.hub.notifyThread(header.threadId, [change], {
    projectId: header.projectId,
  });
  deps.hub.notifyProject(header.projectId, ["threads-changed"]);
}

async function restoreFromSavedFile(
  deps: SavedThreadDeps,
  file: OpenThreadFile,
  visited: Set<string>,
): Promise<ThreadFileImportResult> {
  const { threadId } = file.header;
  visited.add(threadId);
  const recreate = deps.db.transaction(
    (tx) => {
      if (
        !updateSavedThreadInState(tx, {
          expectedState: "saved",
          patch: { state: "restoring", updatedAt: Date.now() },
          threadId,
        })
      ) {
        throw new ApiError(
          404,
          "saved_thread_not_found",
          "Saved thread not found",
        );
      }
      cancelThreadPurge(tx, threadId);
      return getThread(tx, threadId) === null;
    },
    { behavior: "immediate" },
  );
  let insertedRows: number;
  try {
    insertedRows = await insertThreadFileRows(deps, file, {
      recreate,
      visited,
    });
  } catch (error) {
    removeThreadAgain(deps, threadId);
    updateSavedThreadInState(deps.db, {
      expectedState: "restoring",
      patch: { state: "saved", updatedAt: Date.now() },
      threadId,
    });
    throw asImportError(error);
  }
  deps.db.transaction(
    (tx) => {
      const now = Date.now();
      unhideSavedThread(tx, threadId);
      updateSavedThreadInState(tx, {
        expectedState: "restoring",
        patch: { restoredAt: now, state: "restored", updatedAt: now },
        threadId,
      });
      restartThreadArchiveClock(tx, { now, threadId });
    },
    { behavior: "immediate" },
  );
  await removeSavedThreadFile(deps.config.dataDir, threadId);
  notifyThreadReturned(
    deps,
    file.header,
    recreate ? "thread-created" : "history-rewritten",
  );
  deps.logger.info(
    { threadId, rows: file.table.rows, insertedRows },
    "Saved thread restored",
  );
  return { insertedRows, rows: file.table.rows, threadId };
}

async function recreateFromImportedFile(
  deps: SavedThreadDeps,
  file: OpenThreadFile,
): Promise<number> {
  const { header } = file;
  requireImportProject(deps, header);
  const now = Date.now();
  upsertSavedThread(deps.db, {
    archivedAt: header.archivedAt,
    formatVersion: THREAD_FILE_FORMAT_VERSION,
    packedBytes: 0,
    projectId: header.projectId,
    providerId: header.providerId,
    rawBytes: 0,
    restoredAt: null,
    rowCount: 0,
    savedAt: null,
    state: "restoring",
    threadCreatedAt: header.createdAt,
    threadId: header.threadId,
    throughSequence: 0,
    title: header.title,
    updatedAt: now,
  });
  let insertedRows: number;
  try {
    insertedRows = await insertThreadFileRows(deps, file, {
      recreate: true,
      visited: new Set([header.threadId]),
    });
  } catch (error) {
    removeThreadAgain(deps, header.threadId);
    deleteSavedThread(deps.db, header.threadId);
    throw asImportError(error);
  }
  deps.db.transaction(
    (tx) => {
      unhideSavedThread(tx, header.threadId);
      deleteSavedThread(tx, header.threadId);
    },
    { behavior: "immediate" },
  );
  return insertedRows;
}

async function importIntoThread(
  deps: SavedThreadDeps,
  file: OpenThreadFile,
): Promise<ThreadFileImportResult> {
  const { threadId } = file.header;
  const saved = getSavedThread(deps.db, threadId);
  if (saved?.state === "saved" || saved?.state === "restoring") {
    throw new ApiError(
      409,
      "saved_thread_busy",
      `Thread ${threadId} was saved to a file while this import started; try again`,
    );
  }
  if (saved?.state === "saving") deleteSavedThread(deps.db, threadId);
  const thread = getThread(deps.db, threadId);
  if (thread !== null && thread.deletedAt !== null) {
    throw new ApiError(
      409,
      "thread_being_deleted",
      `Thread ${threadId} is still being deleted. Import the file again once the deletion finishes.`,
    );
  }
  const recreated = thread === null;
  let insertedRows: number;
  if (recreated) {
    insertedRows = await recreateFromImportedFile(deps, file);
  } else {
    try {
      insertedRows = await insertThreadFileEvents(deps, file);
    } catch (error) {
      throw asImportError(error);
    }
  }
  notifyThreadReturned(
    deps,
    file.header,
    recreated ? "thread-created" : "history-rewritten",
  );
  deps.logger.info(
    { threadId, rows: file.table.rows, insertedRows, recreated },
    "Thread file imported",
  );
  return { insertedRows, rows: file.table.rows, threadId };
}

export async function importThreadFile(
  deps: SavedThreadDeps,
  path: string,
): Promise<ThreadFileImportResult> {
  const file = await openForImport(path);
  try {
    const { threadId } = file.header;
    if (getSavedThread(deps.db, threadId)?.state === "saved") {
      await restoreSavedThreadWith(deps, threadId, new Set());
    }
    const release = holdSavedThread(deps.db, threadId);
    try {
      return await importIntoThread(deps, file);
    } finally {
      release();
    }
  } finally {
    await file.close();
  }
}

async function restoreSavedThreadWith(
  deps: SavedThreadDeps,
  threadId: string,
  visited: Set<string>,
): Promise<ThreadFileImportResult> {
  const release = holdSavedThread(deps.db, threadId);
  try {
    const saved = getSavedThread(deps.db, threadId);
    if (saved?.state !== "saved") {
      throw new ApiError(
        404,
        "saved_thread_not_found",
        "Saved thread not found",
      );
    }
    if (!(await savedThreadFileExists(deps.config.dataDir, threadId))) {
      throw new ApiError(
        409,
        "saved_thread_file_missing",
        `The file for saved thread ${threadId} is missing`,
      );
    }
    const file = await openForImport(
      savedThreadFilePath(deps.config.dataDir, threadId),
    );
    try {
      return await restoreFromSavedFile(deps, file, visited);
    } finally {
      await file.close();
    }
  } finally {
    release();
  }
}

export function restoreSavedThread(
  deps: SavedThreadDeps,
  threadId: string,
): Promise<ThreadFileImportResult> {
  return restoreSavedThreadWith(deps, threadId, new Set());
}
