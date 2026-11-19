import { and, asc, desc, eq, inArray, isNotNull, like, notInArray, sql } from "drizzle-orm";
import type { DbQueryConnection } from "../connection.js";
import {
  environments,
  projectAttachments,
  projectAttachmentThreads,
  retainedEventOutputs,
  savedThreads,
  threads,
  type SavedThreadState,
} from "../schema.js";
import { queryInSqliteVariableBatches } from "./sqlite-variable-batches.js";

export const THREAD_FILE_FORMAT_VERSION = 1;

export const THREAD_FILE_TABLES = [
  "threads",
  "thread_plugin_metadata",
  "thread_tabs",
  "prompt_history_entries",
  "thread_search_segments",
  "thread_conversation_outlines",
] as const;
export type ThreadFileTable = (typeof THREAD_FILE_TABLES)[number];

export type ThreadFileValue = string | number | null;

export interface ThreadFileTableRow {
  table: ThreadFileTable;
  values: Record<string, ThreadFileValue>;
}

export type SavedThread = typeof savedThreads.$inferSelect;
export type SavedThreadAttachment = typeof projectAttachments.$inferSelect;

export interface ThreadHistoryEventRow {
  createdAt: number;
  data: string;
  environmentId: string | null;
  id: string;
  itemId: string | null;
  itemKind: string | null;
  parentToolCallId: string | null;
  providerThreadId: string | null;
  scopeKind: string;
  sequence: number;
  threadId: string;
  turnId: string | null;
  type: string;
}

export interface ThreadHistoryRetainedOutputRow {
  eventId: string;
  expiresAt: number;
  outputPath: string;
  value: string;
}

export interface ThreadEventWindowArgs {
  afterSequence: number;
  maxBytes: number;
  maxRows: number;
  threadId: string;
  throughSequence: number | null;
}

export interface ThreadEventWindow {
  bytes: number;
  lastSequence: number | null;
  retained: ThreadHistoryRetainedOutputRow[];
  rows: ThreadHistoryEventRow[];
}

const LISTED_STATES: readonly SavedThreadState[] = ["saved"];

function saveEligibilitySql(inProgress: boolean) {
  return sql`
    t.deleted_at IS NULL
    AND t.pinned_at IS NULL
    AND t.status NOT IN ('active', 'starting', 'stopping')
    AND (s.thread_id IS NULL OR s.state ${inProgress ? sql`IN ('restored', 'saving')` : sql`= 'restored'`})
    AND EXISTS (
      SELECT 1 FROM projects p WHERE p.id = t.project_id AND p.deleted_at IS NULL
    )
    AND EXISTS (
      SELECT 1 FROM project_attachment_backfills b
      WHERE b.project_id = t.project_id AND b.phase = 'done'
    )
    AND NOT EXISTS (
      SELECT 1 FROM threads c
      WHERE c.parent_thread_id = t.id
        OR c.source_thread_id = t.id
        OR c.lifecycle_owner_thread_id = t.id
    )
    AND NOT EXISTS (
      SELECT 1 FROM pending_interactions p
      WHERE p.thread_id = t.id AND p.status IN ('pending', 'resolving')
    )
    AND NOT EXISTS (SELECT 1 FROM queued_thread_messages q WHERE q.thread_id = t.id)
    AND NOT EXISTS (SELECT 1 FROM thread_pruning_work w WHERE w.thread_id = t.id)
    AND NOT EXISTS (
      SELECT 1 FROM events st
      WHERE st.thread_id = t.id
        AND st.item_kind = 'backgroundTask'
        AND st.type = 'item/started'
        AND st.item_id IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM events c
          WHERE c.thread_id = st.thread_id
            AND c.item_kind = 'backgroundTask'
            AND c.type = 'item/backgroundTask/completed'
            AND c.item_id = st.item_id
        )
    )`;
}

export function isThreadSavable(
  db: DbQueryConnection,
  args: { inProgress: boolean; threadId: string },
): boolean {
  return (
    db.get<{ one: number }>(
      sql`
        SELECT 1 AS one
        FROM threads t
        LEFT JOIN saved_threads s ON s.thread_id = t.id
        WHERE t.id = ${args.threadId}
          AND ${saveEligibilitySql(args.inProgress)}
      `,
    ) !== undefined
  );
}

export function getSavedThread(
  db: DbQueryConnection,
  threadId: string,
): SavedThread | null {
  return (
    db
      .select()
      .from(savedThreads)
      .where(eq(savedThreads.threadId, threadId))
      .get() ?? null
  );
}

export function listSavedThreads(
  db: DbQueryConnection,
  args: { limit: number; offset: number; projectId: string | null; search: string | null },
): SavedThread[] {
  return db
    .select()
    .from(savedThreads)
    .where(
      and(
        inArray(savedThreads.state, [...LISTED_STATES]),
        args.projectId === null ? undefined : eq(savedThreads.projectId, args.projectId),
        args.search === null ? undefined : like(savedThreads.title, `%${args.search}%`),
      ),
    )
    .orderBy(desc(savedThreads.savedAt), desc(savedThreads.threadId))
    .limit(args.limit)
    .offset(args.offset)
    .all();
}

export function listSavedThreadsInStates(
  db: DbQueryConnection,
  states: readonly SavedThreadState[],
): SavedThread[] {
  return db
    .select()
    .from(savedThreads)
    .where(inArray(savedThreads.state, [...states]))
    .all();
}

export function nextThreadSave(
  db: DbQueryConnection,
  args: { excludedThreadIds: ReadonlySet<string> },
): string | null {
  return (
    db
      .select({ threadId: savedThreads.threadId })
      .from(savedThreads)
      .where(
        and(
          eq(savedThreads.state, "saving"),
          args.excludedThreadIds.size === 0
            ? undefined
            : notInArray(savedThreads.threadId, [...args.excludedThreadIds]),
        ),
      )
      .orderBy(asc(savedThreads.updatedAt), asc(savedThreads.threadId))
      .limit(1)
      .get()?.threadId ?? null
  );
}

export function listSavedThreadIds(db: DbQueryConnection): Set<string> {
  return new Set(
    db
      .select({ threadId: savedThreads.threadId })
      .from(savedThreads)
      .where(inArray(savedThreads.state, ["saving", "restoring", ...LISTED_STATES]))
      .all()
      .map((row) => row.threadId),
  );
}

export function upsertSavedThread(db: DbQueryConnection, row: SavedThread): void {
  const { threadId: _threadId, ...set } = row;
  db.insert(savedThreads)
    .values(row)
    .onConflictDoUpdate({ target: savedThreads.threadId, set })
    .run();
}

export function updateSavedThreadInState(
  db: DbQueryConnection,
  args: {
    expectedState: SavedThreadState;
    patch: Partial<Omit<SavedThread, "threadId">>;
    threadId: string;
  },
): boolean {
  return (
    db
      .update(savedThreads)
      .set(args.patch)
      .where(
        and(
          eq(savedThreads.threadId, args.threadId),
          eq(savedThreads.state, args.expectedState),
        ),
      )
      .run().changes > 0
  );
}

export function deleteSavedThread(db: DbQueryConnection, threadId: string): void {
  db.delete(savedThreads).where(eq(savedThreads.threadId, threadId)).run();
}

const eventWindowSql = (args: ThreadEventWindowArgs) => sql`
  SELECT
    id,
    thread_id AS threadId,
    environment_id AS environmentId,
    scope_kind AS scopeKind,
    turn_id AS turnId,
    provider_thread_id AS providerThreadId,
    sequence,
    type,
    item_id AS itemId,
    item_kind AS itemKind,
    parent_tool_call_id AS parentToolCallId,
    data,
    created_at AS createdAt
  FROM events
  WHERE thread_id = ${args.threadId}
    AND sequence > ${args.afterSequence}
    ${args.throughSequence === null ? sql`` : sql`AND sequence <= ${args.throughSequence}`}
  ORDER BY sequence
  LIMIT ${args.maxRows}
`;

function takeWithinBytes<T>(
  rows: readonly T[],
  sizeOf: (row: T) => number,
  maxBytes: number,
): { bytes: number; rows: T[] } {
  const taken: T[] = [];
  let bytes = 0;
  for (const row of rows) {
    const rowBytes = sizeOf(row);
    if (taken.length > 0 && bytes + rowBytes > maxBytes) break;
    taken.push(row);
    bytes += rowBytes;
  }
  return { bytes, rows: taken };
}

function listRetainedOutputsForEvents(
  db: DbQueryConnection,
  eventIds: readonly string[],
): ThreadHistoryRetainedOutputRow[] {
  return queryInSqliteVariableBatches({
    dedupeKey: (eventId) => eventId,
    fixedVariableCount: 0,
    variableCountPerValue: 1,
    values: eventIds,
    queryBatch: (batch) =>
      db
        .select({
          eventId: retainedEventOutputs.eventId,
          expiresAt: retainedEventOutputs.expiresAt,
          outputPath: retainedEventOutputs.outputPath,
          value: retainedEventOutputs.value,
        })
        .from(retainedEventOutputs)
        .where(inArray(retainedEventOutputs.eventId, [...batch]))
        .all(),
  });
}

export function readThreadEventWindow(
  db: DbQueryConnection,
  args: ThreadEventWindowArgs,
): ThreadEventWindow {
  const { bytes, rows } = takeWithinBytes(
    db.all<ThreadHistoryEventRow>(eventWindowSql(args)),
    (row) => Buffer.byteLength(row.data),
    args.maxBytes,
  );
  return {
    bytes,
    lastSequence: rows.at(-1)?.sequence ?? null,
    retained: listRetainedOutputsForEvents(
      db,
      rows.map((row) => row.id),
    ),
    rows,
  };
}

const tableColumnsCache = new Map<ThreadFileTable, Set<string>>();

function tableColumns(db: DbQueryConnection, table: ThreadFileTable): Set<string> {
  const cached = tableColumnsCache.get(table);
  if (cached !== undefined) return cached;
  const columns = new Set(
    db
      .all<{ name: string }>(sql`SELECT name FROM pragma_table_info(${table})`)
      .map((row) => row.name),
  );
  tableColumnsCache.set(table, columns);
  return columns;
}

function isThreadFileValue(value: unknown): value is ThreadFileValue {
  return value === null || typeof value === "string" || typeof value === "number";
}

export function readThreadTableRowsPage(
  db: DbQueryConnection,
  args: { afterRowid: number; limit: number; table: ThreadFileTable; threadId: string },
): { lastRowid: number | null; rows: ThreadFileTableRow[] } {
  const column = args.table === "threads" ? "id" : "thread_id";
  const found = db.all<Record<string, unknown> & { __rowid: number }>(sql`
    SELECT rowid AS __rowid, * FROM ${sql.identifier(args.table)}
    WHERE ${sql.identifier(column)} = ${args.threadId} AND rowid > ${args.afterRowid}
    ORDER BY rowid
    LIMIT ${args.limit}
  `);
  return {
    lastRowid: found.at(-1)?.__rowid ?? null,
    rows: found.map(({ __rowid: _rowid, ...row }) => ({
      table: args.table,
      values: Object.fromEntries(
        Object.entries(row).flatMap(([key, value]) =>
          isThreadFileValue(value) ? [[key, value]] : [],
        ),
      ),
    })),
  };
}

export function readThreadTableRows(
  db: DbQueryConnection,
  threadId: string,
): ThreadFileTableRow[] {
  return THREAD_FILE_TABLES.flatMap((table) => {
    const rows: ThreadFileTableRow[] = [];
    for (let afterRowid = 0; ; ) {
      const page = readThreadTableRowsPage(db, { afterRowid, limit: 1000, table, threadId });
      rows.push(...page.rows);
      if (page.lastRowid === null) return rows;
      afterRowid = page.lastRowid;
    }
  });
}

function existingId(
  db: DbQueryConnection,
  table: "environments" | "thread_sections" | "threads",
  id: ThreadFileValue | undefined,
): string | null {
  if (typeof id !== "string") return null;
  return db.get<{ id: string }>(
    sql`SELECT id FROM ${sql.identifier(table)} WHERE id = ${id}`,
  ) === undefined
    ? null
    : id;
}

function insertRow(
  db: DbQueryConnection,
  table: ThreadFileTable,
  values: Record<string, ThreadFileValue>,
): boolean {
  const columns = tableColumns(db, table);
  const entries = Object.entries(values).filter(([key]) => columns.has(key));
  if (entries.length === 0) return false;
  return (
    db.run(sql`
      INSERT OR IGNORE INTO ${sql.identifier(table)}
        (${sql.join(entries.map(([key]) => sql.identifier(key)), sql`, `)})
      VALUES (${sql.join(entries.map(([, value]) => sql`${value}`), sql`, `)})
    `).changes > 0
  );
}

export function insertHiddenThreadRow(
  db: DbQueryConnection,
  row: ThreadFileTableRow,
  hiddenAt: number,
): boolean {
  if (row.table !== "threads") {
    throw new Error(`Expected a threads row, got ${row.table}`);
  }
  const { values } = row;
  return insertRow(db, "threads", {
    ...values,
    environment_id: existingId(db, "environments", values.environment_id),
    section_id: existingId(db, "thread_sections", values.section_id),
    parent_thread_id: existingId(db, "threads", values.parent_thread_id),
    source_thread_id: existingId(db, "threads", values.source_thread_id),
    lifecycle_owner_thread_id: existingId(db, "threads", values.lifecycle_owner_thread_id),
    deleted_at: hiddenAt,
  });
}

export function insertThreadTableRow(
  db: DbQueryConnection,
  row: ThreadFileTableRow,
): boolean {
  if (row.table === "threads") {
    throw new Error("Insert a thread row with insertHiddenThreadRow");
  }
  return insertRow(db, row.table, row.values);
}

export function readThreadSaveFingerprint(
  db: DbQueryConnection,
  threadId: string,
): string | null {
  return (
    db.get<{ fingerprint: string }>(sql`
      SELECT json_array(
        t.updated_at,
        t.archived_at,
        t.pinned_at,
        t.deleted_at,
        (SELECT max(e.sequence) FROM events e WHERE e.thread_id = ${threadId}),
        (SELECT json_group_array(json_array(m.plugin_id, m.metadata_json))
          FROM thread_plugin_metadata m WHERE m.thread_id = ${threadId}),
        (SELECT json_array(tabs.revision, tabs.updated_at)
          FROM thread_tabs tabs WHERE tabs.thread_id = ${threadId}),
        (SELECT json_array(count(*), max(p.request_sequence))
          FROM prompt_history_entries p WHERE p.thread_id = ${threadId}),
        (SELECT json_array(count(*), max(s.updated_at), total(length(s.text)))
          FROM thread_search_segments s WHERE s.thread_id = ${threadId}),
        (SELECT json_group_array(json_array(o.projection_key, length(o.items_json)))
          FROM thread_conversation_outlines o WHERE o.thread_id = ${threadId})
      ) AS fingerprint
      FROM threads t
      WHERE t.id = ${threadId}
    `)?.fingerprint ?? null
  );
}

export function listThreadAttachments(
  db: DbQueryConnection,
  threadId: string,
): SavedThreadAttachment[] {
  return db
    .select({ attachment: projectAttachments })
    .from(projectAttachmentThreads)
    .innerJoin(
      projectAttachments,
      eq(projectAttachments.id, projectAttachmentThreads.attachmentId),
    )
    .where(
      and(
        eq(projectAttachmentThreads.threadId, threadId),
        isNotNull(projectAttachments.readyAt),
        sql`${projectAttachments.deletionClaimedAt} IS NULL`,
      ),
    )
    .all()
    .map((row) => row.attachment);
}

export function insertThreadHistoryRows(
  db: DbQueryConnection,
  args: {
    now: number;
    retained: readonly ThreadHistoryRetainedOutputRow[];
    rows: readonly ThreadHistoryEventRow[];
  },
): number {
  const environmentIds = [
    ...new Set(
      args.rows.flatMap((row) => (row.environmentId === null ? [] : [row.environmentId])),
    ),
  ];
  const liveEnvironmentIds = new Set(
    environmentIds.length === 0
      ? []
      : db
          .select({ id: environments.id })
          .from(environments)
          .where(inArray(environments.id, environmentIds))
          .all()
          .map((row) => row.id),
  );
  let inserted = 0;
  for (const row of args.rows) {
    const environmentId =
      row.environmentId !== null && liveEnvironmentIds.has(row.environmentId)
        ? row.environmentId
        : null;
    inserted += db.run(sql`
      INSERT OR IGNORE INTO events (
        id, thread_id, environment_id, scope_kind, turn_id, provider_thread_id,
        sequence, type, item_id, item_kind, parent_tool_call_id, data, created_at
      ) VALUES (
        ${row.id}, ${row.threadId}, ${environmentId}, ${row.scopeKind}, ${row.turnId},
        ${row.providerThreadId}, ${row.sequence}, ${row.type}, ${row.itemId},
        ${row.itemKind}, ${row.parentToolCallId}, ${row.data}, ${row.createdAt}
      )
    `).changes;
  }
  for (const output of args.retained) {
    if (output.expiresAt <= args.now) continue;
    db.run(sql`
      INSERT OR IGNORE INTO retained_event_outputs (event_id, output_path, value, expires_at)
      SELECT ${output.eventId}, ${output.outputPath}, ${output.value}, ${output.expiresAt}
      WHERE EXISTS (SELECT 1 FROM events WHERE id = ${output.eventId})
    `);
  }
  return inserted;
}

export function hideThreadForSaving(
  db: DbQueryConnection,
  args: { now: number; threadId: string },
): boolean {
  return (
    db
      .update(threads)
      .set({ deletedAt: args.now })
      .where(and(eq(threads.id, args.threadId), sql`${threads.deletedAt} IS NULL`))
      .run().changes > 0
  );
}

export function unhideSavedThread(db: DbQueryConnection, threadId: string): void {
  db.update(threads).set({ deletedAt: null }).where(eq(threads.id, threadId)).run();
}

export function restartThreadArchiveClock(
  db: DbQueryConnection,
  args: { now: number; threadId: string },
): void {
  db.update(threads)
    .set({ archivedAt: args.now })
    .where(and(eq(threads.id, args.threadId), isNotNull(threads.archivedAt)))
    .run();
}
