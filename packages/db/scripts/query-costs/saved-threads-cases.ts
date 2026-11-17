import type { DbConnection } from "../../src/connection.js";
import {
  deleteSavedThread,
  getSavedThread,
  getSavedThreadTotals,
  hideThreadForSaving,
  insertThreadHistoryRows,
  insertThreadTableRow,
  isThreadSavable,
  listSavedThreadIds,
  listSavedThreads,
  listSavedThreadsInStates,
  listThreadAttachments,
  nextThreadSave,
  readThreadEventWindow,
  readThreadTableRows,
  readThreadTableRowsPage,
  restartThreadArchiveClock,
  THREAD_FILE_FORMAT_VERSION,
  unhideSavedThread,
  updateSavedThreadInState,
  upsertSavedThread,
  type SavedThread,
  type ThreadEventWindow,
  type ThreadFileTableRow,
} from "../../src/data/saved-threads.js";
import type { HistoryFixture } from "../../src/history-fixture.js";
import type { SavedThreadState } from "../../src/schema.js";
import { registerCostGroup, type QueryCostCase } from "./case.js";

const SAVED = "packages/db/src/data/saved-threads.ts";
const SAVED_THREADS_AT_FULL_SCALE = 4_000;
const SAVED_THREAD_LIST_MAX_LIMIT = 200;
const THREAD_FILE_WINDOW_BYTES = 128 * 1024;
const THREAD_FILE_WINDOW_ROWS = 128;
const TABLE_ROWS_PER_STEP = 256;
const RESTORED_THREAD_ID = "query-cost-restored-thread";

interface SavedThreadsSeed {
  savedThreadId: string;
  savingThreadId: string;
  threadRow: ThreadFileTableRow;
  window: ThreadEventWindow;
}

function stateFor(index: number): SavedThreadState {
  if (index % 20 === 0) return "saving";
  if (index % 20 === 1) return "restored";
  return "saved";
}

function savedThreadRow(
  fixture: HistoryFixture,
  index: number,
  now: number,
): SavedThread {
  const state = stateFor(index);
  return {
    threadId: `query-cost-saved-${index}`,
    projectId: fixture.projectId,
    state,
    title: `Saved thread ${index}: fix the failing build`,
    providerId: "query-costs",
    threadCreatedAt: now - (index + 30) * 86_400_000,
    archivedAt: now - (index + 14) * 86_400_000,
    formatVersion: THREAD_FILE_FORMAT_VERSION,
    throughSequence: 1_000 + index,
    rowCount: 1_000 + index,
    rawBytes: 4_000_000 + index,
    packedBytes: 400_000 + index,
    savedAt: state === "saving" ? null : now - index * 60_000,
    restoredAt: state === "restored" ? now - index * 30_000 : null,
    updatedAt: now - index * 60_000,
  };
}

function seedSavedThreads(
  db: DbConnection,
  fixture: HistoryFixture,
  scale: number,
): SavedThreadsSeed {
  const now = Date.now();
  const count = Math.round(SAVED_THREADS_AT_FULL_SCALE * scale);
  db.transaction((tx) => {
    for (let index = 0; index < count; index += 1) {
      upsertSavedThread(tx, savedThreadRow(fixture, index, now));
    }
  });
  const threadRow = readThreadTableRowsPage(db, {
    afterRowid: 0,
    limit: 1,
    table: "threads",
    threadId: fixture.threadId,
  }).rows[0];
  if (threadRow === undefined)
    throw new Error("Expected the fixture thread row");
  const window = readThreadEventWindow(db, {
    afterSequence: 0,
    maxBytes: THREAD_FILE_WINDOW_BYTES,
    maxRows: THREAD_FILE_WINDOW_ROWS,
    threadId: fixture.threadId,
    throughSequence: null,
  });
  return {
    savedThreadId: "query-cost-saved-2",
    savingThreadId: "query-cost-saved-0",
    threadRow,
    window,
  };
}

function restoredThreadRow(seed: SavedThreadsSeed): ThreadFileTableRow {
  return {
    table: "threads",
    values: { ...seed.threadRow.values, id: RESTORED_THREAD_ID },
  };
}

const SAVED_THREAD_CASES: readonly QueryCostCase<SavedThreadsSeed>[] = [
  {
    name: `${SAVED}#isThreadSavable`,
    run: (db, fixture) =>
      isThreadSavable(db, { inProgress: false, threadId: fixture.threadId }),
  },
  {
    name: `${SAVED}#isThreadSavable`,
    variant: "in progress",
    run: (db, fixture) =>
      isThreadSavable(db, { inProgress: true, threadId: fixture.threadId }),
  },
  {
    name: `${SAVED}#getSavedThread`,
    run: (db, _fixture, seed) => getSavedThread(db, seed.savedThreadId),
  },
  {
    name: `${SAVED}#listSavedThreads`,
    run: (db) =>
      listSavedThreads(db, {
        limit: SAVED_THREAD_LIST_MAX_LIMIT + 1,
        offset: 0,
        projectId: null,
        search: null,
      }),
  },
  {
    name: `${SAVED}#listSavedThreads`,
    variant: "project search",
    run: (db, fixture) =>
      listSavedThreads(db, {
        limit: SAVED_THREAD_LIST_MAX_LIMIT + 1,
        offset: 0,
        projectId: fixture.projectId,
        search: "build",
      }),
  },
  {
    name: `${SAVED}#listSavedThreadsInStates`,
    run: (db) => listSavedThreadsInStates(db, ["saving", "restoring"]),
  },
  {
    name: `${SAVED}#nextThreadSave`,
    run: (db, _fixture, seed) =>
      nextThreadSave(db, { excludedThreadIds: new Set([seed.savingThreadId]) }),
  },
  {
    name: `${SAVED}#listSavedThreadIds`,
    run: (db) => listSavedThreadIds(db),
  },
  {
    name: `${SAVED}#upsertSavedThread`,
    run: (db, fixture) =>
      upsertSavedThread(db, {
        ...savedThreadRow(fixture, 2, Date.now()),
        threadId: fixture.threadId,
        state: "saving",
        savedAt: null,
      }),
  },
  {
    name: `${SAVED}#updateSavedThreadInState`,
    run: (db, _fixture, seed) =>
      updateSavedThreadInState(db, {
        expectedState: "saving",
        patch: { state: "saved", savedAt: Date.now(), updatedAt: Date.now() },
        threadId: seed.savingThreadId,
      }),
  },
  {
    name: `${SAVED}#deleteSavedThread`,
    run: (db, _fixture, seed) => deleteSavedThread(db, seed.savedThreadId),
  },
  {
    name: `${SAVED}#getSavedThreadTotals`,
    run: (db) => getSavedThreadTotals(db),
  },
  {
    name: `${SAVED}#readThreadEventWindow`,
    run: (db, fixture) =>
      readThreadEventWindow(db, {
        afterSequence: fixture.turns[0]?.requestSequence ?? 0,
        maxBytes: THREAD_FILE_WINDOW_BYTES,
        maxRows: THREAD_FILE_WINDOW_ROWS,
        threadId: fixture.threadId,
        throughSequence: fixture.latestSequence,
      }),
  },
  {
    name: `${SAVED}#readThreadTableRowsPage`,
    run: (db, fixture) =>
      readThreadTableRowsPage(db, {
        afterRowid: 0,
        limit: TABLE_ROWS_PER_STEP,
        table: "prompt_history_entries",
        threadId: fixture.threadId,
      }),
  },
  {
    name: `${SAVED}#readThreadTableRows`,
    run: (db, fixture) => readThreadTableRows(db, fixture.threadId),
  },
  {
    name: `${SAVED}#insertThreadTableRow`,
    run: (db, _fixture, seed) =>
      insertThreadTableRow(db, restoredThreadRow(seed)),
  },
  {
    name: `${SAVED}#listThreadAttachments`,
    run: (db, fixture) => listThreadAttachments(db, fixture.threadId),
  },
  {
    name: `${SAVED}#insertThreadHistoryRows`,
    prepare: (db, _fixture, seed) => {
      insertThreadTableRow(db, restoredThreadRow(seed));
    },
    run: (db, _fixture, seed) =>
      insertThreadHistoryRows(db, {
        now: Date.now(),
        retained: seed.window.retained,
        rows: seed.window.rows.map((row) => ({
          ...row,
          id: `${RESTORED_THREAD_ID}-${row.id}`,
          threadId: RESTORED_THREAD_ID,
        })),
      }),
  },
  {
    name: `${SAVED}#hideThreadForSaving`,
    run: (db, fixture) =>
      hideThreadForSaving(db, { now: Date.now(), threadId: fixture.threadId }),
  },
  {
    name: `${SAVED}#unhideSavedThread`,
    run: (db, fixture) => unhideSavedThread(db, fixture.threadId),
  },
  {
    name: `${SAVED}#restartThreadArchiveClock`,
    run: (db, fixture) =>
      restartThreadArchiveClock(db, {
        now: Date.now(),
        threadId: fixture.threadId,
      }),
  },
];

export const SAVED_THREADS_COST_GROUP = registerCostGroup({
  name: "saved-threads",
  seed: seedSavedThreads,
  cases: SAVED_THREAD_CASES,
});
