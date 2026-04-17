import { publicUptimeOverviewResponseSchema } from '../schemas/public-uptime-overview';
import type {
  PublicUptimeOverviewRange,
  PublicUptimeOverviewResponse,
} from '../schemas/public-uptime-overview';

const MAX_AGE_SECONDS = 2 * 60;

const SNAPSHOT_KEY_BY_RANGE = {
  '30d': 'analytics-uptime:30d',
  '90d': 'analytics-uptime:90d',
} satisfies Record<PublicUptimeOverviewRange, string>;

const READ_UPTIME_OVERVIEW_SQL = `
  SELECT generated_at, updated_at, body_json
  FROM public_snapshots
  WHERE key = ?1
`;

const UPSERT_UPTIME_OVERVIEW_SQL = `
  INSERT INTO public_snapshots (key, generated_at, body_json, updated_at)
  VALUES (?1, ?2, ?3, ?4)
  ON CONFLICT(key) DO UPDATE SET
    generated_at = excluded.generated_at,
    body_json = excluded.body_json,
    updated_at = excluded.updated_at
  WHERE excluded.generated_at >= public_snapshots.generated_at
`;

type SnapshotRow = {
  generated_at: number;
  updated_at?: number | null;
  body_json: string;
};

type SnapshotCacheEntry = {
  generatedAt: number;
  updatedAt: number;
  bodyJson: string;
  data: PublicUptimeOverviewResponse;
};

const readStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const upsertStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const cacheByDb = new WeakMap<D1Database, Map<PublicUptimeOverviewRange, SnapshotCacheEntry>>();

function getSnapshotKey(range: PublicUptimeOverviewRange): string {
  return SNAPSHOT_KEY_BY_RANGE[range];
}

function readStatement(db: D1Database): D1PreparedStatement {
  const cached = readStatementByDb.get(db);
  if (cached) {
    return cached;
  }

  const statement = db.prepare(READ_UPTIME_OVERVIEW_SQL);
  readStatementByDb.set(db, statement);
  return statement;
}

function upsertStatement(db: D1Database): D1PreparedStatement {
  const cached = upsertStatementByDb.get(db);
  if (cached) {
    return cached;
  }

  const statement = db.prepare(UPSERT_UPTIME_OVERVIEW_SQL);
  upsertStatementByDb.set(db, statement);
  return statement;
}

function readCacheMap(db: D1Database): Map<PublicUptimeOverviewRange, SnapshotCacheEntry> {
  let cached = cacheByDb.get(db);
  if (!cached) {
    cached = new Map<PublicUptimeOverviewRange, SnapshotCacheEntry>();
    cacheByDb.set(db, cached);
  }
  return cached;
}

function toSnapshotUpdatedAt(row: Pick<SnapshotRow, 'generated_at' | 'updated_at'>): number {
  return typeof row.updated_at === 'number' && Number.isFinite(row.updated_at)
    ? row.updated_at
    : row.generated_at;
}

function parseSnapshotBody(
  range: PublicUptimeOverviewRange,
  bodyJson: string,
): { bodyJson: string; data: PublicUptimeOverviewResponse } | null {
  const trimmed = bodyJson.trim();
  if (!trimmed) {
    return null;
  }

  try {
    const parsed = publicUptimeOverviewResponseSchema.safeParse(JSON.parse(trimmed) as unknown);
    if (!parsed.success || parsed.data.range !== range) {
      return null;
    }
    return {
      bodyJson: trimmed,
      data: parsed.data,
    };
  } catch {
    return null;
  }
}

function writeCachedSnapshot(
  db: D1Database,
  range: PublicUptimeOverviewRange,
  entry: SnapshotCacheEntry,
): SnapshotCacheEntry {
  readCacheMap(db).set(range, entry);
  return entry;
}

export async function readPublicUptimeOverviewSnapshotJson(
  db: D1Database,
  range: PublicUptimeOverviewRange,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  try {
    const row = await readStatement(db).bind(getSnapshotKey(range)).first<SnapshotRow>();
    if (!row?.body_json) {
      return null;
    }

    const age = Math.max(0, now - row.generated_at);
    if (age > MAX_AGE_SECONDS) {
      return null;
    }

    const updatedAt = toSnapshotUpdatedAt(row);
    const cached = readCacheMap(db).get(range);
    if (cached && cached.generatedAt === row.generated_at && cached.updatedAt === updatedAt) {
      return {
        bodyJson: cached.bodyJson,
        age,
      };
    }

    const validated = parseSnapshotBody(range, row.body_json);
    if (!validated) {
      console.warn('uptime overview snapshot: invalid payload, falling back to live');
      return null;
    }

    const next = writeCachedSnapshot(db, range, {
      generatedAt: row.generated_at,
      updatedAt,
      bodyJson: validated.bodyJson,
      data: validated.data,
    });
    return {
      bodyJson: next.bodyJson,
      age,
    };
  } catch (err) {
    console.warn('uptime overview snapshot: read failed, falling back to live', err);
    return null;
  }
}

export async function writePublicUptimeOverviewSnapshots(
  db: D1Database,
  now: number,
  snapshots: Record<PublicUptimeOverviewRange, PublicUptimeOverviewResponse>,
): Promise<void> {
  const statement = upsertStatement(db);
  const writes: D1PreparedStatement[] = [];

  for (const range of Object.keys(SNAPSHOT_KEY_BY_RANGE) as PublicUptimeOverviewRange[]) {
    const parsed = publicUptimeOverviewResponseSchema.safeParse(snapshots[range]);
    if (!parsed.success) {
      throw new Error(`Failed to validate uptime overview snapshot for range ${range}`);
    }

    const bodyJson = JSON.stringify(parsed.data);
    writeCachedSnapshot(db, range, {
      generatedAt: parsed.data.generated_at,
      updatedAt: now,
      bodyJson,
      data: parsed.data,
    });
    writes.push(statement.bind(getSnapshotKey(range), parsed.data.generated_at, bodyJson, now));
  }

  await db.batch(writes);
}
