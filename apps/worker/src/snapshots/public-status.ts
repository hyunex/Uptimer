import { AppError } from '../middleware/errors';
import { publicStatusResponseSchema, type PublicStatusResponse } from '../schemas/public-status';
import { primeStatusSnapshotCache } from './public-status-read';

const SNAPSHOT_KEY = 'status';
const MAX_AGE_SECONDS = 60;
const FUTURE_SNAPSHOT_TOLERANCE_SECONDS = 60;
const READ_STATUS_SQL = `
  SELECT generated_at, body_json
  FROM public_snapshots
  WHERE key = ?1
`;
const UPSERT_STATUS_SQL = `
  INSERT INTO public_snapshots (key, generated_at, body_json, updated_at)
  VALUES (?1, ?2, ?3, ?4)
  ON CONFLICT(key) DO UPDATE SET
    generated_at = excluded.generated_at,
    body_json = excluded.body_json,
    updated_at = excluded.updated_at
  WHERE excluded.generated_at >= public_snapshots.generated_at
    OR public_snapshots.generated_at > ?5
`;

const readStatusStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const upsertStatusStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();

export function getSnapshotKey() {
  return SNAPSHOT_KEY;
}

export function getSnapshotMaxAgeSeconds() {
  return MAX_AGE_SECONDS;
}

function safeJsonParse(text: string): unknown | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  try {
    return JSON.parse(trimmed) as unknown;
  } catch {
    return null;
  }
}

function normalizeStatusSnapshotPayload(value: unknown): PublicStatusResponse | null {
  const parsed = publicStatusResponseSchema.safeParse(value);
  return parsed.success ? parsed.data : null;
}

function normalizeStatusSnapshotBodyJson(bodyJson: string): string | null {
  const parsed = safeJsonParse(bodyJson);
  if (parsed === null) {
    return null;
  }

  const payload = normalizeStatusSnapshotPayload(parsed);
  return payload ? JSON.stringify(payload) : null;
}

export async function readStatusSnapshot(
  db: D1Database,
  now: number,
): Promise<{ data: PublicStatusResponse; age: number } | null> {
  try {
    const cached = readStatusStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_STATUS_SQL);
    if (!cached) {
      readStatusStatementByDb.set(db, statement);
    }

    const row = await statement
      .bind(SNAPSHOT_KEY)
      .first<{ generated_at: number; body_json: string }>();

    if (!row) return null;
    if (row.generated_at > now + FUTURE_SNAPSHOT_TOLERANCE_SECONDS) return null;

    const age = Math.max(0, now - row.generated_at);
    if (age > MAX_AGE_SECONDS) return null;

    const parsed = safeJsonParse(row.body_json);
    if (parsed === null) {
      console.warn('public snapshot: invalid JSON, falling back to live');
      return null;
    }
    const payload = normalizeStatusSnapshotPayload(parsed);
    if (!payload) {
      console.warn('public snapshot: invalid payload, falling back to live');
      return null;
    }
    return { data: payload, age };
  } catch (err) {
    // Backward compatible: if the table doesn't exist yet or snapshot is invalid,
    // callers should fall back to live computation.
    console.warn('public snapshot: read failed, falling back to live', err);
    return null;
  }
}

export async function readStatusSnapshotJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  try {
    const cached = readStatusStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_STATUS_SQL);
    if (!cached) {
      readStatusStatementByDb.set(db, statement);
    }

    const row = await statement
      .bind(SNAPSHOT_KEY)
      .first<{ generated_at: number; body_json: string }>();

    if (!row) return null;
    if (row.generated_at > now + FUTURE_SNAPSHOT_TOLERANCE_SECONDS) return null;

    const age = Math.max(0, now - row.generated_at);
    if (age > MAX_AGE_SECONDS) return null;

    const bodyJson = normalizeStatusSnapshotBodyJson(row.body_json);
    if (!bodyJson) {
      console.warn('public snapshot: invalid payload, falling back to live');
      return null;
    }
    return { bodyJson, age };
  } catch (err) {
    console.warn('public snapshot: read failed, falling back to live', err);
    return null;
  }
}

export async function writeStatusSnapshot(
  db: D1Database,
  now: number,
  payload: PublicStatusResponse,
): Promise<void> {
  const bodyJson = JSON.stringify(payload);
  const cached = upsertStatusStatementByDb.get(db);
  const statement = cached ?? db.prepare(UPSERT_STATUS_SQL);
  if (!cached) {
    upsertStatusStatementByDb.set(db, statement);
  }

  primeStatusSnapshotCache({
    db,
    generatedAt: payload.generated_at,
    updatedAt: now,
    bodyJson,
    data: payload,
  });
  await statement
    .bind(
      SNAPSHOT_KEY,
      payload.generated_at,
      bodyJson,
      now,
      now + FUTURE_SNAPSHOT_TOLERANCE_SECONDS,
    )
    .run();
}

export function applyStatusCacheHeaders(res: Response, ageSeconds: number): void {
  // Guarantee freshness bound <= 60s. Prefer <= 30s in normal cases.
  //
  // We ensure (max-age + stale-*) never exceeds MAX_AGE_SECONDS.
  const remaining = Math.max(0, MAX_AGE_SECONDS - ageSeconds);
  const maxAge = Math.min(30, remaining);
  const stale = Math.max(0, remaining - maxAge);

  res.headers.set(
    'Cache-Control',
    `public, max-age=${maxAge}, stale-while-revalidate=${stale}, stale-if-error=${stale}`,
  );
}

export function toSnapshotPayload(value: unknown): PublicStatusResponse {
  const parsed = publicStatusResponseSchema.safeParse(value);
  if (!parsed.success) {
    throw new AppError(500, 'INTERNAL', 'Failed to generate status snapshot');
  }
  return parsed.data;
}
