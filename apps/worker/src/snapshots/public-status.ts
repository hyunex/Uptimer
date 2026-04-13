import { AppError } from '../middleware/errors';
import { publicStatusResponseSchema, type PublicStatusResponse } from '../schemas/public-status';

const SNAPSHOT_KEY = 'status';
const MAX_AGE_SECONDS = 60;
const READ_STATUS_SQL = `
  SELECT generated_at, body_json
  FROM public_snapshots
  WHERE key = ?1
`;

const readStatusStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();

export function getSnapshotKey() {
  return SNAPSHOT_KEY;
}

export function getSnapshotMaxAgeSeconds() {
  return MAX_AGE_SECONDS;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function looksLikeStatusPayload(value: unknown): value is PublicStatusResponse {
  if (!isRecord(value)) return false;
  const maintenance = value.maintenance_windows;
  return (
    typeof value.generated_at === 'number' &&
    typeof value.site_title === 'string' &&
    typeof value.site_description === 'string' &&
    typeof value.site_locale === 'string' &&
    typeof value.site_timezone === 'string' &&
    typeof value.uptime_rating_level === 'number' &&
    typeof value.overall_status === 'string' &&
    isRecord(value.banner) &&
    isRecord(value.summary) &&
    Array.isArray(value.monitors) &&
    Array.isArray(value.active_incidents) &&
    isRecord(maintenance) &&
    Array.isArray(maintenance.active) &&
    Array.isArray(maintenance.upcoming)
  );
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

    const age = Math.max(0, now - row.generated_at);
    if (age > MAX_AGE_SECONDS) return null;

    const parsed = safeJsonParse(row.body_json);
    if (parsed === null) {
      console.warn('public snapshot: invalid JSON, falling back to live');
      return null;
    }
    if (!looksLikeStatusPayload(parsed)) {
      console.warn('public snapshot: invalid payload, falling back to live');
      return null;
    }
    return { data: parsed, age };
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

    const age = Math.max(0, now - row.generated_at);
    if (age > MAX_AGE_SECONDS) return null;

    const parsed = safeJsonParse(row.body_json);
    if (parsed === null) {
      console.warn('public snapshot: invalid JSON, falling back to live');
      return null;
    }
    if (!looksLikeStatusPayload(parsed)) {
      console.warn('public snapshot: invalid payload, falling back to live');
      return null;
    }
    return { bodyJson: row.body_json, age };
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
  await db
    .prepare(
      `
      INSERT INTO public_snapshots (key, generated_at, body_json, updated_at)
      VALUES (?1, ?2, ?3, ?4)
      ON CONFLICT(key) DO UPDATE SET
        generated_at = excluded.generated_at,
        body_json = excluded.body_json,
        updated_at = excluded.updated_at
    `,
    )
    .bind(SNAPSHOT_KEY, payload.generated_at, bodyJson, now)
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
