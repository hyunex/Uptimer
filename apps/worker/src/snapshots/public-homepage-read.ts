import { AppError } from '../middleware/errors';
import {
  publicHomepageRenderArtifactSchema,
  publicHomepageResponseSchema,
} from '../schemas/public-homepage';

const SNAPSHOT_KEY = 'homepage';
const SNAPSHOT_ARTIFACT_KEY = 'homepage:artifact';
const MAX_AGE_SECONDS = 60;
const MAX_STALE_SECONDS = 10 * 60;
const SPLIT_SNAPSHOT_VERSION = 3;
const LEGACY_COMBINED_SNAPSHOT_VERSION = 2;
type SnapshotKey = typeof SNAPSHOT_KEY | typeof SNAPSHOT_ARTIFACT_KEY;

const READ_SNAPSHOT_SQL = `
  SELECT generated_at, body_json
  FROM public_snapshots
  WHERE key = ?1
`;
const READ_SNAPSHOT_METADATA_SQL = `
  SELECT generated_at, updated_at
  FROM public_snapshots
  WHERE key = ?1
`;
const READ_REFRESH_SNAPSHOT_ROWS_SQL = `
  SELECT key, generated_at, updated_at, body_json
  FROM public_snapshots
  WHERE key = ?1 OR key = ?2
`;
const readSnapshotStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const readSnapshotMetadataStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const readRefreshSnapshotRowsStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const normalizedHomepagePayloadCacheByDb = new WeakMap<
  D1Database,
  Map<SnapshotKey, NormalizedSnapshotRow>
>();
const normalizedHomepageArtifactCacheByDb = new WeakMap<
  D1Database,
  Map<SnapshotKey, NormalizedSnapshotRow>
>();

type SnapshotRow = {
  generated_at: number;
  body_json: string;
};

type SnapshotRefreshRow = SnapshotRow & {
  key: SnapshotKey;
  updated_at?: number | null;
};

type SnapshotMetadataRow = {
  generated_at: number;
  updated_at?: number | null;
};

type SnapshotCandidate = {
  key: SnapshotKey;
  generatedAt: number;
  updatedAt: number;
};

type NormalizedSnapshotRow = {
  generatedAt: number;
  updatedAt: number;
  bodyJson: string;
};

type ParsedJsonText = {
  trimmed: string;
  value: unknown;
};

type CandidateReadResult = {
  row: NormalizedSnapshotRow | null;
  invalid: boolean;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function parseJsonText(text: string): ParsedJsonText | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  try {
    return {
      trimmed,
      value: JSON.parse(trimmed) as unknown,
    };
  } catch {
    return null;
  }
}

function normalizeDirectHomepagePayload(
  value: unknown,
  rawBodyJson: string | null,
): string | null {
  const directPayload = publicHomepageResponseSchema.safeParse(value);
  if (directPayload.success) {
    return rawBodyJson ?? JSON.stringify(directPayload.data);
  }
  if (!isRecord(value)) {
    return null;
  }

  const normalizedPayload = publicHomepageResponseSchema.safeParse({
    ...value,
    bootstrap_mode:
      value.bootstrap_mode === 'full' || value.bootstrap_mode === 'partial'
        ? value.bootstrap_mode
        : 'full',
    monitor_count_total: Array.isArray(value.monitors) ? value.monitors.length : 0,
  });
  return normalizedPayload.success ? JSON.stringify(normalizedPayload.data) : null;
}

function normalizeHomepagePayloadBodyJson(bodyJson: string): string | null {
  const parsed = parseJsonText(bodyJson);
  if (parsed === null) return null;

  const artifact = publicHomepageRenderArtifactSchema.safeParse(parsed.value);
  if (artifact.success) {
    return JSON.stringify(artifact.data.snapshot);
  }
  if (!isRecord(parsed.value)) {
    return null;
  }

  const version = parsed.value.version;
  if (version === SPLIT_SNAPSHOT_VERSION || version === LEGACY_COMBINED_SNAPSHOT_VERSION) {
    return normalizeDirectHomepagePayload(parsed.value.data, null);
  }

  return normalizeDirectHomepagePayload(parsed.value, parsed.trimmed);
}

function normalizeHomepageArtifactBodyJson(bodyJson: string): string | null {
  const parsed = parseJsonText(bodyJson);
  if (parsed === null) return null;

  const artifact = publicHomepageRenderArtifactSchema.safeParse(parsed.value);
  if (artifact.success) {
    return parsed.trimmed;
  }
  if (!isRecord(parsed.value)) {
    return null;
  }

  const version = parsed.value.version;
  if (version !== SPLIT_SNAPSHOT_VERSION && version !== LEGACY_COMBINED_SNAPSHOT_VERSION) {
    return null;
  }

  const legacyArtifact = publicHomepageRenderArtifactSchema.safeParse(parsed.value.render);
  return legacyArtifact.success ? JSON.stringify(legacyArtifact.data) : null;
}

function toSnapshotUpdatedAt(row: SnapshotMetadataRow): number {
  return typeof row.updated_at === 'number' && Number.isFinite(row.updated_at)
    ? row.updated_at
    : row.generated_at;
}

function getNormalizedSnapshotCache(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>,
  db: D1Database,
): Map<SnapshotKey, NormalizedSnapshotRow> {
  const cached = cacheByDb.get(db);
  if (cached) {
    return cached;
  }

  const next = new Map<SnapshotKey, NormalizedSnapshotRow>();
  cacheByDb.set(db, next);
  return next;
}

function readCachedNormalizedSnapshotRow(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>,
  db: D1Database,
  candidate: SnapshotCandidate,
): NormalizedSnapshotRow | null {
  const cache = getNormalizedSnapshotCache(cacheByDb, db);
  const row = cache.get(candidate.key);
  if (!row) {
    return null;
  }

  return row.generatedAt === candidate.generatedAt && row.updatedAt === candidate.updatedAt
    ? row
    : null;
}

function writeCachedNormalizedSnapshotRow(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>,
  db: D1Database,
  candidate: SnapshotCandidate,
  bodyJson: string,
): NormalizedSnapshotRow {
  const row: NormalizedSnapshotRow = {
    generatedAt: candidate.generatedAt,
    updatedAt: candidate.updatedAt,
    bodyJson,
  };
  getNormalizedSnapshotCache(cacheByDb, db).set(candidate.key, row);
  return row;
}

function isSameUtcDay(a: number, b: number): boolean {
  return Math.floor(a / 86_400) === Math.floor(b / 86_400);
}

async function readSnapshotRow(
  db: D1Database,
  key: string,
): Promise<SnapshotRow | null> {
  try {
    const cached = readSnapshotStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_SNAPSHOT_SQL);
    if (!cached) {
      readSnapshotStatementByDb.set(db, statement);
    }

    return await statement.bind(key).first<SnapshotRow>();
  } catch (err) {
    console.warn('homepage snapshot: read failed', err);
    return null;
  }
}

async function readSnapshotMetadataRow(
  db: D1Database,
  key: SnapshotKey,
): Promise<SnapshotMetadataRow | null> {
  try {
    const cached = readSnapshotMetadataStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_SNAPSHOT_METADATA_SQL);
    if (!cached) {
      readSnapshotMetadataStatementByDb.set(db, statement);
    }

    return await statement.bind(key).first<SnapshotMetadataRow>();
  } catch (err) {
    console.warn('homepage snapshot: metadata read failed', err);
    return null;
  }
}

async function readRefreshSnapshotRows(
  db: D1Database,
): Promise<SnapshotRefreshRow[]> {
  try {
    const cached = readRefreshSnapshotRowsStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_REFRESH_SNAPSHOT_ROWS_SQL);
    if (!cached) {
      readRefreshSnapshotRowsStatementByDb.set(db, statement);
    }

    const { results } = await statement
      .bind(SNAPSHOT_KEY, SNAPSHOT_ARTIFACT_KEY)
      .all<SnapshotRefreshRow>();
    return results ?? [];
  } catch (err) {
    console.warn('homepage snapshot: refresh rows read failed', err);
    return [];
  }
}

function comparePayloadCandidates(a: SnapshotCandidate, b: SnapshotCandidate): number {
  if (a.generatedAt !== b.generatedAt) {
    return b.generatedAt - a.generatedAt;
  }
  if (a.key === b.key) {
    return 0;
  }
  return a.key === SNAPSHOT_KEY ? -1 : 1;
}

function compareArtifactCandidates(a: SnapshotCandidate, b: SnapshotCandidate): number {
  if (a.generatedAt !== b.generatedAt) {
    return b.generatedAt - a.generatedAt;
  }
  if (a.key === b.key) {
    return 0;
  }
  return a.key === SNAPSHOT_ARTIFACT_KEY ? -1 : 1;
}

async function listSnapshotCandidates(db: D1Database): Promise<SnapshotCandidate[]> {
  const [artifactRow, homepageRow] = await Promise.all([
    readSnapshotMetadataRow(db, SNAPSHOT_ARTIFACT_KEY),
    readSnapshotMetadataRow(db, SNAPSHOT_KEY),
  ]);

  const candidates: SnapshotCandidate[] = [];
  if (homepageRow) {
    candidates.push({
      key: SNAPSHOT_KEY,
      generatedAt: homepageRow.generated_at,
      updatedAt: toSnapshotUpdatedAt(homepageRow),
    });
  }
  if (artifactRow) {
    candidates.push({
      key: SNAPSHOT_ARTIFACT_KEY,
      generatedAt: artifactRow.generated_at,
      updatedAt: toSnapshotUpdatedAt(artifactRow),
    });
  }
  return candidates;
}

async function readValidatedSnapshotCandidate(opts: {
  db: D1Database;
  candidate: SnapshotCandidate;
  normalize: (bodyJson: string) => string | null;
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>;
}): Promise<CandidateReadResult> {
  const cached = readCachedNormalizedSnapshotRow(opts.cacheByDb, opts.db, opts.candidate);
  if (cached) {
    return { row: cached, invalid: false };
  }

  const row = await readSnapshotRow(opts.db, opts.candidate.key);
  if (!row || row.generated_at !== opts.candidate.generatedAt) {
    return { row: null, invalid: false };
  }

  const bodyJson = opts.normalize(row.body_json);
  if (!bodyJson) {
    return { row: null, invalid: true };
  }

  return {
    row: writeCachedNormalizedSnapshotRow(opts.cacheByDb, opts.db, opts.candidate, bodyJson),
    invalid: false,
  };
}

async function readFirstValidPayloadCandidate(
  db: D1Database,
  candidates: readonly SnapshotCandidate[],
): Promise<{ row: NormalizedSnapshotRow | null; invalid: boolean }> {
  let invalid = false;

  for (const candidate of candidates) {
    const result = await readValidatedSnapshotCandidate({
      db,
      candidate,
      normalize: normalizeHomepagePayloadBodyJson,
      cacheByDb: normalizedHomepagePayloadCacheByDb,
    });
    if (result.invalid) {
      invalid = true;
    }
    if (result.row) {
      return {
        row: result.row,
        invalid,
      };
    }
  }

  return {
    row: null,
    invalid,
  };
}

export async function readHomepageSnapshotGeneratedAt(db: D1Database): Promise<number | null> {
  const { row } = await readFirstValidPayloadCandidate(
    db,
    (await listSnapshotCandidates(db)).sort(comparePayloadCandidates),
  );
  return row?.generatedAt ?? null;
}

export async function readHomepageArtifactSnapshotGeneratedAt(
  db: D1Database,
): Promise<number | null> {
  const candidates = (await listSnapshotCandidates(db))
    .filter((candidate) => candidate.key === SNAPSHOT_ARTIFACT_KEY)
    .sort(compareArtifactCandidates);

  for (const candidate of candidates) {
    const result = await readValidatedSnapshotCandidate({
      db,
      candidate,
      normalize: normalizeHomepageArtifactBodyJson,
      cacheByDb: normalizedHomepageArtifactCacheByDb,
    });
    if (result.row) {
      return result.row.generatedAt;
    }
  }

  return null;
}

export async function readHomepageRefreshBaseSnapshot(
  db: D1Database,
  now: number,
): Promise<{
  generatedAt: number | null;
  bodyJson: string | null;
  seedDataSnapshot: boolean;
}> {
  const refreshRows = await readRefreshSnapshotRows(db);
  const candidates = refreshRows
    .map((row) => ({
      key: row.key,
      generatedAt: row.generated_at,
      updatedAt: toSnapshotUpdatedAt(row),
    }))
    .sort(comparePayloadCandidates);
  const rowByKey = new Map(refreshRows.map((row) => [row.key, row]));
  const readRefreshCandidate = async (
    candidateList: readonly SnapshotCandidate[],
  ): Promise<{ row: NormalizedSnapshotRow | null; invalid: boolean }> => {
    let invalid = false;

    for (const candidate of candidateList) {
      const cached = readCachedNormalizedSnapshotRow(
        normalizedHomepagePayloadCacheByDb,
        db,
        candidate,
      );
      if (cached) {
        return { row: cached, invalid };
      }

      const row = rowByKey.get(candidate.key);
      if (!row || row.generated_at !== candidate.generatedAt) {
        continue;
      }

      const bodyJson = normalizeHomepagePayloadBodyJson(row.body_json);
      if (!bodyJson) {
        invalid = true;
        continue;
      }

      return {
        row: writeCachedNormalizedSnapshotRow(
          normalizedHomepagePayloadCacheByDb,
          db,
          candidate,
          bodyJson,
        ),
        invalid,
      };
    }

    return { row: null, invalid };
  };

  const { row: sameDayBase, invalid: sameDayInvalid } = await readRefreshCandidate(
    candidates.filter((candidate) => isSameUtcDay(candidate.generatedAt, now)),
  );
  if (sameDayBase) {
    return {
      generatedAt: sameDayBase.generatedAt,
      bodyJson: sameDayBase.bodyJson,
      seedDataSnapshot: false,
    };
  }

  const { row: freshestBase, invalid: freshestInvalid } = await readRefreshCandidate(candidates);
  if (freshestBase) {
    return {
      generatedAt: freshestBase.generatedAt,
      bodyJson: freshestBase.bodyJson,
      seedDataSnapshot: true,
    };
  }

  if (candidates.length === 0) {
    return {
      generatedAt: null,
      bodyJson: null,
      seedDataSnapshot: true,
    };
  }

  if (sameDayInvalid || freshestInvalid) {
    console.warn('homepage snapshot: invalid refresh payload');
  }

  return {
    generatedAt: null,
    bodyJson: null,
    seedDataSnapshot: true,
  };
}

export function applyHomepageCacheHeaders(res: Response, ageSeconds: number): void {
  const remaining = Math.max(0, MAX_AGE_SECONDS - ageSeconds);
  const maxAge = Math.min(30, remaining);
  const stale = Math.max(0, remaining - maxAge);

  res.headers.set(
    'Cache-Control',
    `public, max-age=${maxAge}, stale-while-revalidate=${stale}, stale-if-error=${stale}`,
  );
}

export async function readHomepageSnapshotJsonAnyAge(
  db: D1Database,
  now: number,
  maxStaleSeconds = MAX_STALE_SECONDS,
): Promise<{ bodyJson: string; age: number } | null> {
  const candidates = (await listSnapshotCandidates(db))
    .filter((candidate) => Math.max(0, now - candidate.generatedAt) <= maxStaleSeconds)
    .sort(comparePayloadCandidates);

  for (const candidate of candidates) {
    const result = await readValidatedSnapshotCandidate({
      db,
      candidate,
      normalize: normalizeHomepagePayloadBodyJson,
      cacheByDb: normalizedHomepagePayloadCacheByDb,
    });
    if (!result.row) {
      if (result.invalid) {
        console.warn('homepage snapshot: invalid payload');
      }
      continue;
    }

    return {
      bodyJson: result.row.bodyJson,
      age: Math.max(0, now - result.row.generatedAt),
    };
  }

  return null;
}

export async function readHomepageSnapshotJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  return await readHomepageSnapshotJsonAnyAge(db, now, MAX_AGE_SECONDS);
}

export async function readHomepageSnapshotArtifactJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  const candidates = (await listSnapshotCandidates(db))
    .filter((candidate) => Math.max(0, now - candidate.generatedAt) <= MAX_AGE_SECONDS)
    .sort(compareArtifactCandidates);

  for (const candidate of candidates) {
    const result = await readValidatedSnapshotCandidate({
      db,
      candidate,
      normalize: normalizeHomepageArtifactBodyJson,
      cacheByDb: normalizedHomepageArtifactCacheByDb,
    });
    if (!result.row) {
      if (result.invalid) {
        console.warn('homepage snapshot: invalid artifact payload');
      }
      continue;
    }

    return {
      bodyJson: result.row.bodyJson,
      age: Math.max(0, now - result.row.generatedAt),
    };
  }

  return null;
}

export async function readStaleHomepageSnapshotArtifactJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  const candidates = (await listSnapshotCandidates(db))
    .filter((candidate) => Math.max(0, now - candidate.generatedAt) <= MAX_STALE_SECONDS)
    .sort(compareArtifactCandidates);

  for (const candidate of candidates) {
    const result = await readValidatedSnapshotCandidate({
      db,
      candidate,
      normalize: normalizeHomepageArtifactBodyJson,
      cacheByDb: normalizedHomepageArtifactCacheByDb,
    });
    if (!result.row) {
      if (result.invalid) {
        console.warn('homepage snapshot: invalid stale artifact payload');
      }
      continue;
    }

    return {
      bodyJson: result.row.bodyJson,
      age: Math.max(0, now - result.row.generatedAt),
    };
  }

  return null;
}

export function assertHomepageArtifactAvailable(): never {
  throw new AppError(503, 'UNAVAILABLE', 'Homepage artifact unavailable');
}
