import { afterEach, describe, expect, it, vi } from 'vitest';

vi.mock('../src/scheduler/lock', () => ({
  acquireLease: vi.fn(),
}));

import { acquireLease } from '../src/scheduler/lock';
import {
  applyHomepageCacheHeaders,
  buildHomepageRenderArtifact,
  getHomepageSnapshotKey,
  getHomepageSnapshotMaxAgeSeconds,
  getHomepageSnapshotMaxStaleSeconds,
  refreshPublicHomepageArtifactSnapshotIfNeeded,
  readHomepageSnapshot,
  readHomepageSnapshotArtifact,
  readStaleHomepageSnapshot,
  readStaleHomepageSnapshotArtifact,
  refreshPublicHomepageSnapshotIfNeeded,
  toHomepageSnapshotPayload,
  writeHomepageArtifactSnapshot,
  writeHomepageSnapshot,
} from '../src/snapshots/public-homepage';
import {
  readHomepageRefreshBaseSnapshot,
  readHomepageSnapshotJsonAnyAge,
} from '../src/snapshots/public-homepage-read';
import { createFakeD1Database } from './helpers/fake-d1';

function samplePayload(now = 1_728_000_000) {
  return {
    generated_at: now,
    bootstrap_mode: 'full' as const,
    monitor_count_total: 1,
    site_title: 'Uptimer',
    site_description: '',
    site_locale: 'auto' as const,
    site_timezone: 'UTC',
    uptime_rating_level: 3 as const,
    overall_status: 'up' as const,
    banner: {
      source: 'monitors' as const,
      status: 'operational' as const,
      title: 'All Systems Operational',
      down_ratio: null,
    },
    summary: {
      up: 1,
      down: 0,
      maintenance: 0,
      paused: 0,
      unknown: 0,
    },
    monitors: [
      {
        id: 1,
        name: 'API',
        type: 'http' as const,
        group_name: null,
        status: 'up' as const,
        is_stale: false,
        last_checked_at: now - 30,
        heartbeat_strip: {
          checked_at: [now - 60],
          status_codes: 'u',
          latency_ms: [42],
        },
        uptime_30d: { uptime_pct: 100 },
        uptime_day_strip: {
          day_start_at: [Math.max(0, now - 86_400)],
          downtime_sec: [0],
          unknown_sec: [0],
          uptime_pct_milli: [100_000],
        },
      },
    ],
    active_incidents: [],
    maintenance_windows: {
      active: [],
      upcoming: [],
    },
    resolved_incident_preview: null,
    maintenance_history_preview: null,
  };
}

function hydrateStoredRenderArtifact(
  artifact: ReturnType<typeof buildHomepageRenderArtifact>,
) {
  if ('snapshot' in artifact) {
    return artifact;
  }

  const { snapshot_json: _ignoredSnapshotJson, ...rest } = artifact;
  return {
    ...rest,
    snapshot: JSON.parse(artifact.snapshot_json),
  };
}

describe('snapshots/public-homepage', () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it('exposes stable snapshot constants', () => {
    expect(getHomepageSnapshotKey()).toBe('homepage');
    expect(getHomepageSnapshotMaxAgeSeconds()).toBe(60);
    expect(getHomepageSnapshotMaxStaleSeconds()).toBe(600);
  });

  it('reads fresh and bounded-stale homepage snapshots without live compute', async () => {
    const payload = samplePayload(190);
    const storedRender = buildHomepageRenderArtifact(payload);
    const hydratedRender = hydrateStoredRenderArtifact(storedRender);
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: (args) => {
          if (args[0] === 'homepage:artifact') {
            return {
              generated_at: payload.generated_at,
              body_json: JSON.stringify(storedRender),
            };
          }
          return null;
        },
      },
    ]);

    await expect(readHomepageSnapshot(db, 200)).resolves.toEqual({
      data: payload,
      age: 10,
    });
    await expect(readHomepageSnapshotArtifact(db, 200)).resolves.toEqual({
      data: hydratedRender,
      age: 10,
    });
    await expect(readStaleHomepageSnapshot(db, 200)).resolves.toEqual({
      data: payload,
      age: 10,
    });
    await expect(readStaleHomepageSnapshotArtifact(db, 200)).resolves.toEqual({
      data: hydratedRender,
      age: 10,
    });
  });

  it('reads legacy homepage payloads but refuses to synthesize render artifacts on the read path', async () => {
    const { bootstrap_mode: _ignoredMode, monitor_count_total: _ignoredCount, ...legacyPayload } =
      samplePayload(190);
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: legacyPayload.generated_at,
          body_json: JSON.stringify(legacyPayload),
        }),
      },
    ]);

    await expect(readHomepageSnapshot(db, 200)).resolves.toEqual({
      data: samplePayload(190),
      age: 10,
    });
    await expect(readHomepageSnapshotArtifact(db, 200)).resolves.toBeNull();
  });

  it('keeps the full monitor list in render artifacts', () => {
    const payload = {
      ...samplePayload(190),
      monitor_count_total: 30,
      monitors: Array.from({ length: 30 }, (_, index) => ({
        ...samplePayload(190).monitors[0],
        id: index + 1,
        name: `Monitor ${index + 1}`,
      })),
      summary: {
        up: 30,
        down: 0,
        maintenance: 0,
        paused: 0,
        unknown: 0,
      },
      maintenance_history_preview: {
        id: 1,
        title: 'Database patching',
        message: null,
        starts_at: 120,
        ends_at: 180,
        monitor_ids: [30],
      },
    };

    const artifact = buildHomepageRenderArtifact(payload);
    const bootstrapped = hydrateStoredRenderArtifact(artifact).snapshot;
    expect(bootstrapped.bootstrap_mode).toBe('full');
    expect(bootstrapped.monitor_count_total).toBe(30);
    expect(bootstrapped.monitors).toHaveLength(30);
    expect(artifact.preload_html).toContain('Monitor 30');
    expect(artifact.preload_html).toContain('<path d="M');
    expect(artifact.preload_html).not.toContain('<rect ');
    expect(artifact.preload_html).not.toContain('#30');
    expect(artifact.preload_html).not.toContain('more services will appear after the app finishes loading');
  });

  it('returns null when homepage snapshot is too old or invalid', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);

    const staleDb = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: 0,
          body_json: JSON.stringify(samplePayload(0)),
        }),
      },
    ]);
    await expect(readHomepageSnapshot(staleDb, 200)).resolves.toBeNull();
    await expect(readStaleHomepageSnapshot(staleDb, 800)).resolves.toBeNull();

    const invalidDb = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: 190,
          body_json: '{not-json',
        }),
      },
    ]);
    await expect(readHomepageSnapshot(invalidDb, 200)).resolves.toBeNull();
    warn.mockRestore();
  });

  it('writes normalized homepage snapshots with upsert semantics', async () => {
    const boundArgs: unknown[][] = [];
    const db = createFakeD1Database([
      {
        match: 'insert into public_snapshots',
        run: (args) => {
          boundArgs.push(args);
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const payload = samplePayload(280);
    await writeHomepageSnapshot(db, 300, payload);

    const storedRender = buildHomepageRenderArtifact(payload);

    expect(boundArgs).toEqual([
      ['homepage', 280, JSON.stringify(payload), 300],
      ['homepage:artifact', 280, JSON.stringify(storedRender), 300],
    ]);
  });

  it('does not let an older homepage snapshot overwrite a newer one', async () => {
    const rows = new Map<string, { generated_at: number; body_json: string; updated_at: number }>();
    const db = createFakeD1Database([
      {
        match: 'insert into public_snapshots',
        run: (args) => {
          const [key, generatedAt, bodyJson, updatedAt] = args as [string, number, string, number];
          const existing = rows.get(key);
          if (!existing || generatedAt >= existing.generated_at) {
            rows.set(key, {
              generated_at: generatedAt,
              body_json: bodyJson,
              updated_at: updatedAt,
            });
          }
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const olderPayload = samplePayload(280);
    const newerPayload = samplePayload(300);

    await writeHomepageSnapshot(db, 300, newerPayload);
    await writeHomepageSnapshot(db, 320, olderPayload);

    expect(rows.get('homepage:artifact')?.generated_at).toBe(300);
    expect(rows.get('homepage:artifact')?.body_json).toBe(
      JSON.stringify(buildHomepageRenderArtifact(newerPayload)),
    );
  });

  it('continues writing the compact homepage payload row when requested', async () => {
    const boundArgs: unknown[][] = [];
    const db = createFakeD1Database([
      {
        match: 'insert into public_snapshots',
        run: (args) => {
          boundArgs.push(args);
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const payload = samplePayload(280);
    await writeHomepageSnapshot(db, 300, payload, undefined, true);

    expect(boundArgs).toEqual([
      ['homepage', 280, JSON.stringify(payload), 300],
      ['homepage:artifact', 280, JSON.stringify(buildHomepageRenderArtifact(payload)), 300],
    ]);
  });

  it('writes artifact-only homepage snapshots without touching the full payload row', async () => {
    const boundArgs: unknown[][] = [];
    const db = createFakeD1Database([
      {
        match: 'insert into public_snapshots',
        run: (args) => {
          boundArgs.push(args);
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const payload = {
      ...samplePayload(280),
      bootstrap_mode: 'full' as const,
      monitor_count_total: 30,
      monitors: Array.from({ length: 30 }, (_, index) => ({
        ...samplePayload(280).monitors[0],
        id: index + 1,
        name: `Monitor ${index + 1}`,
      })),
    };

    await writeHomepageArtifactSnapshot(db, 300, payload);

    expect(boundArgs).toEqual([
      ['homepage:artifact', 280, JSON.stringify(buildHomepageRenderArtifact(payload)), 300],
    ]);
  });

  it('applies bounded cache headers for homepage payloads', () => {
    const fresh = new Response('ok');
    applyHomepageCacheHeaders(fresh, 10);
    expect(fresh.headers.get('Cache-Control')).toBe(
      'public, max-age=30, stale-while-revalidate=20, stale-if-error=20',
    );

    const stale = new Response('ok');
    applyHomepageCacheHeaders(stale, 120);
    expect(stale.headers.get('Cache-Control')).toBe(
      'public, max-age=0, stale-while-revalidate=0, stale-if-error=0',
    );
  });

  it('validates homepage snapshot payload shape before persistence', () => {
    const payload = samplePayload(123);
    expect(toHomepageSnapshotPayload(payload)).toEqual(payload);
    expect(() => toHomepageSnapshotPayload({ generated_at: 1 })).toThrow();
  });

  it('skips refresh when the homepage snapshot was already generated this minute', async () => {
    const now = 1_728_000_045;
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: 1_728_000_031,
          body_json: JSON.stringify(samplePayload(1_728_000_031)),
        }),
      },
    ]);

    const compute = vi.fn(async () => samplePayload(now));
    const refreshed = await refreshPublicHomepageSnapshotIfNeeded({ db, now, compute });

    expect(refreshed).toBe(false);
    expect(acquireLease).not.toHaveBeenCalled();
    expect(compute).not.toHaveBeenCalled();
  });

  it('refreshes once when the minute changed and a refresh lease is acquired', async () => {
    vi.mocked(acquireLease).mockResolvedValue(true);

    let readCount = 0;
    const writtenArgs: unknown[][] = [];
    const now = 1_728_000_120;
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: (args) => {
          if (args[0] !== 'homepage') {
            return null;
          }
          readCount += 1;
          if (readCount <= 2) {
            return {
              generated_at: 1_728_000_001,
              body_json: JSON.stringify(samplePayload(1_728_000_001)),
            };
          }
          return {
            generated_at: now,
            body_json: JSON.stringify(samplePayload(now)),
          };
        },
      },
      {
        match: 'insert into public_snapshots',
        run: (args) => {
          writtenArgs.push(args);
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const compute = vi.fn(async () => samplePayload(now));
    const refreshed = await refreshPublicHomepageSnapshotIfNeeded({ db, now, compute });
    const storedRender = buildHomepageRenderArtifact(samplePayload(now));

    expect(refreshed).toBe(true);
    expect(acquireLease).toHaveBeenCalledWith(db, 'snapshot:homepage:refresh', now, 55);
    expect(compute).toHaveBeenCalledTimes(1);
    expect(writtenArgs).toEqual([
      ['homepage', now, JSON.stringify(samplePayload(now)), now],
      ['homepage:artifact', now, JSON.stringify(storedRender), now],
    ]);
  });

  it('forces a refresh within the same minute for admin-triggered invalidations', async () => {
    vi.mocked(acquireLease).mockResolvedValue(true);

    const writtenArgs: unknown[][] = [];
    const now = 1_728_000_045;
    const payload = samplePayload(now);
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: now - 5,
          body_json: JSON.stringify(samplePayload(now - 5)),
        }),
      },
      {
        match: 'insert into public_snapshots',
        run: (args) => {
          writtenArgs.push(args);
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const compute = vi.fn(async () => payload);
    const refreshed = await refreshPublicHomepageSnapshotIfNeeded({
      db,
      now,
      compute,
      force: true,
      seedDataSnapshot: true,
    });

    expect(refreshed).toBe(true);
    expect(compute).toHaveBeenCalledTimes(1);
    expect(writtenArgs).toEqual([
      ['homepage', now, JSON.stringify(payload), now],
      ['homepage:artifact', now, JSON.stringify(buildHomepageRenderArtifact(payload)), now],
    ]);
  });

  it('prefers the freshest valid same-day snapshot as the internal refresh base snapshot', async () => {
    const now = 1_728_000_500;
    const payload = samplePayload(now - 300);
    const artifactPayload = samplePayload(now - 60);
    const db = createFakeD1Database([
      {
        match: 'select key, generated_at, updated_at, body_json from public_snapshots',
        all: () => [
          {
            key: 'homepage',
            generated_at: payload.generated_at,
            updated_at: payload.generated_at,
            body_json: JSON.stringify(payload),
          },
          {
            key: 'homepage:artifact',
            generated_at: artifactPayload.generated_at,
            updated_at: artifactPayload.generated_at,
            body_json: JSON.stringify(buildHomepageRenderArtifact(artifactPayload)),
          },
        ],
      },
    ]);

    await expect(readHomepageRefreshBaseSnapshot(db, now)).resolves.toEqual({
      generatedAt: artifactPayload.generated_at,
      snapshot: artifactPayload,
      seedDataSnapshot: false,
    });
  });

  it('keeps using the valid homepage row when an equal-age artifact row is corrupted', async () => {
    const now = 1_728_000_500;
    const payload = samplePayload(now - 60);
    const db = createFakeD1Database([
      {
        match: 'select key, generated_at, updated_at, body_json from public_snapshots',
        all: () => [
          {
            key: 'homepage',
            generated_at: payload.generated_at,
            updated_at: payload.generated_at,
            body_json: JSON.stringify(payload),
          },
          {
            key: 'homepage:artifact',
            generated_at: payload.generated_at,
            updated_at: payload.generated_at,
            body_json:
              '{"generated_at":190,"preload_html":"<div>bad</div>","snapshot":{"generated_at":190',
          },
        ],
      },
    ]);

    await expect(readHomepageRefreshBaseSnapshot(db, now)).resolves.toEqual({
      generatedAt: payload.generated_at,
      snapshot: payload,
      seedDataSnapshot: false,
    });
  });

  it('ignores future-dated refresh snapshot candidates', async () => {
    const now = 1_728_000_500;
    const validPayload = samplePayload(now - 60);
    const futurePayload = samplePayload(now + 600);
    const db = createFakeD1Database([
      {
        match: 'select key, generated_at, updated_at, body_json from public_snapshots',
        all: () => [
          {
            key: 'homepage',
            generated_at: futurePayload.generated_at,
            updated_at: futurePayload.generated_at,
            body_json: JSON.stringify(futurePayload),
          },
          {
            key: 'homepage:artifact',
            generated_at: validPayload.generated_at,
            updated_at: validPayload.generated_at,
            body_json: JSON.stringify(buildHomepageRenderArtifact(validPayload)),
          },
        ],
      },
    ]);

    await expect(readHomepageRefreshBaseSnapshot(db, now)).resolves.toEqual({
      generatedAt: validPayload.generated_at,
      snapshot: validPayload,
      seedDataSnapshot: false,
    });
  });

  it('normalizes artifact rows to the homepage payload shape on the public read path', async () => {
    const now = 1_728_000_200;
    const payload = samplePayload(now - 10);
    const render = buildHomepageRenderArtifact(payload);
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: (args) =>
          args[0] === 'homepage:artifact'
            ? {
                generated_at: payload.generated_at,
                body_json: JSON.stringify(render),
              }
            : null,
      },
    ]);

    await expect(readHomepageSnapshotJsonAnyAge(db, now)).resolves.toEqual({
      bodyJson: JSON.stringify(payload),
      age: 10,
    });
  });

  it('prefers the freshest valid homepage payload row over an older valid artifact row on the hot read path', async () => {
    const now = 1_728_000_200;
    const olderPayload = samplePayload(now - 30);
    const fresherPayload = samplePayload(now - 10);
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: (args) => {
          if (args[0] === 'homepage:artifact') {
            return {
              generated_at: olderPayload.generated_at,
              body_json: JSON.stringify(buildHomepageRenderArtifact(olderPayload)),
            };
          }
          if (args[0] === 'homepage') {
            return {
              generated_at: fresherPayload.generated_at,
              body_json: JSON.stringify(fresherPayload),
            };
          }
          return null;
        },
      },
    ]);

    await expect(readHomepageSnapshotJsonAnyAge(db, now)).resolves.toEqual({
      bodyJson: JSON.stringify(fresherPayload),
      age: 10,
    });
  });

  it('falls back to the homepage payload row when the artifact row is invalid on the hot read path', async () => {
    const now = 1_728_000_200;
    const payload = samplePayload(now - 10);
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: (args) => {
          if (args[0] === 'homepage:artifact') {
            return {
              generated_at: payload.generated_at,
              body_json:
                '{"generated_at":190,"preload_html":"<div>bad</div>","snapshot":{"generated_at":190',
            };
          }
          if (args[0] === 'homepage') {
            return {
              generated_at: payload.generated_at,
              body_json: JSON.stringify(payload),
            };
          }
          return null;
        },
      },
    ]);

    await expect(readHomepageSnapshotJsonAnyAge(db, now)).resolves.toEqual({
      bodyJson: JSON.stringify(payload),
      age: 10,
    });
  });

  it('refreshes only the artifact snapshot when the scheduler path requests it', async () => {
    vi.mocked(acquireLease).mockResolvedValue(true);

    let artifactGeneratedAt = 1_728_000_001;
    const writtenArgs: unknown[][] = [];
    const now = 1_728_000_120;
    const payload = {
      ...samplePayload(now),
      bootstrap_mode: 'full' as const,
      monitor_count_total: 30,
      monitors: Array.from({ length: 30 }, (_, index) => ({
        ...samplePayload(now).monitors[0],
        id: index + 1,
        name: `Monitor ${index + 1}`,
      })),
    };
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: (args) => {
          if (args[0] !== 'homepage:artifact') {
            return null;
          }
          return {
            generated_at: artifactGeneratedAt,
            body_json: JSON.stringify(buildHomepageRenderArtifact(samplePayload(artifactGeneratedAt))),
          };
        },
      },
      {
        match: 'insert into public_snapshots',
        run: (args) => {
          writtenArgs.push(args);
          artifactGeneratedAt = Number(args[1]);
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const compute = vi.fn(async () => payload);
    const refreshed = await refreshPublicHomepageArtifactSnapshotIfNeeded({ db, now, compute });

    expect(refreshed).toBe(true);
    expect(acquireLease).toHaveBeenCalledWith(db, 'snapshot:homepage:refresh', now, 55);
    expect(compute).toHaveBeenCalledTimes(1);
    expect(writtenArgs).toEqual([
      ['homepage:artifact', now, JSON.stringify(buildHomepageRenderArtifact(payload)), now],
    ]);
  });

  it('refreshes the artifact snapshot when the homepage row is fresh but the artifact row is missing', async () => {
    vi.mocked(acquireLease).mockResolvedValue(true);

    const writtenArgs: unknown[][] = [];
    const now = 1_728_000_120;
    const payload = samplePayload(now);
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: (args) => {
          if (args[0] === 'homepage') {
            return {
              generated_at: now - 5,
              body_json: JSON.stringify(samplePayload(now - 5)),
            };
          }
          return null;
        },
      },
      {
        match: 'insert into public_snapshots',
        run: (args) => {
          writtenArgs.push(args);
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const compute = vi.fn(async () => payload);
    const refreshed = await refreshPublicHomepageArtifactSnapshotIfNeeded({ db, now, compute });

    expect(refreshed).toBe(true);
    expect(compute).toHaveBeenCalledTimes(1);
    expect(writtenArgs).toEqual([
      ['homepage:artifact', now, JSON.stringify(buildHomepageRenderArtifact(payload)), now],
    ]);
  });
});
