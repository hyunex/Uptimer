import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../src/scheduler/lock', () => ({
  acquireLease: vi.fn(),
  releaseLease: vi.fn(),
}));
vi.mock('../src/public/homepage', () => ({
  computePublicHomepagePayload: vi.fn(),
  tryComputePublicHomepagePayloadFromScheduledRuntimeUpdates: vi.fn(),
}));
vi.mock('../src/public/status-refresh', () => ({
  tryComputePublicStatusPayloadFromScheduledRuntimeUpdates: vi.fn(),
}));
vi.mock('../src/public/uptime-overview', () => ({
  computePublicUptimeOverviewSnapshots: vi.fn(),
}));
vi.mock('../src/snapshots/public-homepage', () => ({
  toHomepageSnapshotPayload: vi.fn((value) => value),
  writeHomepageSnapshot: vi.fn(),
}));
vi.mock('../src/snapshots/public-status', () => ({
  writeStatusSnapshot: vi.fn(),
}));
vi.mock('../src/snapshots/public-uptime-overview', () => ({
  writePublicUptimeOverviewSnapshots: vi.fn(),
}));

import type { Env } from '../src/env';
import worker from '../src/index';
import {
  computePublicHomepagePayload,
  tryComputePublicHomepagePayloadFromScheduledRuntimeUpdates,
} from '../src/public/homepage';
import { tryComputePublicStatusPayloadFromScheduledRuntimeUpdates } from '../src/public/status-refresh';
import { computePublicUptimeOverviewSnapshots } from '../src/public/uptime-overview';
import { acquireLease, releaseLease } from '../src/scheduler/lock';
import { toHomepageSnapshotPayload, writeHomepageSnapshot } from '../src/snapshots/public-homepage';
import { writeStatusSnapshot } from '../src/snapshots/public-status';
import { writePublicUptimeOverviewSnapshots } from '../src/snapshots/public-uptime-overview';
import { createFakeD1Database } from './helpers/fake-d1';

function createBaseSnapshot(now: number) {
  return {
    generated_at: now - 60,
    bootstrap_mode: 'full' as const,
    monitor_count_total: 1,
    site_title: 'Status Hub',
    site_description: 'Production services',
    site_locale: 'en' as const,
    site_timezone: 'UTC',
    uptime_rating_level: 4 as const,
    overall_status: 'up' as const,
    banner: {
      source: 'monitors' as const,
      status: 'operational' as const,
      title: 'All Systems Operational',
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
        group_name: 'Core',
        status: 'up' as const,
        is_stale: false,
        last_checked_at: now - 60,
        heartbeat_strip: {
          checked_at: [now - 60],
          status_codes: 'u',
          latency_ms: [42],
        },
        uptime_30d: { uptime_pct: 100 },
        uptime_day_strip: {
          day_start_at: [Math.floor(now / 86_400) * 86_400],
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

function createEnv(now: number): Env {
  const baseSnapshot = createBaseSnapshot(now);
  return {
    DB: createFakeD1Database([
      {
        match: 'select key, generated_at, updated_at, body_json from public_snapshots',
        all: () => [
          {
            key: 'homepage',
            generated_at: baseSnapshot.generated_at,
            updated_at: baseSnapshot.generated_at,
            body_json: JSON.stringify(baseSnapshot),
          },
        ],
      },
    ]),
    ADMIN_TOKEN: 'test-admin-token',
  } as unknown as Env;
}

describe('internal homepage refresh route', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.mocked(acquireLease).mockResolvedValue(true);
    vi.mocked(releaseLease).mockResolvedValue(undefined);
    vi.mocked(tryComputePublicStatusPayloadFromScheduledRuntimeUpdates).mockResolvedValue(
      null as never,
    );
    vi.mocked(computePublicUptimeOverviewSnapshots).mockResolvedValue(null as never);
  });

  it('uses the scheduled runtime fast path when available', async () => {
    const now = 1_776_230_340;
    vi.spyOn(Date, 'now').mockReturnValue(now * 1000);
    const env = createEnv(now);
    const baseSnapshot = createBaseSnapshot(now);
    const fastPayload = {
      ...baseSnapshot,
      generated_at: now,
      monitors: [
        {
          ...baseSnapshot.monitors[0]!,
          last_checked_at: now,
          heartbeat_strip: {
            checked_at: [now],
            status_codes: 'u',
            latency_ms: [55],
          },
        },
      ],
    };
    vi.mocked(tryComputePublicHomepagePayloadFromScheduledRuntimeUpdates).mockResolvedValue(
      fastPayload as never,
    );

    const res = await worker.fetch(
      new Request('https://status.example.com/api/v1/internal/refresh/homepage', {
        method: 'POST',
        headers: {
          Authorization: 'Bearer test-admin-token',
          'Content-Type': 'application/json; charset=utf-8',
          'X-Uptimer-Refresh-Source': 'scheduled',
        },
        body: JSON.stringify({
          token: 'test-admin-token',
          runtime_updates: [[1, 60, now - 300, now, 'up', 'up', 55]],
        }),
      }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(200);
    expect(tryComputePublicHomepagePayloadFromScheduledRuntimeUpdates).toHaveBeenCalledTimes(1);
    expect(computePublicHomepagePayload).not.toHaveBeenCalled();
    expect(toHomepageSnapshotPayload).not.toHaveBeenCalled();
    expect(writeHomepageSnapshot).toHaveBeenCalledWith(env.DB, now, fastPayload, undefined, false);
    expect(tryComputePublicStatusPayloadFromScheduledRuntimeUpdates).toHaveBeenCalledWith({
      db: env.DB,
      now,
      updates: [
        {
          monitor_id: 1,
          interval_sec: 60,
          created_at: now - 300,
          checked_at: now,
          check_status: 'up',
          next_status: 'up',
          latency_ms: 55,
        },
      ],
    });
    expect(writeStatusSnapshot).not.toHaveBeenCalled();
    expect(writePublicUptimeOverviewSnapshots).not.toHaveBeenCalled();
    expect(releaseLease).toHaveBeenCalledWith(env.DB, 'snapshot:homepage:refresh', now + 55);
    expect(
      vi.mocked(tryComputePublicHomepagePayloadFromScheduledRuntimeUpdates),
    ).toHaveBeenCalledWith({
      db: env.DB,
      now,
      baseSnapshot,
      baseSnapshotBodyJson: null,
      updates: [
        {
          monitor_id: 1,
          interval_sec: 60,
          created_at: now - 300,
          checked_at: now,
          check_status: 'up',
          next_status: 'up',
          latency_ms: 55,
        },
      ],
    });
  });

  it('writes a patched status snapshot when the scheduled fast path can produce one', async () => {
    const now = 1_776_230_340;
    vi.spyOn(Date, 'now').mockReturnValue(now * 1000);
    const env = createEnv(now);
    const baseSnapshot = createBaseSnapshot(now);
    vi.mocked(tryComputePublicHomepagePayloadFromScheduledRuntimeUpdates).mockResolvedValue(
      {
        ...baseSnapshot,
        generated_at: now,
      } as never,
    );
    const statusPayload = {
      generated_at: now,
      site_title: 'Status Hub',
      site_description: 'Production services',
      site_locale: 'en' as const,
      site_timezone: 'UTC',
      uptime_rating_level: 4 as const,
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
      monitors: [],
      active_incidents: [],
      maintenance_windows: {
        active: [],
        upcoming: [],
      },
    };
    vi.mocked(tryComputePublicStatusPayloadFromScheduledRuntimeUpdates).mockResolvedValue(
      statusPayload as never,
    );

    const res = await worker.fetch(
      new Request('https://status.example.com/api/v1/internal/refresh/homepage', {
        method: 'POST',
        headers: {
          Authorization: 'Bearer test-admin-token',
          'Content-Type': 'application/json; charset=utf-8',
          'X-Uptimer-Refresh-Source': 'scheduled',
        },
        body: JSON.stringify({
          token: 'test-admin-token',
          runtime_updates: [[1, 60, now - 300, now, 'up', 'up', 55]],
        }),
      }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(200);
    expect(writeStatusSnapshot).toHaveBeenCalledWith(env.DB, now, statusPayload);
  });

  it('writes uptime overview snapshots when refresh data is available', async () => {
    const now = 1_776_230_340;
    vi.spyOn(Date, 'now').mockReturnValue(now * 1000);
    const env = createEnv(now);
    const baseSnapshot = createBaseSnapshot(now);
    vi.mocked(tryComputePublicHomepagePayloadFromScheduledRuntimeUpdates).mockResolvedValue(
      {
        ...baseSnapshot,
        generated_at: now,
      } as never,
    );
    vi.mocked(computePublicUptimeOverviewSnapshots).mockResolvedValue(
      {
        '30d': {
          generated_at: now,
          range: '30d',
          range_start_at: now - 30 * 86_400,
          range_end_at: now,
          overall: {
            total_sec: 3_600,
            downtime_sec: 0,
            unknown_sec: 0,
            uptime_sec: 3_600,
            uptime_pct: 100,
          },
          monitors: [],
        },
        '90d': {
          generated_at: now,
          range: '90d',
          range_start_at: now - 90 * 86_400,
          range_end_at: now,
          overall: {
            total_sec: 3_600,
            downtime_sec: 0,
            unknown_sec: 0,
            uptime_sec: 3_600,
            uptime_pct: 100,
          },
          monitors: [],
        },
      } as never,
    );

    const res = await worker.fetch(
      new Request('https://status.example.com/api/v1/internal/refresh/homepage', {
        method: 'POST',
        headers: {
          Authorization: 'Bearer test-admin-token',
          'Content-Type': 'application/json; charset=utf-8',
          'X-Uptimer-Refresh-Source': 'scheduled',
        },
        body: JSON.stringify({
          token: 'test-admin-token',
          runtime_updates: [[1, 60, now - 300, now, 'up', 'up', 55]],
        }),
      }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(200);
    expect(computePublicUptimeOverviewSnapshots).toHaveBeenCalledWith(env.DB, now);
    expect(writePublicUptimeOverviewSnapshots).toHaveBeenCalledWith(
      env.DB,
      now,
      expect.objectContaining({
        '30d': expect.objectContaining({ range: '30d' }),
        '90d': expect.objectContaining({ range: '90d' }),
      }),
    );
  });

  it('normalizes privileged runtime update latency values before fast-path compute', async () => {
    const now = 1_776_230_340;
    vi.spyOn(Date, 'now').mockReturnValue(now * 1000);
    const env = createEnv(now);
    const baseSnapshot = createBaseSnapshot(now);
    vi.mocked(tryComputePublicHomepagePayloadFromScheduledRuntimeUpdates).mockResolvedValue(
      null as never,
    );
    vi.mocked(computePublicHomepagePayload).mockResolvedValue({
      ...baseSnapshot,
      generated_at: now,
    } as never);

    const res = await worker.fetch(
      new Request('https://status.example.com/api/v1/internal/refresh/homepage', {
        method: 'POST',
        headers: {
          Authorization: 'Bearer test-admin-token',
          'Content-Type': 'application/json; charset=utf-8',
          'X-Uptimer-Refresh-Source': 'scheduled',
        },
        body: JSON.stringify({
          token: 'test-admin-token',
          runtime_updates: [[1, 60, now - 300, now, 'up', 'up', -3.7]],
        }),
      }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(200);
    expect(
      vi.mocked(tryComputePublicHomepagePayloadFromScheduledRuntimeUpdates),
    ).toHaveBeenCalledWith({
      db: env.DB,
      now,
      baseSnapshot,
      baseSnapshotBodyJson: null,
      updates: [
        {
          monitor_id: 1,
          interval_sec: 60,
          created_at: now - 300,
          checked_at: now,
          check_status: 'up',
          next_status: 'up',
          latency_ms: 0,
        },
      ],
    });
  });

  it('falls back to full compute when the scheduled runtime fast path misses', async () => {
    const now = 1_776_230_340;
    vi.spyOn(Date, 'now').mockReturnValue(now * 1000);
    const env = createEnv(now);
    const baseSnapshot = createBaseSnapshot(now);
    const computedPayload = {
      ...baseSnapshot,
      generated_at: now,
    };
    vi.mocked(tryComputePublicHomepagePayloadFromScheduledRuntimeUpdates).mockResolvedValue(
      null as never,
    );
    vi.mocked(computePublicHomepagePayload).mockResolvedValue(computedPayload as never);

    const res = await worker.fetch(
      new Request('https://status.example.com/api/v1/internal/refresh/homepage', {
        method: 'POST',
        headers: {
          Authorization: 'Bearer test-admin-token',
          'Content-Type': 'application/json; charset=utf-8',
          'X-Uptimer-Refresh-Source': 'scheduled',
        },
        body: JSON.stringify({
          token: 'test-admin-token',
          runtime_updates: [],
        }),
      }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(200);
    expect(tryComputePublicHomepagePayloadFromScheduledRuntimeUpdates).toHaveBeenCalledTimes(1);
    expect(computePublicHomepagePayload).toHaveBeenCalledWith(env.DB, now, {
      baseSnapshot,
      baseSnapshotBodyJson: null,
    });
    expect(toHomepageSnapshotPayload).not.toHaveBeenCalled();
    expect(writeHomepageSnapshot).toHaveBeenCalledWith(
      env.DB,
      now,
      computedPayload,
      undefined,
      false,
    );
    expect(releaseLease).toHaveBeenCalledWith(env.DB, 'snapshot:homepage:refresh', now + 55);
  });
});
