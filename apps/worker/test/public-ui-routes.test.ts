import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { Env } from '../src/env';
import { publicUiRoutes } from '../src/routes/public-ui';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

type CacheStore = Map<string, Response>;

function installCacheMock(store: CacheStore) {
  const open = vi.fn(async () => ({
    async match(request: Request) {
      const cached = store.get(request.url);
      return cached ? cached.clone() : undefined;
    },
    async put(request: Request, response: Response) {
      store.set(request.url, response.clone());
    },
  }));

  Object.defineProperty(globalThis, 'caches', {
    configurable: true,
    value: { open },
  });

  return open;
}

async function requestPublicUi(path: string, handlers: FakeD1QueryHandler[]) {
  const env = {
    DB: createFakeD1Database(handlers),
    ADMIN_TOKEN: 'test-admin-token',
  } as unknown as Env;

  const res = await publicUiRoutes.fetch(
    new Request(`https://status.example.com${path}`),
    env,
    { waitUntil: vi.fn() } as unknown as ExecutionContext,
  );
  const body = (await res.json()) as Record<string, unknown>;
  return { res, body };
}

describe('public ui routes', () => {
  const originalCaches = (globalThis as { caches?: unknown }).caches;

  beforeEach(() => {
    installCacheMock(new Map());
  });

  afterEach(() => {
    if (originalCaches === undefined) {
      delete (globalThis as { caches?: unknown }).caches;
    } else {
      Object.defineProperty(globalThis, 'caches', {
        configurable: true,
        value: originalCaches,
      });
    }
    vi.restoreAllMocks();
  });

  it('keeps monitor uptime totals stable on the fast public-ui route', async () => {
    const rangeEnd = 1_728_000_000;
    const rangeStart = rangeEnd - 86_400;

    vi.spyOn(Date, 'now').mockReturnValue(rangeEnd * 1000 + 15_000);

    const handlers: FakeD1QueryHandler[] = [
      {
        match: (sql) =>
          sql.includes('from monitors m') &&
          sql.includes('left join monitor_state') &&
          !sql.includes('with input'),
        first: () => ({
          id: 12,
          name: 'Legacy Monitor',
          interval_sec: 60,
          created_at: rangeStart - 5 * 86_400,
          last_checked_at: rangeEnd - 60,
        }),
      },
      {
        match: (sql) =>
          sql.includes('select checked_at') &&
          sql.includes('from check_results') &&
          sql.includes('order by checked_at'),
        first: () => null,
      },
      {
        match: (sql) =>
          sql.includes('with input(monitor_id, interval_sec, created_at, last_checked_at) as (') &&
          sql.includes('unknown_overlap'),
        first: () => ({
          start_at: rangeStart,
          total_sec: 86_400,
          downtime_sec: 300,
          unknown_sec: 0,
        }),
      },
    ];

    const { res, body } = await requestPublicUi('/monitors/12/uptime?range=24h', handlers);

    expect(res.status).toBe(200);
    expect(body).toMatchObject({
      monitor: { id: 12, name: 'Legacy Monitor' },
      range_start_at: rangeStart,
      range_end_at: rangeEnd,
      total_sec: 86_400,
      downtime_sec: 300,
      unknown_sec: 0,
      uptime_sec: 86_100,
    });
  });

  it('keeps analytics uptime totals stable on the fast public-ui route', async () => {
    const dayStart = 1_728_000_000;
    const rangeEnd = dayStart + 3_600;

    vi.spyOn(Date, 'now').mockReturnValue(rangeEnd * 1000 + 10_000);

    const handlers: FakeD1QueryHandler[] = [
      {
        match: (sql) => sql.includes('from public_snapshots') && sql.includes("where key = ?1"),
        first: () => ({
          generated_at: rangeEnd + 10,
          updated_at: rangeEnd + 10,
          body_json: JSON.stringify({
            generated_at: rangeEnd + 10,
            range: '30d',
            range_start_at: rangeEnd - 30 * 86_400,
            range_end_at: rangeEnd,
            overall: {
              total_sec: 3_600,
              downtime_sec: 300,
              unknown_sec: 0,
              uptime_sec: 3_300,
              uptime_pct: (3_300 / 3_600) * 100,
            },
            monitors: [
              {
                id: 21,
                name: 'Core API',
                type: 'http',
                total_sec: 3_600,
                downtime_sec: 300,
                unknown_sec: 0,
                uptime_sec: 3_300,
                uptime_pct: (3_300 / 3_600) * 100,
              },
            ],
          }),
        }),
      },
    ];

    const { res, body } = await requestPublicUi('/analytics/uptime?range=30d', handlers);

    expect(res.status).toBe(200);
    expect(body).toMatchObject({
      range_end_at: rangeEnd,
      overall: {
        total_sec: 3_600,
        downtime_sec: 300,
      },
      monitors: [
        {
          id: 21,
          total_sec: 3_600,
          downtime_sec: 300,
        },
      ],
    });
  });
});
