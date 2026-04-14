import { describe, expect, it } from 'vitest';

import { computePublicHomepagePayload } from '../src/public/homepage';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

describe('computePublicHomepagePayload', () => {
  it('builds compact homepage monitor cards with the expected strips and uptime summary', async () => {
    const now = 1_728_000_000;

    const handlers: FakeD1QueryHandler[] = [
      {
        match: 'from monitors m',
        all: () => [
          {
            id: 1,
            name: 'API',
            type: 'http',
            group_name: 'Core',
            group_sort_order: 0,
            sort_order: 0,
            interval_sec: 60,
            created_at: now - 40 * 86_400,
            state_status: 'up',
            last_checked_at: now - 30,
          },
        ],
      },
      {
        match: 'select distinct mwm.monitor_id',
        all: () => [],
      },
      {
        match: 'json_group_array(checked_at)',
        raw: () => [
          [
            1,
            JSON.stringify([now - 60, now - 120]),
            JSON.stringify([42, null]),
            'ud',
          ],
        ],
      },
      {
        match: 'json_group_array(day_start_at)',
        raw: () => [
          [
            1,
            JSON.stringify([now - 2 * 86_400, now - 86_400]),
            JSON.stringify([0, 60]),
            JSON.stringify([0, 0]),
            JSON.stringify([100_000, 99_931]),
            172_800,
            172_740,
          ],
        ],
      },
      {
        match: (sql) => sql.startsWith('select key, value from settings'),
        all: () => [
          { key: 'site_title', value: 'Status Hub' },
          { key: 'site_description', value: 'Production services' },
          { key: 'site_locale', value: 'en' },
          { key: 'site_timezone', value: 'UTC' },
          { key: 'uptime_rating_level', value: '4' },
        ],
      },
      {
        match: 'from incidents',
        all: () => [],
      },
      {
        match: 'from maintenance_windows',
        all: () => [],
      },
    ];

    const payload = await computePublicHomepagePayload(createFakeD1Database(handlers), now);

    expect(payload.generated_at).toBe(now);
    expect(payload.bootstrap_mode).toBe('full');
    expect(payload.monitor_count_total).toBe(1);
    expect(payload.uptime_rating_level).toBe(4);
    expect(payload.summary).toEqual({
      up: 1,
      down: 0,
      maintenance: 0,
      paused: 0,
      unknown: 0,
    });
    expect(payload.banner).toEqual({
      source: 'monitors',
      status: 'operational',
      title: 'All Systems Operational',
    });

    expect(payload.monitors).toHaveLength(1);
    expect(payload.monitors[0]).toMatchObject({
      id: 1,
      name: 'API',
      type: 'http',
      group_name: 'Core',
      status: 'up',
      is_stale: false,
      last_checked_at: now - 30,
      heartbeat_strip: {
        checked_at: [now - 60, now - 120],
        status_codes: 'ud',
        latency_ms: [42, null],
      },
      uptime_day_strip: {
        day_start_at: [now - 2 * 86_400, now - 86_400],
        downtime_sec: [0, 60],
        unknown_sec: [0, 0],
        uptime_pct_milli: [100_000, 99_931],
      },
    });
    expect(payload.monitors[0]?.uptime_30d?.uptime_pct).toBeCloseTo(99.965, 3);
  });

  it('includes today uptime when all monitors are created after UTC day start', async () => {
    const dayStart = 1_728_000_000;
    const now = dayStart + 36_600; // 10h 10m into current UTC day
    const createdAt = now - 600; // created 10m ago

    const handlers: FakeD1QueryHandler[] = [
      {
        match: 'from monitors m',
        all: () => [
          {
            id: 1,
            name: 'API',
            type: 'http',
            group_name: null,
            interval_sec: 60,
            created_at: createdAt,
            state_status: 'up',
            last_checked_at: now - 30,
          },
        ],
      },
      {
        match: 'select distinct mwm.monitor_id',
        all: () => [],
      },
      {
        match: (sql) => sql.startsWith('select key, value from settings'),
        all: () => [
          { key: 'site_title', value: 'Status Hub' },
          { key: 'site_description', value: 'Production services' },
          { key: 'site_locale', value: 'en' },
          { key: 'site_timezone', value: 'UTC' },
          { key: 'uptime_rating_level', value: '4' },
        ],
      },
      {
        match: 'from incidents',
        all: () => [],
      },
      {
        match: 'from maintenance_windows',
        all: () => [],
      },
      {
        match: 'json_group_array(checked_at)',
        raw: () => [[1, JSON.stringify([now - 120]), JSON.stringify([42]), 'u']],
      },
      {
        match: 'json_group_array(day_start_at)',
        raw: () => [],
      },
      {
        match: 'select monitor_id, started_at, ended_at',
        all: () => [],
      },
      {
        match: 'select monitor_id, checked_at, status from check_results',
        all: () => [{ monitor_id: 1, checked_at: now - 120, status: 'up' }],
      },
    ];

    const payload = await computePublicHomepagePayload(createFakeD1Database(handlers), now);

    expect(payload.monitors).toHaveLength(1);
    expect(payload.monitors[0]?.uptime_30d?.uptime_pct).toBeCloseTo(100, 6);
    expect(payload.monitors[0]?.uptime_day_strip).toMatchObject({
      day_start_at: [dayStart],
      downtime_sec: [0],
      unknown_sec: [0],
      uptime_pct_milli: [100_000],
    });
  });
});
