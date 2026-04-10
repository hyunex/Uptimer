import { writeFile } from 'node:fs/promises';

import { Hono } from 'hono';
import { describe, expect, it } from 'vitest';

import {
  computePublicHomepageArtifactPayload,
  computePublicHomepagePayload,
} from '../src/public/homepage';
import type { Env } from '../src/env';
import { handleError, handleNotFound } from '../src/middleware/errors';
import { publicRoutes } from '../src/routes/public';
import { buildHomepageRenderArtifact } from '../src/snapshots/public-homepage';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';
import pageWorker from '../../web/public/_worker.js';

type Scenario = {
  name: string;
  monitorCount: number;
  heartbeatPoints: number;
  uptimeDays: number;
};

type Sample = {
  elapsedMs: number;
  monitorCount: number;
  heartbeatRows: number;
  rollupRows: number;
};

type RootMissScenario = {
  name: string;
  monitorCount: number;
};

type RootMissSample = {
  elapsedMs: number;
  artifactKB: number;
  preloadKB: number;
  snapshotKB: number;
};

type RouteReadScenario = {
  name: string;
  endpoint: 'homepage' | 'homepage-artifact';
  monitorCount: number;
};

type RouteReadSample = {
  elapsedMs: number;
  bodyKB: number;
};

type HomepageHotPathScenario = {
  name: string;
  mode: 'direct-homepage-compute' | 'state-snapshot-materialize';
  monitorCount: number;
};

type HomepageHotPathSample = {
  elapsedMs: number;
  bodyKB: number;
};

const BENCH_LABEL = process.env.HOMEPAGE_BENCH_LABEL ?? 'current-working-tree';
const OUTPUT_PATH = process.env.HOMEPAGE_BENCH_OUTPUT ?? null;

const SCENARIOS: Scenario[] = [
  { name: '1000 monitors / 30 heartbeats / 14 uptime days', monitorCount: 1000, heartbeatPoints: 30, uptimeDays: 14 },
  { name: '5000 monitors / 30 heartbeats / 14 uptime days', monitorCount: 5000, heartbeatPoints: 30, uptimeDays: 14 },
];

const ROOT_MISS_SCENARIOS: RootMissScenario[] = [
  { name: '50 monitors', monitorCount: 50 },
  { name: '100 monitors', monitorCount: 100 },
  { name: '250 monitors', monitorCount: 250 },
];

const ROUTE_READ_SCENARIOS: RouteReadScenario[] = [
  { name: 'homepage / 250 monitors', endpoint: 'homepage', monitorCount: 250 },
  { name: 'homepage / 1000 monitors', endpoint: 'homepage', monitorCount: 1000 },
  { name: 'homepage-artifact / 250 monitors', endpoint: 'homepage-artifact', monitorCount: 250 },
  { name: 'homepage-artifact / 1000 monitors', endpoint: 'homepage-artifact', monitorCount: 1000 },
];

const HOMEPAGE_HOT_PATH_SCENARIOS: HomepageHotPathScenario[] = [
  {
    name: 'homepage via state snapshot materialize / 250 monitors',
    mode: 'state-snapshot-materialize',
    monitorCount: 250,
  },
  {
    name: 'homepage via state snapshot materialize / 1000 monitors',
    mode: 'state-snapshot-materialize',
    monitorCount: 1000,
  },
  {
    name: 'homepage via direct homepage compute / 250 monitors',
    mode: 'direct-homepage-compute',
    monitorCount: 250,
  },
  {
    name: 'homepage via direct homepage compute / 1000 monitors',
    mode: 'direct-homepage-compute',
    monitorCount: 1000,
  },
];

function parsePositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return fallback;
  }
  return parsed;
}

const WARMUP_RUNS = parsePositiveIntEnv('HOMEPAGE_BENCH_WARMUPS', 3);
const MEASURE_RUNS = parsePositiveIntEnv('HOMEPAGE_BENCH_RUNS', 12);

function serializePublicMonitorCache(cache: {
  heartbeat: {
    checked_at: number[];
    status_codes: string;
    latency_ms: Array<number | null>;
  };
  uptime_days: {
    day_start_at: number[];
    total_sec: number[];
    downtime_sec: number[];
    unknown_sec: number[];
    uptime_sec: number[];
  };
}) {
  return JSON.stringify(cache);
}

function serializeHomepageState(state: {
  generated_at: number;
  monitor_count_total: number;
  site_title: string;
  site_description: string;
  site_locale: string;
  site_timezone: string;
  uptime_rating_level: number;
  monitors: Array<{
    id: number;
    name: string;
    type: 'http' | 'tcp';
    group_name: string | null;
    interval_sec: number;
    created_at: number;
    state_status: string;
    last_checked_at: number | null;
    covered_until_at: number;
    cache: {
      heartbeat: {
        checked_at: number[];
        status_codes: string;
        latency_ms: Array<number | null>;
      };
      uptime_days: {
        day_start_at: number[];
        total_sec: number[];
        downtime_sec: number[];
        unknown_sec: number[];
        uptime_sec: number[];
      };
    };
  }>;
  resolved_incident_preview: null;
  maintenance_history_preview: null;
}) {
  return JSON.stringify({
    v: 1,
    g: state.generated_at,
    c: state.monitor_count_total,
    t: state.site_title,
    d: state.site_description,
    l: state.site_locale,
    z: state.site_timezone,
    r: state.uptime_rating_level,
    m: state.monitors.map((monitor) => [
      monitor.id,
      monitor.name,
      monitor.type === 'tcp' ? 't' : 'h',
      monitor.group_name ?? '',
      monitor.interval_sec,
      monitor.created_at,
      monitor.state_status === 'up'
        ? 'u'
        : monitor.state_status === 'down'
          ? 'd'
          : monitor.state_status === 'maintenance'
            ? 'm'
            : monitor.state_status === 'paused'
              ? 'p'
              : 'x',
      monitor.last_checked_at ?? 0,
      monitor.covered_until_at,
      monitor.cache.heartbeat.checked_at,
      monitor.cache.heartbeat.status_codes,
      monitor.cache.heartbeat.latency_ms,
      monitor.cache.uptime_days.day_start_at,
      monitor.cache.uptime_days.total_sec,
      monitor.cache.uptime_days.downtime_sec,
      monitor.cache.uptime_days.unknown_sec,
      monitor.cache.uptime_days.uptime_sec,
    ]),
    i: state.resolved_incident_preview,
    w: state.maintenance_history_preview,
  });
}

const scenarioCache = new Map<string, ReturnType<typeof buildScenarioRows>>();

function buildScenarioRows(scenario: Scenario, now: number) {
  const monitorIds = Array.from({ length: scenario.monitorCount }, (_, index) => index + 1);

  const heartbeats = monitorIds.flatMap((monitorId) =>
    Array.from({ length: scenario.heartbeatPoints }, (_, index) => ({
      monitor_id: monitorId,
      checked_at: now - (index + 1) * 60,
      status: 'up',
      latency_ms: 40 + ((monitorId + index) % 50),
    })),
  );

  const rollups = monitorIds.flatMap((monitorId) =>
    Array.from({ length: scenario.uptimeDays }, (_, index) => ({
      monitor_id: monitorId,
      day_start_at: now - (scenario.uptimeDays - index) * 86_400,
      total_sec: 86_400,
      downtime_sec: 0,
      unknown_sec: 0,
      uptime_sec: 86_400,
    })),
  );

  const heartbeatsByMonitorId = new Map<number, typeof heartbeats>();
  for (const row of heartbeats) {
    const existing = heartbeatsByMonitorId.get(row.monitor_id);
    if (existing) {
      existing.push(row);
      continue;
    }
    heartbeatsByMonitorId.set(row.monitor_id, [row]);
  }

  const rollupsByMonitorId = new Map<number, typeof rollups>();
  for (const row of rollups) {
    const existing = rollupsByMonitorId.get(row.monitor_id);
    if (existing) {
      existing.push(row);
      continue;
    }
    rollupsByMonitorId.set(row.monitor_id, [row]);
  }

  const monitors = monitorIds.map((id, index) => ({
    id,
    name: `Monitor ${id}`,
    type: 'http',
    group_name: index % 2 === 0 ? 'Core' : 'Edge',
    group_sort_order: index % 2,
    sort_order: index,
    interval_sec: 60,
    created_at: now - 40 * 86_400,
    state_status: 'up',
    last_checked_at: now - 30,
    last_latency_ms: 40 + (index % 50),
    public_cache_json: serializePublicMonitorCache({
      heartbeat: {
        checked_at: (heartbeatsByMonitorId.get(id) ?? []).map((row) => row.checked_at),
        status_codes: 'u'.repeat(scenario.heartbeatPoints),
        latency_ms: (heartbeatsByMonitorId.get(id) ?? []).map((row) => row.latency_ms),
      },
      uptime_days: {
        day_start_at: (rollupsByMonitorId.get(id) ?? []).map((row) => row.day_start_at),
        total_sec: (rollupsByMonitorId.get(id) ?? []).map((row) => row.total_sec),
        downtime_sec: (rollupsByMonitorId.get(id) ?? []).map((row) => row.downtime_sec),
        unknown_sec: (rollupsByMonitorId.get(id) ?? []).map((row) => row.unknown_sec),
        uptime_sec: (rollupsByMonitorId.get(id) ?? []).map((row) => row.uptime_sec),
      },
    }),
  }));

  return { monitors, heartbeats, rollups };
}

function getScenarioRows(scenario: Scenario, now: number) {
  const key = `${scenario.name}:${now}`;
  const cached = scenarioCache.get(key);
  if (cached) return cached;

  const built = buildScenarioRows(scenario, now);
  scenarioCache.set(key, built);
  return built;
}

function buildHomepageStateSnapshotJson(scenario: Scenario, now: number): string {
  const rows = getScenarioRows(scenario, now);
  const heartbeatRowsByMonitorId = new Map<number, typeof rows.heartbeats>();
  for (const row of rows.heartbeats) {
    const existing = heartbeatRowsByMonitorId.get(row.monitor_id);
    if (existing) {
      existing.push(row);
      continue;
    }
    heartbeatRowsByMonitorId.set(row.monitor_id, [row]);
  }

  const rollupRowsByMonitorId = new Map<number, typeof rows.rollups>();
  for (const row of rows.rollups) {
    const existing = rollupRowsByMonitorId.get(row.monitor_id);
    if (existing) {
      existing.push(row);
      continue;
    }
    rollupRowsByMonitorId.set(row.monitor_id, [row]);
  }

  return serializeHomepageState({
    generated_at: now - 60,
    monitor_count_total: rows.monitors.length,
    site_title: 'Status Hub',
    site_description: 'Production services',
    site_locale: 'en',
    site_timezone: 'UTC',
    uptime_rating_level: 3,
    monitors: rows.monitors.map((row) => ({
      id: row.id,
      name: row.name,
      type: row.type === 'tcp' ? 'tcp' : 'http',
      group_name: row.group_name,
      interval_sec: row.interval_sec,
      created_at: row.created_at,
      state_status: row.state_status,
      last_checked_at: row.last_checked_at,
      covered_until_at: now - 60,
      cache: JSON.parse(
        serializePublicMonitorCache({
          heartbeat: {
            checked_at: (heartbeatRowsByMonitorId.get(row.id) ?? []).map(
              (heartbeatRow) => heartbeatRow.checked_at,
            ),
            status_codes: 'u'.repeat(scenario.heartbeatPoints),
            latency_ms: (heartbeatRowsByMonitorId.get(row.id) ?? []).map(
              (heartbeatRow) => heartbeatRow.latency_ms,
            ),
          },
          uptime_days: {
            day_start_at: (rollupRowsByMonitorId.get(row.id) ?? []).map(
              (rollupRow) => rollupRow.day_start_at,
            ),
            total_sec: (rollupRowsByMonitorId.get(row.id) ?? []).map(
              (rollupRow) => rollupRow.total_sec,
            ),
            downtime_sec: (rollupRowsByMonitorId.get(row.id) ?? []).map(
              (rollupRow) => rollupRow.downtime_sec,
            ),
            unknown_sec: (rollupRowsByMonitorId.get(row.id) ?? []).map(
              (rollupRow) => rollupRow.unknown_sec,
            ),
            uptime_sec: (rollupRowsByMonitorId.get(row.id) ?? []).map(
              (rollupRow) => rollupRow.uptime_sec,
            ),
          },
        }),
      ),
    })),
    resolved_incident_preview: null,
    maintenance_history_preview: null,
  });
}

function createHandlersForScenario(scenario: Scenario, now: number): {
  handlers: FakeD1QueryHandler[];
  rowCounts: {
    monitorCount: number;
    heartbeatRows: number;
    rollupRows: number;
  };
} {
  const rows = getScenarioRows(scenario, now);

  const handlers: FakeD1QueryHandler[] = [
    {
      match: 'from monitors m',
      all: (args, normalizedSql) =>
        normalizedSql.includes('limit ?1')
          ? rows.monitors.slice(0, Number(args[0] ?? rows.monitors.length))
          : rows.monitors,
    },
    {
      match: 'with active_maintenance',
      first: () => ({
        monitor_count_total: rows.monitors.length,
        up: rows.monitors.length,
        down: 0,
        maintenance: 0,
        paused: 0,
        unknown: 0,
      }),
    },
    {
      match: 'select distinct mwm.monitor_id',
      all: () => [],
    },
    {
      match: (sql) => sql.startsWith('select value from settings where key = ?1'),
      first: () => ({ value: '3' }),
    },
    {
      match: 'row_number() over',
      all: () => rows.heartbeats,
    },
    {
      match: 'select monitor_id, checked_at, status from check_results',
      all: () =>
        rows.heartbeats.map((row) => ({
          monitor_id: row.monitor_id,
          checked_at: row.checked_at,
          status: row.status,
        })),
    },
    {
      match: 'from monitor_daily_rollups',
      all: () => rows.rollups,
    },
    {
      match: 'from outages',
      all: () => [],
    },
    {
      match: (sql) => sql.startsWith('select key, value from settings'),
      all: () => [
        { key: 'site_title', value: 'Status Hub' },
        { key: 'site_description', value: 'Production services' },
        { key: 'site_locale', value: 'en' },
        { key: 'site_timezone', value: 'UTC' },
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

  return {
    handlers,
    rowCounts: {
      monitorCount: rows.monitors.length,
      heartbeatRows: rows.heartbeats.length,
      rollupRows: rows.rollups.length,
    },
  };
}

function createDbForScenario(scenario: Scenario, now: number) {
  const { handlers, rowCounts } = createHandlersForScenario(scenario, now);
  return {
    db: createFakeD1Database(handlers),
    rowCounts,
  };
}

async function runOne(scenario: Scenario): Promise<Sample> {
  const now = 1_728_000_000;
  const { db, rowCounts } = createDbForScenario(scenario, now);

  const started = performance.now();
  const payload = await computePublicHomepagePayload(db, now);
  const elapsedMs = performance.now() - started;

  expect(payload.monitors).toHaveLength(scenario.monitorCount);

  return {
    elapsedMs,
    ...rowCounts,
  };
}

async function runOneArtifactCompute(scenario: Scenario): Promise<Sample> {
  const now = 1_728_000_000;
  const { db, rowCounts } = createDbForScenario(scenario, now);

  const started = performance.now();
  const payload = await computePublicHomepageArtifactPayload(db, now);
  const elapsedMs = performance.now() - started;

  expect(payload.monitors.length).toBeLessThanOrEqual(12);
  expect(payload.monitor_count_total).toBe(scenario.monitorCount);

  return {
    elapsedMs,
    ...rowCounts,
  };
}

function percentile(sorted: number[], ratio: number): number {
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
  return sorted[index] ?? 0;
}

function summarize(scenario: Scenario, samples: Sample[]) {
  const elapsed = samples.map((sample) => sample.elapsedMs).sort((a, b) => a - b);
  const totalElapsed = elapsed.reduce((sum, value) => sum + value, 0);
  const first = samples[0];

  return {
    scenario: scenario.name,
    runs: samples.length,
    meanMs: Number((totalElapsed / samples.length).toFixed(3)),
    medianMs: Number(percentile(elapsed, 0.5).toFixed(3)),
    p95Ms: Number(percentile(elapsed, 0.95).toFixed(3)),
    monitorCount: first?.monitorCount ?? 0,
    heartbeatRows: first?.heartbeatRows ?? 0,
    rollupRows: first?.rollupRows ?? 0,
  };
}

function buildSyntheticHomepagePayload(
  monitorCount: number,
  heartbeatPoints: number,
  uptimeDays: number,
  now: number,
) {
  return {
    generated_at: now,
    bootstrap_mode: 'full' as const,
    monitor_count_total: monitorCount,
    site_title: 'Status Hub',
    site_description: 'Production services',
    site_locale: 'en' as const,
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
      up: monitorCount,
      down: 0,
      maintenance: 0,
      paused: 0,
      unknown: 0,
    },
    monitors: Array.from({ length: monitorCount }, (_, monitorIndex) => ({
      id: monitorIndex + 1,
      name: `Monitor ${monitorIndex + 1}`,
      type: 'http' as const,
      group_name: monitorIndex % 2 === 0 ? 'Core' : 'Edge',
      status: 'up' as const,
      is_stale: false,
      last_checked_at: now - 30,
      heartbeat_strip: {
        checked_at: Array.from({ length: heartbeatPoints }, (_, pointIndex) => now - (pointIndex + 1) * 60),
        status_codes: 'u'.repeat(heartbeatPoints),
        latency_ms: Array.from(
          { length: heartbeatPoints },
          (_, pointIndex) => 40 + ((monitorIndex + pointIndex) % 50),
        ),
      },
      uptime_30d: { uptime_pct: 100 },
      uptime_day_strip: {
        day_start_at: Array.from(
          { length: uptimeDays },
          (_, dayIndex) => now - (uptimeDays - dayIndex) * 86_400,
        ),
        downtime_sec: Array.from({ length: uptimeDays }, () => 0),
        unknown_sec: Array.from({ length: uptimeDays }, () => 0),
        uptime_pct_milli: Array.from({ length: uptimeDays }, () => 100_000),
      },
    })),
    active_incidents: [],
    maintenance_windows: {
      active: [],
      upcoming: [],
    },
    resolved_incident_preview: null,
    maintenance_history_preview: null,
  };
}

async function runOneRootMiss(scenario: RootMissScenario): Promise<RootMissSample> {
  const now = 1_728_000_000;
  const artifact = buildHomepageRenderArtifact(
    buildSyntheticHomepagePayload(scenario.monitorCount, 30, 14, now),
  );

  const originalFetch = globalThis.fetch;
  const originalCaches = globalThis.caches;

  Object.defineProperty(globalThis, 'caches', {
    configurable: true,
    value: {
      default: {
        match: async () => null,
        put: async () => undefined,
      },
    },
  });

  globalThis.fetch = (async () =>
    new Response(JSON.stringify(artifact), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })) as typeof fetch;

  try {
    const started = performance.now();
    const response = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      {
        UPTIMER_API_ORIGIN: 'https://api.example.com',
        ASSETS: {
          fetch: async () =>
            new Response('<!doctype html><html><head><title>Uptimer</title></head><body><div id="root"></div></body></html>', {
              status: 200,
              headers: { 'Content-Type': 'text/html; charset=utf-8' },
            }),
        },
      },
      { waitUntil: () => undefined } as ExecutionContext,
    );
    await response.text();
    const elapsedMs = performance.now() - started;

    return {
      elapsedMs,
      artifactKB: Number((JSON.stringify(artifact).length / 1024).toFixed(1)),
      preloadKB: Number((artifact.preload_html.length / 1024).toFixed(1)),
      snapshotKB: Number((JSON.stringify(artifact.snapshot).length / 1024).toFixed(1)),
    };
  } finally {
    globalThis.fetch = originalFetch;
    Object.defineProperty(globalThis, 'caches', {
      configurable: true,
      value: originalCaches,
    });
  }
}

function summarizeRootMiss(scenario: RootMissScenario, samples: RootMissSample[]) {
  const elapsed = samples.map((sample) => sample.elapsedMs).sort((a, b) => a - b);
  const totalElapsed = elapsed.reduce((sum, value) => sum + value, 0);
  const first = samples[0];

  return {
    scenario: scenario.name,
    runs: samples.length,
    meanMs: Number((totalElapsed / samples.length).toFixed(3)),
    medianMs: Number(percentile(elapsed, 0.5).toFixed(3)),
    p95Ms: Number(percentile(elapsed, 0.95).toFixed(3)),
    artifactKB: first?.artifactKB ?? 0,
    preloadKB: first?.preloadKB ?? 0,
    snapshotKB: first?.snapshotKB ?? 0,
  };
}

async function runOneRouteRead(scenario: RouteReadScenario): Promise<RouteReadSample> {
  const now = Math.floor(Date.now() / 1000);
  const payload = buildSyntheticHomepagePayload(scenario.monitorCount, 30, 14, now);
  const artifact = buildHomepageRenderArtifact(payload);
  const bodyJson = scenario.endpoint === 'homepage' ? JSON.stringify(payload) : JSON.stringify(artifact);
  const key = scenario.endpoint === 'homepage' ? 'homepage' : 'homepage:artifact';
  const originalCaches = globalThis.caches;

  Object.defineProperty(globalThis, 'caches', {
    configurable: true,
    value: {
      open: async () => ({
        match: async () => undefined,
        put: async () => undefined,
      }),
    },
  });

  try {
    const env = {
      DB: createFakeD1Database([
        {
          match: 'from public_snapshots',
          first: (args) =>
            args[0] === key
              ? {
                  generated_at: now,
                  body_json: bodyJson,
                }
              : null,
        },
        {
          match: 'insert into public_snapshots',
          run: () => ({ meta: { changes: 1 } }),
        },
      ]),
      ADMIN_TOKEN: 'test-admin-token',
    } as unknown as Env;

    const app = new Hono<{ Bindings: Env }>();
    app.onError(handleError);
    app.notFound(handleNotFound);
    app.route('/api/v1/public', publicRoutes);

    const started = performance.now();
    const response = await app.fetch(
      new Request(`https://status.example.com/api/v1/public/${scenario.endpoint}`),
      env,
      { waitUntil: () => undefined } as ExecutionContext,
    );
    const responseBody = await response.text();
    const elapsedMs = performance.now() - started;
    expect(response.ok).toBe(true);

    return {
      elapsedMs,
      bodyKB: Number((responseBody.length / 1024).toFixed(1)),
    };
  } finally {
    Object.defineProperty(globalThis, 'caches', {
      configurable: true,
      value: originalCaches,
    });
  }
}

function summarizeRouteRead(scenario: RouteReadScenario, samples: RouteReadSample[]) {
  const elapsed = samples.map((sample) => sample.elapsedMs).sort((a, b) => a - b);
  const totalElapsed = elapsed.reduce((sum, value) => sum + value, 0);
  const first = samples[0];

  return {
    scenario: scenario.name,
    runs: samples.length,
    meanMs: Number((totalElapsed / samples.length).toFixed(3)),
    medianMs: Number(percentile(elapsed, 0.5).toFixed(3)),
    p95Ms: Number(percentile(elapsed, 0.95).toFixed(3)),
    bodyKB: first?.bodyKB ?? 0,
  };
}

async function runOneHomepageHotPath(
  scenario: HomepageHotPathScenario,
): Promise<HomepageHotPathSample> {
  const now = 1_728_000_000;
  const originalCaches = globalThis.caches;

  Object.defineProperty(globalThis, 'caches', {
    configurable: true,
    value: {
      open: async () => ({
        match: async () => undefined,
        put: async () => undefined,
      }),
    },
  });

  try {
    const artifactPayload = buildSyntheticHomepagePayload(
      Math.min(scenario.monitorCount, 12),
      30,
      14,
      now,
    );
    const artifact = buildHomepageRenderArtifact({
      ...artifactPayload,
      bootstrap_mode: scenario.monitorCount > artifactPayload.monitors.length ? 'partial' : 'full',
      monitor_count_total: scenario.monitorCount,
    });
    const scenarioShape: Scenario = {
      name: scenario.name,
      monitorCount: scenario.monitorCount,
      heartbeatPoints: 30,
      uptimeDays: 14,
    };
    const { handlers } = createHandlersForScenario(scenarioShape, now);
    const homepageStateBodyJson = buildHomepageStateSnapshotJson(scenarioShape, now);
    const liveHandlers = [
      ...handlers,
      {
        match: 'insert into public_snapshots',
        run: () => ({ meta: { changes: 1 } }),
      } satisfies FakeD1QueryHandler,
    ];
    const env = {
      DB: createFakeD1Database([
        {
          match: 'from public_snapshots',
          first: (args) => {
            if (args[0] === 'homepage') return null;
            if (args[0] === 'homepage:artifact') {
              return {
                generated_at: now,
                body_json: JSON.stringify(artifact),
              };
            }
            if (args[0] === 'homepage:state' && scenario.mode === 'state-snapshot-materialize') {
              return {
                generated_at: now - 60,
                body_json: homepageStateBodyJson,
              };
            }
            return null;
          },
        },
        ...liveHandlers,
      ]),
      ADMIN_TOKEN: 'test-admin-token',
    } as unknown as Env;

    const app = new Hono<{ Bindings: Env }>();
    app.onError(handleError);
    app.notFound(handleNotFound);
    app.route('/api/v1/public', publicRoutes);

    const started = performance.now();
    const response = await app.fetch(
      new Request('https://status.example.com/api/v1/public/homepage'),
      env,
      { waitUntil: () => undefined } as ExecutionContext,
    );
    const responseBody = await response.text();
    const elapsedMs = performance.now() - started;
    expect(response.ok).toBe(true);

    return {
      elapsedMs,
      bodyKB: Number((responseBody.length / 1024).toFixed(1)),
    };
  } finally {
    Object.defineProperty(globalThis, 'caches', {
      configurable: true,
      value: originalCaches,
    });
  }
}

function summarizeHomepageHotPath(
  scenario: HomepageHotPathScenario,
  samples: HomepageHotPathSample[],
) {
  const elapsed = samples.map((sample) => sample.elapsedMs).sort((a, b) => a - b);
  const totalElapsed = elapsed.reduce((sum, value) => sum + value, 0);
  const first = samples[0];

  return {
    scenario: scenario.name,
    runs: samples.length,
    meanMs: Number((totalElapsed / samples.length).toFixed(3)),
    medianMs: Number(percentile(elapsed, 0.5).toFixed(3)),
    p95Ms: Number(percentile(elapsed, 0.95).toFixed(3)),
    bodyKB: first?.bodyKB ?? 0,
  };
}

describe('homepage snapshot benchmark', () => {
  it('measures homepage snapshot compute cost', async () => {
    const rows = [];
    const artifactRows = [];
    const rootMissRows = [];
    const routeReadRows = [];
    const hotPathRows = [];

    for (const scenario of SCENARIOS) {
      for (let index = 0; index < WARMUP_RUNS; index += 1) {
        await runOne(scenario);
      }

      const samples: Sample[] = [];
      for (let index = 0; index < MEASURE_RUNS; index += 1) {
        samples.push(await runOne(scenario));
      }

      rows.push(summarize(scenario, samples));
    }

    for (const scenario of SCENARIOS) {
      for (let index = 0; index < WARMUP_RUNS; index += 1) {
        await runOneArtifactCompute(scenario);
      }

      const samples: Sample[] = [];
      for (let index = 0; index < MEASURE_RUNS; index += 1) {
        samples.push(await runOneArtifactCompute(scenario));
      }

      artifactRows.push(summarize(scenario, samples));
    }

    for (const scenario of ROOT_MISS_SCENARIOS) {
      for (let index = 0; index < WARMUP_RUNS; index += 1) {
        await runOneRootMiss(scenario);
      }

      const samples: RootMissSample[] = [];
      for (let index = 0; index < MEASURE_RUNS; index += 1) {
        samples.push(await runOneRootMiss(scenario));
      }

      rootMissRows.push(summarizeRootMiss(scenario, samples));
    }

    for (const scenario of ROUTE_READ_SCENARIOS) {
      for (let index = 0; index < WARMUP_RUNS; index += 1) {
        await runOneRouteRead(scenario);
      }

      const samples: RouteReadSample[] = [];
      for (let index = 0; index < MEASURE_RUNS; index += 1) {
        samples.push(await runOneRouteRead(scenario));
      }

      routeReadRows.push(summarizeRouteRead(scenario, samples));
    }

    for (const scenario of HOMEPAGE_HOT_PATH_SCENARIOS) {
      for (let index = 0; index < WARMUP_RUNS; index += 1) {
        await runOneHomepageHotPath(scenario);
      }

      const samples: HomepageHotPathSample[] = [];
      for (let index = 0; index < MEASURE_RUNS; index += 1) {
        samples.push(await runOneHomepageHotPath(scenario));
      }

      hotPathRows.push(summarizeHomepageHotPath(scenario, samples));
    }

    console.log('Homepage snapshot benchmark');
    console.log(`Label: ${BENCH_LABEL}`);
    if (process.env.HOMEPAGE_BENCH_RUNS || process.env.HOMEPAGE_BENCH_WARMUPS) {
      console.log(
        `Runs: ${process.env.HOMEPAGE_BENCH_RUNS ?? '12'} (warmups: ${process.env.HOMEPAGE_BENCH_WARMUPS ?? '3'})`,
      );
    }
    console.log('');
    console.table(rows);
    console.log('');
    console.log('Homepage artifact bootstrap compute benchmark');
    console.table(artifactRows);
    console.log('');
    console.log('Pages homepage root miss benchmark');
    console.table(rootMissRows);
    console.log('');
    console.log('Worker homepage route read benchmark');
    console.table(routeReadRows);
    console.log('');
    console.log('Worker homepage hot path benchmark');
    console.table(hotPathRows);

    if (OUTPUT_PATH) {
      await writeFile(
        OUTPUT_PATH,
        JSON.stringify(
          {
            snapshotCompute: rows,
            artifactCompute: artifactRows,
            rootMiss: rootMissRows,
            routeRead: routeReadRows,
            hotPath: hotPathRows,
          },
          null,
          2,
        ),
        'utf8',
      );
      console.log(`Wrote raw benchmark data to ${OUTPUT_PATH}`);
    }
  });
});
