import { writeFile } from 'node:fs/promises';

import { describe, expect, it, vi } from 'vitest';

vi.mock('../src/monitor/http', () => ({
  runHttpCheck: vi.fn(),
}));
vi.mock('../src/monitor/tcp', () => ({
  runTcpCheck: vi.fn(),
}));

import type { Env } from '../src/env';
import { runScheduledTick } from '../src/scheduler/scheduled';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

type Scenario = {
  name: string;
  monitorCount: number;
  withChannel: boolean;
  recentHomepageAccess?: boolean;
};

type Sample = {
  elapsedMs: number;
  batchCalls: number;
  statementCount: number;
  waitUntilCalls: number;
};

const BENCH_LABEL = process.env.SCHEDULER_BENCH_LABEL ?? 'current-working-tree';
const OUTPUT_PATH = process.env.SCHEDULER_BENCH_OUTPUT ?? null;

const SCENARIOS: Scenario[] = [
  { name: '1000 due monitors / no channels', monitorCount: 1000, withChannel: false },
  {
    name: '1000 due monitors / no channels / recent homepage access',
    monitorCount: 1000,
    withChannel: false,
    recentHomepageAccess: true,
  },
  { name: '5000 due monitors / no channels', monitorCount: 5000, withChannel: false },
  { name: '5000 due monitors / 1 webhook channel', monitorCount: 5000, withChannel: true },
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

const WARMUP_RUNS = parsePositiveIntEnv('SCHEDULER_BENCH_WARMUPS', 3);
const MEASURE_RUNS = parsePositiveIntEnv('SCHEDULER_BENCH_RUNS', 12);

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

function makeDueRows(count: number) {
  return Array.from({ length: count }, (_, index) => ({
    id: index + 1,
    name: `Monitor ${index + 1}`,
    type: 'unsupported',
    target: `benchmark-target-${index + 1}`,
    show_on_status_page: 1,
    group_name: index % 2 === 0 ? 'Core' : 'Edge',
    interval_sec: 60,
    created_at: 1_700_000_000 - 40 * 86_400,
    last_checked_at: 1_700_000_000 - 60,
    timeout_ms: 5000,
    http_method: null,
    http_headers_json: null,
    http_body: null,
    expected_status_json: null,
    response_keyword: null,
    response_keyword_mode: null,
    response_forbidden_keyword: null,
    response_forbidden_keyword_mode: null,
    state_status: 'up',
    state_last_error: null,
    last_changed_at: 1_700_000_000,
    consecutive_failures: 0,
    consecutive_successes: 3,
    public_cache_json: serializePublicMonitorCache({
      heartbeat: {
        checked_at: Array.from({ length: 30 }, (_, heartbeatIndex) => 1_700_000_000 - (heartbeatIndex + 1) * 60),
        status_codes: 'u'.repeat(30),
        latency_ms: Array.from({ length: 30 }, (_, heartbeatIndex) => 40 + ((index + heartbeatIndex) % 50)),
      },
      uptime_days: {
        day_start_at: Array.from({ length: 14 }, (_, dayIndex) => 1_700_000_000 - (14 - dayIndex) * 86_400),
        total_sec: Array.from({ length: 14 }, () => 86_400),
        downtime_sec: Array.from({ length: 14 }, () => 0),
        unknown_sec: Array.from({ length: 14 }, () => 0),
        uptime_sec: Array.from({ length: 14 }, () => 86_400),
      },
    }),
  }));
}

function buildHomepageStateJson(
  dueRows: ReturnType<typeof makeDueRows>,
  generatedAt: number,
) {
  return serializeHomepageState({
    generated_at: generatedAt,
    monitor_count_total: dueRows.length,
    site_title: 'Status Hub',
    site_description: 'Production services',
    site_locale: 'en',
    site_timezone: 'UTC',
    uptime_rating_level: 3,
    monitors: dueRows.map((row) => ({
      id: row.id,
      name: row.name,
      type: row.type === 'tcp' ? 'tcp' : 'http',
      group_name: row.group_name,
      interval_sec: row.interval_sec,
      created_at: row.created_at,
      state_status: row.state_status,
      last_checked_at: row.last_checked_at,
      covered_until_at: generatedAt,
      cache: JSON.parse(row.public_cache_json),
    })),
    resolved_incident_preview: null,
    maintenance_history_preview: null,
  });
}

function createEnvForScenario(scenario: Scenario): {
  env: Env;
  sampleState: Omit<Sample, 'elapsedMs'>;
} {
  const sampleState = {
    batchCalls: 0,
    statementCount: 0,
    waitUntilCalls: 0,
  };
  const dueRows = makeDueRows(scenario.monitorCount);
  let homepageGeneratedAt = scenario.recentHomepageAccess ? 1_700_000_000 - 60 : 0;
  let homepageArtifactGeneratedAt = scenario.recentHomepageAccess ? 1_700_000_000 - 60 : 0;
  let homepageStateGeneratedAt = scenario.recentHomepageAccess ? 1_700_000_000 - 60 : 0;
  let homepageStateBodyJson = scenario.recentHomepageAccess
    ? buildHomepageStateJson(dueRows, homepageStateGeneratedAt)
    : '';
  const channels = scenario.withChannel
    ? [
        {
          id: 1,
          name: 'primary',
          config_json: JSON.stringify({
            url: 'https://hooks.example.com/uptimer',
            method: 'POST',
            payload_type: 'json',
          }),
          created_at: 1_700_000_000,
        },
      ]
    : [];

  const handlers: FakeD1QueryHandler[] = [
    {
      match: 'insert into locks',
      run: () => ({ meta: { changes: 1 } }),
    },
    {
      match: 'from notification_channels',
      all: () => channels,
    },
    {
      match: 'select key, value from settings',
      all: () => [
        { key: 'site_title', value: 'Status Hub' },
        { key: 'site_description', value: 'Production services' },
        { key: 'site_locale', value: 'en' },
        { key: 'site_timezone', value: 'UTC' },
        { key: 'uptime_rating_level', value: '3' },
      ],
    },
    {
      match: 'from monitors m',
      all: (args, normalizedSql) => {
        if (normalizedSql.includes('limit ?1')) {
          return dueRows.slice(0, Number(args[0] ?? dueRows.length));
        }
        return dueRows;
      },
    },
    {
      match: 'select distinct mwm.monitor_id',
      all: () => [],
    },
    {
      match: 'from maintenance_windows',
      all: () => [],
    },
    {
      match: 'from incidents',
      all: () => [],
    },
    {
      match: 'from maintenance_window_monitors',
      all: () => [],
    },
    {
      match: 'with active_maintenance',
      first: () => ({
        monitor_count_total: dueRows.length,
        up: dueRows.length,
        down: 0,
        maintenance: 0,
        paused: 0,
        unknown: 0,
      }),
    },
    {
      match: 'row_number() over',
      all: () =>
        dueRows.slice(0, 12).flatMap((row) =>
          Array.from({ length: 30 }, (_, index) => ({
            monitor_id: row.id,
            checked_at: 1_700_000_000 - (index + 1) * 60,
            status: 'up',
            latency_ms: 40 + ((row.id + index) % 50),
          })),
        ),
    },
    {
      match: 'from monitor_daily_rollups',
      all: () =>
        dueRows.slice(0, 12).flatMap((row) =>
          Array.from({ length: 14 }, (_, index) => ({
            monitor_id: row.id,
            day_start_at: 1_700_000_000 - (14 - index) * 86_400,
            total_sec: 86_400,
            downtime_sec: 0,
            unknown_sec: 0,
            uptime_sec: 86_400,
          })),
        ),
    },
    {
      match: 'from public_snapshots',
      first: (args) => {
        if (args[0] === 'homepage:access' && scenario.recentHomepageAccess) {
          return {
            generated_at: 1_700_000_000,
            body_json: '{}',
          };
        }
        if (args[0] === 'homepage' && homepageGeneratedAt > 0) {
          return {
            generated_at: homepageGeneratedAt,
            body_json: '{"generated_at":0}',
          };
        }
        if (args[0] === 'homepage:artifact' && homepageArtifactGeneratedAt > 0) {
          return {
            generated_at: homepageArtifactGeneratedAt,
            body_json: '{"generated_at":0}',
          };
        }
        if (args[0] === 'homepage:state' && homepageStateGeneratedAt > 0) {
          return {
            generated_at: homepageStateGeneratedAt,
            body_json: homepageStateBodyJson,
          };
        }
        return null;
      },
    },
    {
      match: 'insert into check_results',
      run: () => ({ meta: { changes: 1 } }),
    },
    {
      match: 'insert into monitor_state',
      run: () => ({ meta: { changes: 1 } }),
    },
    {
      match: 'into outages',
      run: () => ({ meta: { changes: 1 } }),
    },
    {
      match: 'update outages',
      run: () => ({ meta: { changes: 1 } }),
    },
    {
      match: 'insert into public_snapshots',
      run: (args) => {
        if (args[0] === 'homepage') {
          homepageGeneratedAt = Number(args[1]);
        }
        if (args[0] === 'homepage:artifact') {
          homepageArtifactGeneratedAt = Number(args[1]);
        }
        if (args[0] === 'homepage:state') {
          homepageStateGeneratedAt = Number(args[1]);
          homepageStateBodyJson = String(args[2]);
        }
        return { meta: { changes: 1 } };
      },
    },
  ];

  const db = createFakeD1Database(handlers);
  const originalBatch = db.batch.bind(db);
  db.batch = async (statements) => {
    sampleState.batchCalls += 1;
    sampleState.statementCount += statements.length;
    return originalBatch(statements);
  };

  return {
    env: { DB: db } as unknown as Env,
    sampleState,
  };
}

async function runOne(scenario: Scenario): Promise<Sample> {
  const { env, sampleState } = createEnvForScenario(scenario);
  const waitUntilPromises: Promise<unknown>[] = [];
  const ctx = {
    waitUntil(promise: Promise<unknown>) {
      sampleState.waitUntilCalls += 1;
      waitUntilPromises.push(
        promise.catch(() => undefined),
      );
    },
  } as unknown as ExecutionContext;

  const started = performance.now();
  await runScheduledTick(env, ctx);
  await Promise.all(waitUntilPromises);
  const elapsedMs = performance.now() - started;

  return {
    elapsedMs,
    ...sampleState,
  };
}

async function withMutedConsole<T>(fn: () => Promise<T>): Promise<T> {
  const originalLog = console.log;
  const originalError = console.error;
  const originalWarn = console.warn;
  console.log = () => undefined;
  console.error = () => undefined;
  console.warn = () => undefined;
  try {
    return await fn();
  } finally {
    console.log = originalLog;
    console.error = originalError;
    console.warn = originalWarn;
  }
}

function percentile(sorted: number[], ratio: number): number {
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
  return sorted[index] ?? 0;
}

function summarize(samples: Sample[]) {
  const elapsed = samples.map((sample) => sample.elapsedMs).sort((a, b) => a - b);
  const batchCalls = samples.map((sample) => sample.batchCalls);
  const statementCounts = samples.map((sample) => sample.statementCount);
  const waitUntilCalls = samples.map((sample) => sample.waitUntilCalls);
  const totalElapsed = elapsed.reduce((sum, value) => sum + value, 0);

  return {
    runs: samples.length,
    meanMs: totalElapsed / samples.length,
    medianMs: percentile(elapsed, 0.5),
    p95Ms: percentile(elapsed, 0.95),
    minMs: elapsed[0] ?? 0,
    maxMs: elapsed.at(-1) ?? 0,
    batchCallsAvg: batchCalls.reduce((sum, value) => sum + value, 0) / batchCalls.length,
    statementCountAvg:
      statementCounts.reduce((sum, value) => sum + value, 0) / statementCounts.length,
    waitUntilCallsAvg: waitUntilCalls.reduce((sum, value) => sum + value, 0) / waitUntilCalls.length,
  };
}

async function benchmarkScenario(scenario: Scenario) {
  for (let index = 0; index < WARMUP_RUNS; index += 1) {
    await runOne(scenario);
  }

  const samples: Sample[] = [];
  for (let index = 0; index < MEASURE_RUNS; index += 1) {
    samples.push(await runOne(scenario));
  }

  return {
    label: BENCH_LABEL,
    scenario: scenario.name,
    monitorCount: scenario.monitorCount,
    withChannel: scenario.withChannel,
    ...summarize(samples),
  };
}

describe('scheduler benchmark', () => {
  it(
    'measures local scheduled tick throughput',
    async () => {
      const rows: Array<Record<string, unknown>> = [];

      await withMutedConsole(async () => {
        for (const scenario of SCENARIOS) {
          rows.push(await benchmarkScenario(scenario));
        }
      });

      const payload = JSON.stringify(rows, null, 2);
      if (OUTPUT_PATH) {
        await writeFile(OUTPUT_PATH, payload, 'utf8');
      } else {
        console.log(payload);
      }

      expect(rows).toHaveLength(SCENARIOS.length);
    },
    120_000,
  );
});
