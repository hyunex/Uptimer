import { utcDayStart } from '../analytics/uptime';
import type {
  PublicUptimeOverviewRange,
  PublicUptimeOverviewResponse,
} from '../schemas/public-uptime-overview';
import {
  materializeMonitorRuntimeTotals,
  readPublicMonitorRuntimeTotalsSnapshot,
  totalsSnapshotHasMonitorIds,
  toMonitorRuntimeTotalsEntryMap,
} from './monitor-runtime';
import { monitorVisibilityPredicate } from './visibility';

const UPTIME_OVERVIEW_RANGES = ['30d', '90d'] as const satisfies PublicUptimeOverviewRange[];

const READ_VISIBLE_MONITORS_SQL = `
  SELECT m.id, m.name, m.type
  FROM monitors m
  WHERE m.is_active = 1
    AND ${monitorVisibilityPredicate(false, 'm')}
  ORDER BY m.id
`;

const READ_VISIBLE_ROLLUPS_SQL = `
  SELECT
    r.monitor_id,
    r.day_start_at,
    r.total_sec,
    r.downtime_sec,
    r.unknown_sec,
    r.uptime_sec
  FROM monitor_daily_rollups r
  JOIN monitors m ON m.id = r.monitor_id
  WHERE m.is_active = 1
    AND ${monitorVisibilityPredicate(false, 'm')}
    AND r.day_start_at >= ?1
    AND r.day_start_at < ?2
  ORDER BY r.monitor_id, r.day_start_at
`;

type VisibleMonitorRow = {
  id: number;
  name: string;
  type: 'http' | 'tcp';
};

type DailyRollupRow = {
  monitor_id: number;
  day_start_at: number;
  total_sec: number | null;
  downtime_sec: number | null;
  unknown_sec: number | null;
  uptime_sec: number | null;
};

type UptimeTotals = {
  total_sec: number;
  downtime_sec: number;
  unknown_sec: number;
  uptime_sec: number;
};

type PublicUptimeOverviewSnapshots = Record<PublicUptimeOverviewRange, PublicUptimeOverviewResponse>;

const preparedStatementsByDb = new WeakMap<D1Database, Map<string, D1PreparedStatement>>();

function prepareStatement(db: D1Database, sql: string): D1PreparedStatement {
  let statements = preparedStatementsByDb.get(db);
  if (!statements) {
    statements = new Map<string, D1PreparedStatement>();
    preparedStatementsByDb.set(db, statements);
  }

  const cached = statements.get(sql);
  if (cached) {
    return cached;
  }

  const statement = db.prepare(sql);
  statements.set(sql, statement);
  return statement;
}

function rangeToSeconds(range: PublicUptimeOverviewRange): number {
  return range === '30d' ? 30 * 86_400 : 90 * 86_400;
}

function createEmptyTotals(): UptimeTotals {
  return {
    total_sec: 0,
    downtime_sec: 0,
    unknown_sec: 0,
    uptime_sec: 0,
  };
}

function addTotals(target: UptimeTotals, source: UptimeTotals): void {
  target.total_sec += source.total_sec;
  target.downtime_sec += source.downtime_sec;
  target.unknown_sec += source.unknown_sec;
  target.uptime_sec += source.uptime_sec;
}

function sumTotals(a: UptimeTotals, b: UptimeTotals): UptimeTotals {
  return {
    total_sec: a.total_sec + b.total_sec,
    downtime_sec: a.downtime_sec + b.downtime_sec,
    unknown_sec: a.unknown_sec + b.unknown_sec,
    uptime_sec: a.uptime_sec + b.uptime_sec,
  };
}

function toUptimePct(totalSec: number, uptimeSec: number): number {
  return totalSec === 0 ? 0 : (uptimeSec / totalSec) * 100;
}

export async function computePublicUptimeOverviewSnapshots(
  db: D1Database,
  now: number,
): Promise<PublicUptimeOverviewSnapshots | null> {
  const rangeEnd = Math.floor(now / 60) * 60;
  const rangeEndFullDays = utcDayStart(rangeEnd);
  const rangeStarts = {
    '30d': rangeEnd - rangeToSeconds('30d'),
    '90d': rangeEnd - rangeToSeconds('90d'),
  } satisfies Record<PublicUptimeOverviewRange, number>;

  const { results: monitorRows } = await prepareStatement(db, READ_VISIBLE_MONITORS_SQL).all<VisibleMonitorRow>();
  const monitors = monitorRows ?? [];
  const monitorIds = monitors.map((monitor) => monitor.id);

  const runtimeSnapshot =
    monitorIds.length > 0 ? await readPublicMonitorRuntimeTotalsSnapshot(db, rangeEnd) : null;
  if (
    monitorIds.length > 0 &&
    (!runtimeSnapshot || !totalsSnapshotHasMonitorIds(runtimeSnapshot, monitorIds))
  ) {
    return null;
  }

  const runtimeByMonitorId = runtimeSnapshot ? toMonitorRuntimeTotalsEntryMap(runtimeSnapshot) : null;
  const { results: rollupRows } = await prepareStatement(db, READ_VISIBLE_ROLLUPS_SQL)
    .bind(rangeStarts['90d'], rangeEndFullDays)
    .all<DailyRollupRow>();

  const rollupsByRange = new Map<number, Record<PublicUptimeOverviewRange, UptimeTotals>>();
  for (const row of rollupRows ?? []) {
    let byRange = rollupsByRange.get(row.monitor_id);
    if (!byRange) {
      byRange = {
        '30d': createEmptyTotals(),
        '90d': createEmptyTotals(),
      };
      rollupsByRange.set(row.monitor_id, byRange);
    }

    const totals = {
      total_sec: row.total_sec ?? 0,
      downtime_sec: row.downtime_sec ?? 0,
      unknown_sec: row.unknown_sec ?? 0,
      uptime_sec: row.uptime_sec ?? 0,
    };
    addTotals(byRange['90d'], totals);
    if (row.day_start_at >= rangeStarts['30d']) {
      addTotals(byRange['30d'], totals);
    }
  }

  const snapshots = {} as PublicUptimeOverviewSnapshots;

  for (const range of UPTIME_OVERVIEW_RANGES) {
    const overall = createEmptyTotals();

    snapshots[range] = {
      generated_at: now,
      range,
      range_start_at: rangeStarts[range],
      range_end_at: rangeEnd,
      overall: {
        total_sec: 0,
        downtime_sec: 0,
        unknown_sec: 0,
        uptime_sec: 0,
        uptime_pct: 0,
      },
      monitors: monitors.map((monitor) => {
        const rollupTotals = rollupsByRange.get(monitor.id)?.[range] ?? createEmptyTotals();
        const partialTotals =
          runtimeByMonitorId === null
            ? createEmptyTotals()
            : materializeMonitorRuntimeTotals(runtimeByMonitorId.get(monitor.id)!, rangeEnd);
        const totals = sumTotals(rollupTotals, partialTotals);

        addTotals(overall, totals);

        return {
          id: monitor.id,
          name: monitor.name,
          type: monitor.type,
          total_sec: totals.total_sec,
          downtime_sec: totals.downtime_sec,
          unknown_sec: totals.unknown_sec,
          uptime_sec: totals.uptime_sec,
          uptime_pct: toUptimePct(totals.total_sec, totals.uptime_sec),
        };
      }),
    };

    snapshots[range].overall = {
      ...overall,
      uptime_pct: toUptimePct(overall.total_sec, overall.uptime_sec),
    };
  }

  return snapshots;
}
