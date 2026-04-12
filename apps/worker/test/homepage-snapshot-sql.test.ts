import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

import { publicHomepageResponseSchema } from '../src/schemas/public-homepage';
import { __testOnly_homepageDataSnapshotSql } from '../src/snapshots/public-homepage';

const require = createRequire(import.meta.url);
const { DatabaseSync } = require('node:sqlite') as typeof import('node:sqlite');

function applyMigrations(db: DatabaseSync): void {
  const migrationsDir = join(process.cwd(), 'migrations');
  const files = [
    '0001_init.sql',
    '0002_incident_maintenance_scopes.sql',
    '0003_daily_rollups.sql',
    '0004_public_snapshots.sql',
    '0005_uptime_rating_setting.sql',
    '0006_settings_phase12.sql',
    '0007_monitor_group_sort.sql',
    '0008_monitor_group_manual_order.sql',
    '0009_monitor_status_page_visibility.sql',
    '0010_http_response_match_modes.sql',
  ];

  for (const file of files) {
    db.exec(readFileSync(join(migrationsDir, file), 'utf8'));
  }
}

function seedScenario(db: DatabaseSync, now: number): void {
  const createdAt = now - 40 * 86_400;

  db.prepare(
    `
      INSERT INTO monitors (
        id, name, type, target,
        interval_sec, timeout_ms,
        group_name, group_sort_order, sort_order,
        show_on_status_page, is_active,
        created_at, updated_at
      )
      VALUES
        (1, 'API', 'http', 'https://api.example.com', 60, 10000, 'Core', 0, 0, 1, 1, ?1, ?1),
        (2, 'Website', 'http', 'https://example.com', 60, 10000, 'Core', 0, 1, 1, 1, ?1, ?1),
        (3, 'DB', 'tcp', 'db.example.com:5432', 60, 10000, 'Backend', 1, 0, 1, 1, ?1, ?1),
        (4, 'Hidden', 'http', 'https://hidden.example.com', 60, 10000, NULL, 0, 0, 0, 1, ?1, ?1)
    `,
  ).run(createdAt);

  db.prepare(
    `
      INSERT INTO monitor_state (
        monitor_id, status, last_checked_at, last_changed_at, last_latency_ms,
        last_error, consecutive_failures, consecutive_successes
      )
      VALUES
        (1, 'up', ?1, ?1, 120, NULL, 0, 10),
        (2, 'down', ?1, ?1, NULL, 'timeout', 3, 0),
        (3, 'up', ?2, ?2, 80, NULL, 0, 10),
        (4, 'up', ?1, ?1, 50, NULL, 0, 10)
    `,
  ).run(now - 30, now - 10_000);

  for (let index = 1; index <= 5; index += 1) {
    const checkedAt = now - 30 - index * 60;
    db.prepare(
      `
        INSERT INTO check_results (monitor_id, checked_at, status, latency_ms, attempt)
        VALUES (1, ?1, 'up', ?2, 1)
      `,
    ).run(checkedAt, 100 + index);

    db.prepare(
      `
        INSERT INTO check_results (monitor_id, checked_at, status, latency_ms, attempt)
        VALUES (2, ?1, ?2, ?3, 1)
      `,
    ).run(checkedAt, index === 1 ? 'down' : 'up', 200 + index);

    const checkedAtDb = now - 10_000 - index * 60;
    db.prepare(
      `
        INSERT INTO check_results (monitor_id, checked_at, status, latency_ms, attempt)
        VALUES (3, ?1, 'up', ?2, 1)
      `,
    ).run(checkedAtDb, 50 + index);
  }

  const day1 = now - 3 * 86_400;
  const day2 = now - 2 * 86_400;
  db.prepare(
    `
      INSERT INTO monitor_daily_rollups (
        monitor_id, day_start_at, total_sec, downtime_sec, unknown_sec, uptime_sec,
        checks_total, checks_up, checks_down, checks_unknown, checks_maintenance,
        avg_latency_ms, p50_latency_ms, p95_latency_ms, latency_histogram_json,
        created_at, updated_at
      )
      VALUES
        (1, ?1, 86400, 0, 0, 86400, 1440, 1440, 0, 0, 0, 100, 100, 110, '[]', ?3, ?3),
        (1, ?2, 86400, 0, 0, 86400, 1440, 1440, 0, 0, 0, 100, 100, 110, '[]', ?3, ?3),
        (2, ?1, 86400, 3600, 0, 82800, 1440, 1380, 60, 0, 0, 150, 150, 200, '[]', ?3, ?3),
        (2, ?2, 86400, 0, 0, 86400, 1440, 1440, 0, 0, 0, 150, 150, 200, '[]', ?3, ?3),
        (3, ?1, 86400, 0, 0, 86400, 1440, 1440, 0, 0, 0, 50, 50, 60, '[]', ?3, ?3)
    `,
  ).run(day1, day2, now);

  // Active maintenance window (visible)
  db.prepare(
    `
      INSERT INTO maintenance_windows (id, title, message, starts_at, ends_at, created_at)
      VALUES (1, 'Deploy', 'Deploying', ?1, ?2, ?1)
    `,
  ).run(now - 1000, now + 1000);
  db.prepare(
    `
      INSERT INTO maintenance_window_monitors (maintenance_window_id, monitor_id)
      VALUES (1, 2)
    `,
  ).run();

  // Historical maintenance window previews: one visible, one hidden-only
  db.prepare(
    `
      INSERT INTO maintenance_windows (id, title, message, starts_at, ends_at, created_at)
      VALUES (2, 'Past Maint', 'Done', ?1, ?2, ?1)
    `,
  ).run(now - 100_000, now - 99_000);
  db.prepare(
    `
      INSERT INTO maintenance_window_monitors (maintenance_window_id, monitor_id)
      VALUES (2, 1)
    `,
  ).run();
  db.prepare(
    `
      INSERT INTO maintenance_windows (id, title, message, starts_at, ends_at, created_at)
      VALUES (3, 'Hidden Maint', 'Ignore', ?1, ?2, ?1)
    `,
  ).run(now - 200_000, now - 199_000);
  db.prepare(
    `
      INSERT INTO maintenance_window_monitors (maintenance_window_id, monitor_id)
      VALUES (3, 4)
    `,
  ).run();

  // Active incident (visible)
  db.prepare(
    `
      INSERT INTO incidents (id, title, status, impact, message, started_at, resolved_at)
      VALUES (1, 'Major Outage', 'investigating', 'major', 'Investigating', ?1, NULL)
    `,
  ).run(now - 1200);
  db.prepare(
    `
      INSERT INTO incident_monitors (incident_id, monitor_id)
      VALUES (1, 1)
    `,
  ).run();

  // Resolved incident previews: one visible (no links), one hidden-only
  db.prepare(
    `
      INSERT INTO incidents (id, title, status, impact, message, started_at, resolved_at)
      VALUES (2, 'Minor Issue', 'resolved', 'minor', 'Resolved', ?1, ?2)
    `,
  ).run(now - 20_000, now - 19_000);
  db.prepare(
    `
      INSERT INTO incidents (id, title, status, impact, message, started_at, resolved_at)
      VALUES (3, 'Hidden Issue', 'resolved', 'critical', 'Hidden', ?1, ?2)
    `,
  ).run(now - 30_000, now - 29_000);
  db.prepare(
    `
      INSERT INTO incident_monitors (incident_id, monitor_id)
      VALUES (3, 4)
    `,
  ).run();
}

describe('homepage snapshot SQL refresh', () => {
  it('produces a schema-valid homepage snapshot row', () => {
    const now = 1_800_000_000;
    const db = new DatabaseSync(':memory:');
    applyMigrations(db);
    seedScenario(db, now);

    db.prepare(__testOnly_homepageDataSnapshotSql).run(
      now,
      'Uptimer',
      '',
      'auto',
      'UTC',
      3,
    );

    const row = db
      .prepare(`SELECT generated_at, body_json FROM public_snapshots WHERE key = 'homepage'`)
      .get() as { generated_at: number; body_json: string } | undefined;

    expect(row?.generated_at).toBe(now);
    expect(typeof row?.body_json).toBe('string');

    const parsed = JSON.parse(row?.body_json ?? 'null') as unknown;
    const validated = publicHomepageResponseSchema.parse(parsed);

    expect(validated.generated_at).toBe(now);
    expect(validated.monitors.length).toBe(3);
    expect(validated.monitors.find((m) => m.id === 3)?.is_stale).toBe(true);
    expect(validated.banner.source).toBe('incident');
    expect(validated.maintenance_history_preview?.title).toBe('Past Maint');
    expect(validated.resolved_incident_preview?.title).toBe('Minor Issue');
  });
});
