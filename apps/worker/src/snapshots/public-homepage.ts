import { AppError } from '../middleware/errors';
import type { Trace } from '../observability/trace';
import { acquireLease } from '../scheduler/lock';
import {
  publicHomepageResponseSchema,
  type PublicHomepageResponse,
} from '../schemas/public-homepage';

const SNAPSHOT_KEY = 'homepage';
const SNAPSHOT_ARTIFACT_KEY = 'homepage:artifact';
const MAX_AGE_SECONDS = 60;
const MAX_STALE_SECONDS = 10 * 60;
const REFRESH_LOCK_NAME = 'snapshot:homepage:refresh';
const READ_SNAPSHOT_SQL = `
  SELECT generated_at, body_json
  FROM public_snapshots
  WHERE key = ?1
`;
const READ_SNAPSHOT_GENERATED_AT_SQL = `
  SELECT generated_at
  FROM public_snapshots
  WHERE key = ?1
`;
const UPSERT_SNAPSHOT_SQL = `
  INSERT INTO public_snapshots (key, generated_at, body_json, updated_at)
  VALUES (?1, ?2, ?3, ?4)
  ON CONFLICT(key) DO UPDATE SET
    generated_at = excluded.generated_at,
    body_json = excluded.body_json,
    updated_at = excluded.updated_at
`;

const SPLIT_SNAPSHOT_VERSION = 3;
const LEGACY_COMBINED_SNAPSHOT_VERSION = 2;

export type PublicHomepageRenderArtifact = {
  generated_at: number;
  preload_html: string;
  snapshot: PublicHomepageResponse;
  meta_title: string;
  meta_description: string;
};

const readSnapshotStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const readSnapshotGeneratedAtStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const upsertSnapshotStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();

function withTraceSync<T>(trace: Trace | undefined, name: string, fn: () => T): T {
  return trace ? trace.time(name, fn) : fn();
}

async function withTraceAsync<T>(
  trace: Trace | undefined,
  name: string,
  fn: () => Promise<T>,
): Promise<T> {
  return trace ? trace.timeAsync(name, fn) : await fn();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function normalizeSnapshotText(value: unknown, fallback: string): string {
  if (typeof value !== 'string') return fallback;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function escapeHtml(value: unknown): string {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatTime(tsSec: number, cache?: Map<number, string>): string {
  if (cache?.has(tsSec)) {
    return cache.get(tsSec) ?? '';
  }

  let formatted = '';
  try {
    formatted = new Date(tsSec * 1000).toISOString().replace('T', ' ').replace('.000Z', 'Z');
  } catch {
    formatted = '';
  }

  cache?.set(tsSec, formatted);
  return formatted;
}

function monitorGroupLabel(value: string | null | undefined): string {
  const trimmed = value?.trim() ?? '';
  return trimmed.length > 0 ? trimmed : 'Ungrouped';
}

function uptimeFillFromMilli(uptimePctMilli: number | null | undefined): string {
  if (typeof uptimePctMilli !== 'number') return '#cbd5e1';
  if (uptimePctMilli >= 99_950) return '#10b981';
  if (uptimePctMilli >= 99_000) return '#84cc16';
  if (uptimePctMilli >= 95_000) return '#f59e0b';
  return '#ef4444';
}

function heartbeatFillFromCode(code: string | undefined): string {
  switch (code) {
    case 'u':
      return '#10b981';
    case 'd':
      return '#ef4444';
    case 'm':
      return '#3b82f6';
    case 'x':
    default:
      return '#cbd5e1';
  }
}

function heartbeatHeightPct(
  code: string | undefined,
  latencyMs: number | null | undefined,
): number {
  if (code === 'd') return 100;
  if (code === 'm') return 62;
  if (code !== 'u') return 48;
  if (typeof latencyMs !== 'number' || !Number.isFinite(latencyMs)) return 74;
  return 36 + Math.min(64, Math.max(0, latencyMs / 12));
}

type StripRect = {
  x: number;
  y: number;
  width: number;
  height: number;
  fill: string;
};

function buildStripSvgFromRects(rects: StripRect[], width: number, height: number): string {
  const pathByFill = new Map<string, string[]>();

  for (const rect of rects) {
    if (rect.width <= 0 || rect.height <= 0) continue;
    const command = `M${rect.x},${rect.y}h${rect.width}v${rect.height}H${rect.x}z`;
    const existing = pathByFill.get(rect.fill);
    if (existing) {
      existing.push(command);
      continue;
    }
    pathByFill.set(rect.fill, [command]);
  }

  const paths: string[] = [];
  for (const [fill, commands] of pathByFill.entries()) {
    paths.push(`<path d="${commands.join('')}" fill="${fill}"/>`);
  }

  return `<svg class="usv" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">${paths.join('')}</svg>`;
}

function buildUptimeStripSvg(
  strip: PublicHomepageResponse['monitors'][number]['uptime_day_strip'],
): string {
  const count = Math.min(
    strip.day_start_at.length,
    strip.downtime_sec.length,
    strip.unknown_sec.length,
    strip.uptime_pct_milli.length,
  );
  const barWidth = 4;
  const gap = 2;
  const height = 20;
  const width = count <= 0 ? barWidth : count * barWidth + Math.max(0, count - 1) * gap;
  const rects: StripRect[] = [];
  for (let index = 0; index < count; index += 1) {
    rects.push({
      x: index * (barWidth + gap),
      y: 0,
      width: barWidth,
      height,
      fill: uptimeFillFromMilli(strip.uptime_pct_milli[index]),
    });
  }
  return buildStripSvgFromRects(rects, width, height);
}

function buildHeartbeatStripSvg(
  strip: PublicHomepageResponse['monitors'][number]['heartbeat_strip'],
): string {
  const count = Math.min(
    strip.checked_at.length,
    strip.latency_ms.length,
    strip.status_codes.length,
  );
  const barWidth = 4;
  const gap = 2;
  const height = 20;
  const width = count <= 0 ? barWidth : count * barWidth + Math.max(0, count - 1) * gap;
  const rects: StripRect[] = [];
  for (let index = 0; index < count; index += 1) {
    const barHeight = Math.round(
      (height * heartbeatHeightPct(strip.status_codes[index], strip.latency_ms[index])) / 100,
    );
    rects.push({
      x: index * (barWidth + gap),
      y: height - barHeight,
      width: barWidth,
      height: barHeight,
      fill: heartbeatFillFromCode(strip.status_codes[index]),
    });
  }
  return buildStripSvgFromRects(rects, width, height);
}

function renderIncidentCard(
  incident: PublicHomepageResponse['active_incidents'][number],
  formatTimestamp: (tsSec: number) => string,
): string {
  const impactVariant =
    incident.impact === 'major' || incident.impact === 'critical' ? 'down' : 'paused';

  const parts: string[] = [
    `<article class="card"><div class="row"><h4 class="mn">${escapeHtml(incident.title)}</h4><span class="sb sb-${impactVariant}">${escapeHtml(incident.impact)}</span></div><div class="ft">${formatTimestamp(incident.started_at)}</div>`,
  ];
  if (incident.message) {
    parts.push(`<p class="bt">${escapeHtml(incident.message)}</p>`);
  }
  parts.push('</article>');
  return parts.join('');
}

function renderMaintenanceCard(
  window: NonNullable<PublicHomepageResponse['maintenance_history_preview']>,
  monitorNames: ReadonlyMap<number, string>,
  formatTimestamp: (tsSec: number) => string,
): string {
  const affected: string[] = [];
  for (let index = 0; index < window.monitor_ids.length; index += 1) {
    const monitorId = window.monitor_ids[index];
    if (typeof monitorId !== 'number') {
      continue;
    }
    affected.push(escapeHtml(monitorNames.get(monitorId) || `#${monitorId}`));
  }

  const parts: string[] = [
    `<article class="card"><div><h4 class="mn">${escapeHtml(window.title)}</h4><div class="ft">${formatTimestamp(window.starts_at)} - ${formatTimestamp(window.ends_at)}</div></div>`,
  ];
  if (affected.length > 0) {
    parts.push(`<div class="bt">Affected: ${affected.join(', ')}</div>`);
  }
  if (window.message) {
    parts.push(`<p class="bt">${escapeHtml(window.message)}</p>`);
  }
  parts.push('</article>');
  return parts.join('');
}

function renderPreload(
  snapshot: PublicHomepageResponse,
  monitorNameById?: ReadonlyMap<number, string>,
): string {
  const overall = snapshot.overall_status;
  const siteTitle = snapshot.site_title;
  const siteDescription = snapshot.site_description;
  const bannerTitle = snapshot.banner.title;
  const generatedAt = snapshot.generated_at;
  const timeCache = new Map<number, string>();
  const formatTimestamp = (tsSec: number) => escapeHtml(formatTime(tsSec, timeCache));
  const needsMonitorNames =
    snapshot.maintenance_windows.active.length > 0 ||
    snapshot.maintenance_windows.upcoming.length > 0 ||
    snapshot.maintenance_history_preview !== null;
  const monitorNames: ReadonlyMap<number, string> | null = needsMonitorNames
    ? monitorNameById ?? new Map(snapshot.monitors.map((monitor) => [monitor.id, monitor.name]))
    : null;
  const groups = new Map<string, PublicHomepageResponse['monitors']>();
  for (const monitor of snapshot.monitors) {
    const key = monitorGroupLabel(monitor.group_name);
    const existing = groups.get(key) ?? [];
    existing.push(monitor);
    groups.set(key, existing);
  }

  const groupedMonitorsParts: string[] = [];
  for (const [groupName, groupMonitors] of groups.entries()) {
    const monitorCardsParts: string[] = [];
    for (const monitor of groupMonitors) {
      const uptimePct =
        typeof monitor.uptime_30d?.uptime_pct === 'number'
          ? `${monitor.uptime_30d.uptime_pct.toFixed(3)}%`
          : '-';
      const status = monitor.status;
      const statusLabel = escapeHtml(status);
      const lastCheckedLabel = monitor.last_checked_at
        ? `Last checked: ${formatTimestamp(monitor.last_checked_at)}`
        : 'Never checked';

      monitorCardsParts.push(
        `<article class="card"><div class="row"><div class="lhs"><span class="dot dot-${status}"></span><div class="ut"><div class="mn">${escapeHtml(monitor.name)}</div><div class="mt">${escapeHtml(monitor.type)}</div></div></div><div class="rhs"><span class="up">${escapeHtml(uptimePct)}</span><span class="sb sb-${status}">${statusLabel}</span></div></div><div><div class="lbl">Availability (30d)</div><div class="strip">${buildUptimeStripSvg(monitor.uptime_day_strip)}</div></div><div><div class="lbl">Recent checks</div><div class="strip">${buildHeartbeatStripSvg(monitor.heartbeat_strip)}</div></div><div class="ft">${lastCheckedLabel}</div></article>`,
      );
    }

    groupedMonitorsParts.push(
      `<section class="sg"><div class="sgh"><h4 class="sgt">${escapeHtml(groupName)}</h4><span class="sgc">${groupMonitors.length}</span></div><div class="grid">${monitorCardsParts.join('')}</div></section>`,
    );
  }

  const activeMaintenance = snapshot.maintenance_windows.active;
  const upcomingMaintenance = snapshot.maintenance_windows.upcoming;
  let maintenanceSection = '';
  if (activeMaintenance.length > 0 || upcomingMaintenance.length > 0) {
    const activeCards: string[] = [];
    for (const window of activeMaintenance) {
      if (monitorNames) {
        activeCards.push(renderMaintenanceCard(window, monitorNames, formatTimestamp));
      }
    }
    const upcomingCards: string[] = [];
    for (const window of upcomingMaintenance) {
      if (monitorNames) {
        upcomingCards.push(renderMaintenanceCard(window, monitorNames, formatTimestamp));
      }
    }

    maintenanceSection = `<section class="sec"><h3 class="sh">Scheduled Maintenance</h3>${activeCards.length > 0 ? `<div class="st">${activeCards.join('')}</div>` : ''}${upcomingCards.length > 0 ? `<div class="st">${upcomingCards.join('')}</div>` : ''}</section>`;
  }

  let incidentSection = '';
  if (snapshot.active_incidents.length > 0) {
    const incidentCards: string[] = [];
    for (const incident of snapshot.active_incidents) {
      incidentCards.push(renderIncidentCard(incident, formatTimestamp));
    }
    incidentSection = `<section class="sec"><h3 class="sh">Active Incidents</h3><div class="st">${incidentCards.join('')}</div></section>`;
  }

  const incidentHistory = snapshot.resolved_incident_preview
    ? renderIncidentCard(snapshot.resolved_incident_preview, formatTimestamp)
    : '<div class="card">No past incidents</div>';
  const maintenanceHistory = snapshot.maintenance_history_preview
    ? monitorNames
      ? renderMaintenanceCard(snapshot.maintenance_history_preview, monitorNames, formatTimestamp)
      : '<div class="card">No past maintenance</div>'
    : '<div class="card">No past maintenance</div>';
  const descriptionHtml = siteDescription
    ? `<div class="ud">${escapeHtml(siteDescription)}</div>`
    : '';
  return `<div class="hp"><header class="uh"><div class="uw uhw"><div class="ut"><div class="un">${escapeHtml(siteTitle)}</div>${descriptionHtml}</div><span class="sb sb-${overall}">${escapeHtml(overall)}</span></div></header><main class="uw um"><section class="bn"><div class="bt">${escapeHtml(bannerTitle)}</div><div class="bu">Updated: ${formatTimestamp(generatedAt)}</div></section>${maintenanceSection}${incidentSection}<section class="sec"><h3 class="sh">Services</h3>${groupedMonitorsParts.join('')}</section><section class="sec ih"><div><h3 class="sh">Incident History</h3>${incidentHistory}</div><div><h3 class="sh">Maintenance History</h3>${maintenanceHistory}</div></section></main></div>`;
}

export function buildHomepageRenderArtifact(
  snapshot: PublicHomepageResponse,
): PublicHomepageRenderArtifact {
  const fullSnapshot: PublicHomepageResponse = {
    ...snapshot,
    bootstrap_mode: 'full',
    monitor_count_total: snapshot.monitors.length,
  };
  const needsMonitorNames =
    fullSnapshot.maintenance_windows.active.length > 0 ||
    fullSnapshot.maintenance_windows.upcoming.length > 0 ||
    fullSnapshot.maintenance_history_preview !== null;
  const allMonitorNames = needsMonitorNames
    ? new Map(fullSnapshot.monitors.map((monitor) => [monitor.id, monitor.name]))
    : undefined;
  const metaTitle = normalizeSnapshotText(fullSnapshot.site_title, 'Uptimer');
  const fallbackDescription = normalizeSnapshotText(
    fullSnapshot.banner.title,
    'Real-time status and incident updates.',
  );
  const metaDescription = normalizeSnapshotText(fullSnapshot.site_description, fallbackDescription)
    .replace(/\s+/g, ' ')
    .trim();

  return {
    generated_at: fullSnapshot.generated_at,
    preload_html: `<div id="uptimer-preload">${renderPreload(fullSnapshot, allMonitorNames)}</div>`,
    snapshot: fullSnapshot,
    meta_title: metaTitle,
    meta_description: metaDescription,
  };
}

function looksLikeHomepagePayload(value: unknown): value is PublicHomepageResponse {
  if (!isRecord(value)) return false;
  return (
    typeof value.generated_at === 'number' &&
    (value.bootstrap_mode === 'full' || value.bootstrap_mode === 'partial') &&
    typeof value.monitor_count_total === 'number' &&
    typeof value.site_title === 'string' &&
    Array.isArray(value.monitors) &&
    Array.isArray(value.active_incidents) &&
    isRecord(value.summary) &&
    isRecord(value.banner) &&
    isRecord(value.maintenance_windows)
  );
}

function looksLikeHomepageArtifact(value: unknown): value is PublicHomepageRenderArtifact {
  if (!isRecord(value)) return false;

  return (
    typeof value.generated_at === 'number' &&
    typeof value.preload_html === 'string' &&
    typeof value.meta_title === 'string' &&
    typeof value.meta_description === 'string' &&
    looksLikeHomepagePayload(value.snapshot)
  );
}

function looksLikeSerializedHomepagePayload(text: string): boolean {
  const trimmed = text.trim();
  return (
    trimmed.startsWith('{"generated_at":') &&
    trimmed.includes('"bootstrap_mode"') &&
    trimmed.includes('"monitor_count_total"')
  );
}

function looksLikeSerializedHomepageArtifact(text: string): boolean {
  const trimmed = text.trim();
  return (
    trimmed.startsWith('{"generated_at":') &&
    trimmed.includes('"preload_html"') &&
    trimmed.includes('"meta_title"') &&
    trimmed.includes('"snapshot"')
  );
}

function readStoredHomepageSnapshotData(value: unknown): PublicHomepageResponse | null {
  if (looksLikeHomepageArtifact(value)) {
    return value.snapshot;
  }

  if (!isRecord(value)) return null;

  const version = value.version;
  if (version === SPLIT_SNAPSHOT_VERSION) {
    return looksLikeHomepagePayload(value.data) ? value.data : null;
  }

  if (version === LEGACY_COMBINED_SNAPSHOT_VERSION) {
    return looksLikeHomepagePayload(value.data) ? value.data : null;
  }

  const parsed = publicHomepageResponseSchema.safeParse({
    ...value,
    bootstrap_mode: 'full',
    monitor_count_total: Array.isArray(value.monitors) ? value.monitors.length : 0,
  });
  if (!parsed.success) {
    return null;
  }

  return parsed.data;
}

function readStoredHomepageSnapshotRender(value: unknown): PublicHomepageRenderArtifact | null {
  if (looksLikeHomepageArtifact(value)) {
    return value;
  }

  if (!isRecord(value)) return null;
  const version = value.version;
  if (version !== SPLIT_SNAPSHOT_VERSION && version !== LEGACY_COMBINED_SNAPSHOT_VERSION) {
    return null;
  }

  return looksLikeHomepageArtifact(value.render) ? value.render : null;
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

async function readSnapshotRow(
  db: D1Database,
  key: string,
): Promise<{ generated_at: number; body_json: string } | null> {
  try {
    const cached = readSnapshotStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_SNAPSHOT_SQL);
    if (!cached) {
      readSnapshotStatementByDb.set(db, statement);
    }

    return await statement
      .bind(key)
      .first<{ generated_at: number; body_json: string }>();
  } catch (err) {
    console.warn('homepage snapshot: read failed', err);
    return null;
  }
}

async function readSnapshotGeneratedAt(db: D1Database, key: string): Promise<number | null> {
  try {
    const cached = readSnapshotGeneratedAtStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_SNAPSHOT_GENERATED_AT_SQL);
    if (!cached) {
      readSnapshotGeneratedAtStatementByDb.set(db, statement);
    }

    const row = await statement.bind(key).first<{ generated_at: number }>();
    return row?.generated_at ?? null;
  } catch (err) {
    console.warn('homepage snapshot: read generated_at failed', err);
    return null;
  }
}

async function readHomepageSnapshotRow(db: D1Database) {
  return readSnapshotRow(db, SNAPSHOT_KEY);
}

async function readHomepageArtifactSnapshotRow(db: D1Database) {
  return readSnapshotRow(db, SNAPSHOT_ARTIFACT_KEY);
}

function isSameMinute(a: number, b: number): boolean {
  return Math.floor(a / 60) === Math.floor(b / 60);
}

export function getHomepageSnapshotKey() {
  return SNAPSHOT_KEY;
}

export function getHomepageSnapshotMaxAgeSeconds() {
  return MAX_AGE_SECONDS;
}

export function getHomepageSnapshotMaxStaleSeconds() {
  return MAX_STALE_SECONDS;
}

export async function readHomepageSnapshot(
  db: D1Database,
  now: number,
): Promise<{ data: PublicHomepageResponse; age: number } | null> {
  const row = (await readHomepageArtifactSnapshotRow(db)) ?? (await readHomepageSnapshotRow(db));
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_AGE_SECONDS) return null;

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const data = readStoredHomepageSnapshotData(parsed);
  if (!data) {
    console.warn('homepage snapshot: invalid payload');
    return null;
  }

  return {
    data,
    age,
  };
}

export async function readHomepageSnapshotJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  const row = (await readHomepageArtifactSnapshotRow(db)) ?? (await readHomepageSnapshotRow(db));
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_AGE_SECONDS) return null;

  if (looksLikeSerializedHomepagePayload(row.body_json)) {
    return {
      bodyJson: row.body_json,
      age,
    };
  }

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const data = readStoredHomepageSnapshotData(parsed);
  if (!data) {
    console.warn('homepage snapshot: invalid payload');
    return null;
  }

  return {
    bodyJson: JSON.stringify(data),
    age,
  };
}

export async function readStaleHomepageSnapshot(
  db: D1Database,
  now: number,
): Promise<{ data: PublicHomepageResponse; age: number } | null> {
  const row = (await readHomepageArtifactSnapshotRow(db)) ?? (await readHomepageSnapshotRow(db));
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_STALE_SECONDS) return null;

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const data = readStoredHomepageSnapshotData(parsed);
  if (!data) {
    console.warn('homepage snapshot: invalid stale payload');
    return null;
  }

  return {
    data,
    age,
  };
}

export async function readStaleHomepageSnapshotJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  const row = (await readHomepageArtifactSnapshotRow(db)) ?? (await readHomepageSnapshotRow(db));
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_STALE_SECONDS) return null;

  if (looksLikeSerializedHomepagePayload(row.body_json)) {
    return {
      bodyJson: row.body_json,
      age,
    };
  }

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const data = readStoredHomepageSnapshotData(parsed);
  if (!data) {
    console.warn('homepage snapshot: invalid stale payload');
    return null;
  }

  return {
    bodyJson: JSON.stringify(data),
    age,
  };
}

export async function readHomepageSnapshotArtifact(
  db: D1Database,
  now: number,
): Promise<{ data: PublicHomepageRenderArtifact; age: number } | null> {
  const row = (await readHomepageArtifactSnapshotRow(db)) ?? (await readHomepageSnapshotRow(db));
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_AGE_SECONDS) return null;

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const render = readStoredHomepageSnapshotRender(parsed);
  if (!render) {
    console.warn('homepage snapshot: invalid render payload');
    return null;
  }

  return {
    data: render,
    age,
  };
}

export async function readHomepageSnapshotArtifactJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  const row = (await readHomepageArtifactSnapshotRow(db)) ?? (await readHomepageSnapshotRow(db));
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_AGE_SECONDS) return null;

  if (looksLikeSerializedHomepageArtifact(row.body_json)) {
    return {
      bodyJson: row.body_json,
      age,
    };
  }

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const render = readStoredHomepageSnapshotRender(parsed);
  if (!render) {
    console.warn('homepage snapshot: invalid render payload');
    return null;
  }

  return {
    bodyJson: JSON.stringify(render),
    age,
  };
}

export async function readStaleHomepageSnapshotArtifact(
  db: D1Database,
  now: number,
): Promise<{ data: PublicHomepageRenderArtifact; age: number } | null> {
  const row = (await readHomepageArtifactSnapshotRow(db)) ?? (await readHomepageSnapshotRow(db));
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_STALE_SECONDS) return null;

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const render = readStoredHomepageSnapshotRender(parsed);
  if (!render) {
    console.warn('homepage snapshot: invalid stale render payload');
    return null;
  }

  return {
    data: render,
    age,
  };
}

export async function readStaleHomepageSnapshotArtifactJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  const row = (await readHomepageArtifactSnapshotRow(db)) ?? (await readHomepageSnapshotRow(db));
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_STALE_SECONDS) return null;

  if (looksLikeSerializedHomepageArtifact(row.body_json)) {
    return {
      bodyJson: row.body_json,
      age,
    };
  }

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const render = readStoredHomepageSnapshotRender(parsed);
  if (!render) {
    console.warn('homepage snapshot: invalid stale render payload');
    return null;
  }

  return {
    bodyJson: JSON.stringify(render),
    age,
  };
}

export async function readHomepageSnapshotGeneratedAt(
  db: D1Database,
): Promise<number | null> {
  return (
    (await readSnapshotGeneratedAt(db, SNAPSHOT_ARTIFACT_KEY)) ??
    (await readSnapshotGeneratedAt(db, SNAPSHOT_KEY))
  );
}

export async function readHomepageArtifactSnapshotGeneratedAt(
  db: D1Database,
): Promise<number | null> {
  return (
    (await readSnapshotGeneratedAt(db, SNAPSHOT_ARTIFACT_KEY)) ??
    (await readSnapshotGeneratedAt(db, SNAPSHOT_KEY))
  );
}

function homepageSnapshotUpsertStatement(
  db: D1Database,
  key: string,
  generatedAt: number,
  bodyJson: string,
  now: number,
): D1PreparedStatement {
  const cached = upsertSnapshotStatementByDb.get(db);
  const statement = cached ?? db.prepare(UPSERT_SNAPSHOT_SQL);
  if (!cached) {
    upsertSnapshotStatementByDb.set(db, statement);
  }

  return statement.bind(key, generatedAt, bodyJson, now);
}

export async function writeHomepageSnapshot(
  db: D1Database,
  now: number,
  payload: PublicHomepageResponse,
  trace?: Trace,
): Promise<void> {
  const render = withTraceSync(trace, 'homepage_write_render', () =>
    buildHomepageRenderArtifact(payload),
  );
  const renderBodyJson = withTraceSync(trace, 'homepage_write_stringify_artifact', () =>
    JSON.stringify(render),
  );

  await withTraceAsync(trace, 'homepage_write_batch', async () =>
    await homepageSnapshotUpsertStatement(
      db,
      SNAPSHOT_ARTIFACT_KEY,
      render.generated_at,
      renderBodyJson,
      now,
    ).run(),
  );
}

export async function writeHomepageArtifactSnapshot(
  db: D1Database,
  now: number,
  payload: PublicHomepageResponse,
  trace?: Trace,
): Promise<void> {
  const render = withTraceSync(trace, 'homepage_artifact_write_render', () =>
    buildHomepageRenderArtifact(payload),
  );
  const renderBodyJson = withTraceSync(trace, 'homepage_artifact_write_stringify', () =>
    JSON.stringify(render),
  );

  await withTraceAsync(trace, 'homepage_artifact_write_run', async () =>
    await homepageSnapshotUpsertStatement(
      db,
      SNAPSHOT_ARTIFACT_KEY,
      render.generated_at,
      renderBodyJson,
      now,
    ).run(),
  );
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

export function toHomepageSnapshotPayload(value: unknown): PublicHomepageResponse {
  const parsed = publicHomepageResponseSchema.safeParse(value);
  if (!parsed.success) {
    throw new AppError(500, 'INTERNAL', 'Failed to generate homepage snapshot');
  }
  return parsed.data;
}

export async function refreshPublicHomepageSnapshot(opts: {
  db: D1Database;
  now: number;
  compute: () => Promise<unknown>;
  trace?: Trace;
}): Promise<void> {
  const computed = await withTraceAsync(opts.trace, 'homepage_refresh_compute', async () =>
    await opts.compute(),
  );
  const payload = withTraceSync(opts.trace, 'homepage_refresh_validate', () =>
    toHomepageSnapshotPayload(computed),
  );
  await writeHomepageSnapshot(opts.db, opts.now, payload, opts.trace);
}

export async function refreshPublicHomepageArtifactSnapshot(opts: {
  db: D1Database;
  now: number;
  compute: () => Promise<unknown>;
  trace?: Trace;
}): Promise<void> {
  const computed = await withTraceAsync(
    opts.trace,
    'homepage_artifact_refresh_compute',
    async () => await opts.compute(),
  );
  const payload = withTraceSync(opts.trace, 'homepage_artifact_refresh_validate', () =>
    toHomepageSnapshotPayload(computed),
  );
  await writeHomepageArtifactSnapshot(opts.db, opts.now, payload, opts.trace);
}

export async function refreshPublicHomepageSnapshotIfNeeded(opts: {
  db: D1Database;
  now: number;
  compute: () => Promise<unknown>;
  trace?: Trace;
}): Promise<boolean> {
  const generatedAt = await withTraceAsync(
    opts.trace,
    'homepage_refresh_read_generated_at_1',
    async () => await readHomepageSnapshotGeneratedAt(opts.db),
  );
  if (generatedAt !== null && isSameMinute(generatedAt, opts.now)) {
    return false;
  }

  const acquired = await withTraceAsync(opts.trace, 'homepage_refresh_lease', async () =>
    await acquireLease(opts.db, REFRESH_LOCK_NAME, opts.now, 55),
  );
  if (!acquired) {
    return false;
  }

  const latestGeneratedAt = await withTraceAsync(
    opts.trace,
    'homepage_refresh_read_generated_at_2',
    async () => await readHomepageSnapshotGeneratedAt(opts.db),
  );
  if (latestGeneratedAt !== null && isSameMinute(latestGeneratedAt, opts.now)) {
    return false;
  }

  await withTraceAsync(opts.trace, 'homepage_refresh_write', async () =>
    await refreshPublicHomepageSnapshot(opts),
  );
  return true;
}

export async function refreshPublicHomepageArtifactSnapshotIfNeeded(opts: {
  db: D1Database;
  now: number;
  compute: () => Promise<unknown>;
  trace?: Trace;
}): Promise<boolean> {
  const generatedAt = await withTraceAsync(
    opts.trace,
    'homepage_artifact_refresh_read_generated_at_1',
    async () => await readHomepageArtifactSnapshotGeneratedAt(opts.db),
  );
  if (generatedAt !== null && isSameMinute(generatedAt, opts.now)) {
    return false;
  }

  const acquired = await withTraceAsync(opts.trace, 'homepage_artifact_refresh_lease', async () =>
    await acquireLease(opts.db, REFRESH_LOCK_NAME, opts.now, 55),
  );
  if (!acquired) {
    return false;
  }

  const latestGeneratedAt = await withTraceAsync(
    opts.trace,
    'homepage_artifact_refresh_read_generated_at_2',
    async () => await readHomepageArtifactSnapshotGeneratedAt(opts.db),
  );
  if (latestGeneratedAt !== null && isSameMinute(latestGeneratedAt, opts.now)) {
    return false;
  }

  await withTraceAsync(opts.trace, 'homepage_artifact_refresh_write', async () =>
    await refreshPublicHomepageArtifactSnapshot(opts),
  );
  return true;
}
