import { AppError } from '../middleware/errors';
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
const REFRESH_DATA_SQL_LOCK_NAME = 'snapshot:homepage:data:refresh';
const MAX_BOOTSTRAP_MONITORS = 12;

const SPLIT_SNAPSHOT_VERSION = 3;
const LEGACY_COMBINED_SNAPSHOT_VERSION = 2;

export type PublicHomepageRenderArtifact = {
  generated_at: number;
  preload_html: string;
  snapshot: PublicHomepageResponse;
  meta_title: string;
  meta_description: string;
};

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
    formatted = new Date(tsSec * 1000).toLocaleString();
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
  let rects = '';
  for (let index = 0; index < count; index += 1) {
    const x = index * (barWidth + gap);
    const fill = uptimeFillFromMilli(strip.uptime_pct_milli[index]);
    rects += `<rect x="${x}" width="${barWidth}" height="${height}" rx="1" fill="${fill}"/>`;
  }
  return `<svg class="usv" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">${rects}</svg>`;
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
  let rects = '';
  for (let index = 0; index < count; index += 1) {
    const x = index * (barWidth + gap);
    const barHeight =
      (height * heartbeatHeightPct(strip.status_codes[index], strip.latency_ms[index])) / 100;
    const y = height - barHeight;
    rects += `<rect x="${x}" y="${y.toFixed(2)}" width="${barWidth}" height="${barHeight.toFixed(2)}" rx="1" fill="${heartbeatFillFromCode(strip.status_codes[index])}"/>`;
  }
  return `<svg class="usv" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">${rects}</svg>`;
}

function renderIncidentCard(
  incident: PublicHomepageResponse['active_incidents'][number],
  formatTimestamp: (tsSec: number) => string,
): string {
  const impactVariant =
    incident.impact === 'major' || incident.impact === 'critical' ? 'down' : 'paused';

  let html = `<article class="card"><div class="row"><h4 class="mn">${escapeHtml(incident.title)}</h4><span class="sb sb-${impactVariant}">${escapeHtml(incident.impact)}</span></div><div class="ft">${formatTimestamp(incident.started_at)}</div>`;
  if (incident.message) {
    html += `<p class="bt">${escapeHtml(incident.message)}</p>`;
  }
  html += '</article>';
  return html;
}

function renderMaintenanceCard(
  window: NonNullable<PublicHomepageResponse['maintenance_history_preview']>,
  monitorNames: Map<number, string>,
  formatTimestamp: (tsSec: number) => string,
): string {
  let affected = '';
  for (let index = 0; index < window.monitor_ids.length; index += 1) {
    const monitorId = window.monitor_ids[index];
    if (typeof monitorId !== 'number') {
      continue;
    }
    if (index > 0) {
      affected += ', ';
    }
    affected += escapeHtml(monitorNames.get(monitorId) || `#${monitorId}`);
  }

  let html = `<article class="card"><div><h4 class="mn">${escapeHtml(window.title)}</h4><div class="ft">${formatTimestamp(window.starts_at)} - ${formatTimestamp(window.ends_at)}</div></div>`;
  if (affected) {
    html += `<div class="bt">Affected: ${affected}</div>`;
  }
  if (window.message) {
    html += `<p class="bt">${escapeHtml(window.message)}</p>`;
  }
  html += '</article>';
  return html;
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
  const monitorNames = new Map<number, string>();
  if (needsMonitorNames) {
    if (monitorNameById) {
      for (const [monitorId, monitorName] of monitorNameById.entries()) {
        monitorNames.set(monitorId, monitorName);
      }
    } else {
      for (const monitor of snapshot.monitors) {
        monitorNames.set(monitor.id, monitor.name);
      }
    }
  }
  const groups = new Map<string, PublicHomepageResponse['monitors']>();
  for (const monitor of snapshot.monitors) {
    const key = monitorGroupLabel(monitor.group_name);
    const existing = groups.get(key) ?? [];
    existing.push(monitor);
    groups.set(key, existing);
  }

  let groupedMonitors = '';
  for (const [groupName, groupMonitors] of groups.entries()) {
    let monitorCards = '';
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

      monitorCards += `<article class="card"><div class="row"><div class="lhs"><span class="dot dot-${status}"></span><div class="ut"><div class="mn">${escapeHtml(monitor.name)}</div><div class="mt">${escapeHtml(monitor.type)}</div></div></div><div class="rhs"><span class="up">${escapeHtml(uptimePct)}</span><span class="sb sb-${status}">${statusLabel}</span></div></div><div><div class="lbl">Availability (30d)</div><div class="strip">${buildUptimeStripSvg(monitor.uptime_day_strip)}</div></div><div><div class="lbl">Recent checks</div><div class="strip">${buildHeartbeatStripSvg(monitor.heartbeat_strip)}</div></div><div class="ft">${lastCheckedLabel}</div></article>`;
    }

    groupedMonitors += `<section class="sg"><div class="sgh"><h4 class="sgt">${escapeHtml(groupName)}</h4><span class="sgc">${groupMonitors.length}</span></div><div class="grid">${monitorCards}</div></section>`;
  }

  const activeMaintenance = snapshot.maintenance_windows.active;
  const upcomingMaintenance = snapshot.maintenance_windows.upcoming;
  const hiddenMonitorCount = Math.max(0, snapshot.monitor_count_total - snapshot.monitors.length);
  let maintenanceSection = '';
  if (activeMaintenance.length > 0 || upcomingMaintenance.length > 0) {
    let activeCards = '';
    for (const window of activeMaintenance) {
      activeCards += renderMaintenanceCard(window, monitorNames, formatTimestamp);
    }
    let upcomingCards = '';
    for (const window of upcomingMaintenance) {
      upcomingCards += renderMaintenanceCard(window, monitorNames, formatTimestamp);
    }

    maintenanceSection = `<section class="sec"><h3 class="sh">Scheduled Maintenance</h3>${activeCards ? `<div class="st">${activeCards}</div>` : ''}${upcomingCards ? `<div class="st">${upcomingCards}</div>` : ''}</section>`;
  }

  let incidentSection = '';
  if (snapshot.active_incidents.length > 0) {
    let incidentCards = '';
    for (const incident of snapshot.active_incidents) {
      incidentCards += renderIncidentCard(incident, formatTimestamp);
    }
    incidentSection = `<section class="sec"><h3 class="sh">Active Incidents</h3><div class="st">${incidentCards}</div></section>`;
  }

  const incidentHistory = snapshot.resolved_incident_preview
    ? renderIncidentCard(snapshot.resolved_incident_preview, formatTimestamp)
    : '<div class="card">No past incidents</div>';
  const maintenanceHistory = snapshot.maintenance_history_preview
    ? renderMaintenanceCard(snapshot.maintenance_history_preview, monitorNames, formatTimestamp)
    : '<div class="card">No past maintenance</div>';
  const descriptionHtml = siteDescription
    ? `<div class="ud">${escapeHtml(siteDescription)}</div>`
    : '';
  const hiddenMonitorMessage =
    hiddenMonitorCount > 0
      ? `<div class="card ft">${hiddenMonitorCount} more services will appear after the app finishes loading.</div>`
      : '';

  return `<div class="hp"><header class="uh"><div class="uw uhw"><div class="ut"><div class="un">${escapeHtml(siteTitle)}</div>${descriptionHtml}</div><span class="sb sb-${overall}">${escapeHtml(overall)}</span></div></header><main class="uw um"><section class="bn"><div class="bt">${escapeHtml(bannerTitle)}</div><div class="bu">Updated: ${formatTimestamp(generatedAt)}</div></section>${maintenanceSection}${incidentSection}<section class="sec"><h3 class="sh">Services</h3>${groupedMonitors}${hiddenMonitorMessage}</section><section class="sec ih"><div><h3 class="sh">Incident History</h3>${incidentHistory}</div><div><h3 class="sh">Maintenance History</h3>${maintenanceHistory}</div></section></main></div>`;
}

export function buildHomepageRenderArtifact(
  snapshot: PublicHomepageResponse,
): PublicHomepageRenderArtifact {
  const allMonitorNames = new Map<number, string>();
  for (const monitor of snapshot.monitors) {
    allMonitorNames.set(monitor.id, monitor.name);
  }
  const bootstrapSnapshot =
    snapshot.bootstrap_mode === 'partial' || snapshot.monitors.length > MAX_BOOTSTRAP_MONITORS
      ? {
          ...snapshot,
          bootstrap_mode: 'partial' as const,
          monitors: snapshot.monitors.slice(0, MAX_BOOTSTRAP_MONITORS),
        }
      : {
          ...snapshot,
          bootstrap_mode: 'full' as const,
        };
  const metaTitle = normalizeSnapshotText(snapshot.site_title, 'Uptimer');
  const fallbackDescription = normalizeSnapshotText(
    snapshot.banner.title,
    'Real-time status and incident updates.',
  );
  const metaDescription = normalizeSnapshotText(snapshot.site_description, fallbackDescription)
    .replace(/\s+/g, ' ')
    .trim();

  return {
    generated_at: snapshot.generated_at,
    preload_html: `<div id="uptimer-preload">${renderPreload(bootstrapSnapshot, allMonitorNames)}</div>`,
    snapshot: bootstrapSnapshot,
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
    return await db
      .prepare(
        `
        SELECT generated_at, body_json
        FROM public_snapshots
        WHERE key = ?1
      `,
      )
      .bind(key)
      .first<{ generated_at: number; body_json: string }>();
  } catch (err) {
    console.warn('homepage snapshot: read failed', err);
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
  const row = await readHomepageSnapshotRow(db);
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
  const row = await readHomepageSnapshotRow(db);
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
  const row = await readHomepageSnapshotRow(db);
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
  const row = await readHomepageSnapshotRow(db);
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
  const row = await readHomepageSnapshotRow(db);
  return row?.generated_at ?? null;
}

export async function readHomepageArtifactSnapshotGeneratedAt(
  db: D1Database,
): Promise<number | null> {
  const row = await readHomepageArtifactSnapshotRow(db);
  return row?.generated_at ?? null;
}

function homepageSnapshotUpsertStatement(
  db: D1Database,
  key: string,
  generatedAt: number,
  bodyJson: string,
  now: number,
): D1PreparedStatement {
  return db
    .prepare(
      `
      INSERT INTO public_snapshots (key, generated_at, body_json, updated_at)
      VALUES (?1, ?2, ?3, ?4)
      ON CONFLICT(key) DO UPDATE SET
        generated_at = excluded.generated_at,
        body_json = excluded.body_json,
        updated_at = excluded.updated_at
    `,
    )
    .bind(key, generatedAt, bodyJson, now);
}

export async function writeHomepageSnapshot(
  db: D1Database,
  now: number,
  payload: PublicHomepageResponse,
): Promise<void> {
  const render = buildHomepageRenderArtifact(payload);
  const dataBodyJson = JSON.stringify(payload);
  const renderBodyJson = JSON.stringify(render);

  await db.batch([
    homepageSnapshotUpsertStatement(db, SNAPSHOT_KEY, payload.generated_at, dataBodyJson, now),
    homepageSnapshotUpsertStatement(
      db,
      SNAPSHOT_ARTIFACT_KEY,
      render.generated_at,
      renderBodyJson,
      now,
    ),
  ]);
}

export async function writeHomepageArtifactSnapshot(
  db: D1Database,
  now: number,
  payload: PublicHomepageResponse,
): Promise<void> {
  const render = buildHomepageRenderArtifact(payload);
  const renderBodyJson = JSON.stringify(render);

  await homepageSnapshotUpsertStatement(
    db,
    SNAPSHOT_ARTIFACT_KEY,
    render.generated_at,
    renderBodyJson,
    now,
  ).run();
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
}): Promise<void> {
  const payload = toHomepageSnapshotPayload(await opts.compute());
  await writeHomepageSnapshot(opts.db, opts.now, payload);
}

export async function refreshPublicHomepageArtifactSnapshot(opts: {
  db: D1Database;
  now: number;
  compute: () => Promise<unknown>;
}): Promise<void> {
  const payload = toHomepageSnapshotPayload(await opts.compute());
  await writeHomepageArtifactSnapshot(opts.db, opts.now, payload);
}

export async function refreshPublicHomepageSnapshotIfNeeded(opts: {
  db: D1Database;
  now: number;
  compute: () => Promise<unknown>;
}): Promise<boolean> {
  const generatedAt = await readHomepageSnapshotGeneratedAt(opts.db);
  if (generatedAt !== null && isSameMinute(generatedAt, opts.now)) {
    return false;
  }

  const acquired = await acquireLease(opts.db, REFRESH_LOCK_NAME, opts.now, 55);
  if (!acquired) {
    return false;
  }

  const latestGeneratedAt = await readHomepageSnapshotGeneratedAt(opts.db);
  if (latestGeneratedAt !== null && isSameMinute(latestGeneratedAt, opts.now)) {
    return false;
  }

  await refreshPublicHomepageSnapshot(opts);
  return true;
}

export async function refreshPublicHomepageArtifactSnapshotIfNeeded(opts: {
  db: D1Database;
  now: number;
  compute: () => Promise<unknown>;
}): Promise<boolean> {
  const generatedAt = await readHomepageArtifactSnapshotGeneratedAt(opts.db);
  if (generatedAt !== null && isSameMinute(generatedAt, opts.now)) {
    return false;
  }

  const acquired = await acquireLease(opts.db, REFRESH_LOCK_NAME, opts.now, 55);
  if (!acquired) {
    return false;
  }

  const latestGeneratedAt = await readHomepageArtifactSnapshotGeneratedAt(opts.db);
  if (latestGeneratedAt !== null && isSameMinute(latestGeneratedAt, opts.now)) {
    return false;
  }

  await refreshPublicHomepageArtifactSnapshot(opts);
  return true;
}

type HomepageSnapshotSiteSettings = {
  site_title: string;
  site_description: string;
  site_locale: 'auto' | 'en' | 'zh-CN' | 'zh-TW' | 'ja' | 'es';
  site_timezone: string;
  uptime_rating_level: 1 | 2 | 3 | 4 | 5;
};

const HOMEPAGE_DATA_SNAPSHOT_SQL = `
  WITH
    active_maintenance AS (
      SELECT DISTINCT mwm.monitor_id
      FROM maintenance_window_monitors mwm
      JOIN maintenance_windows mw ON mw.id = mwm.maintenance_window_id
      WHERE mw.starts_at <= ?1 AND mw.ends_at > ?1
    ),
    visible_monitors AS (
      SELECT
        m.id,
        m.name,
        m.type,
        m.group_name,
        m.group_sort_order,
        m.sort_order,
        m.interval_sec,
        COALESCE(s.status, 'unknown') AS state_status,
        s.last_checked_at,
        CASE WHEN am.monitor_id IS NULL THEN 0 ELSE 1 END AS in_maintenance
      FROM monitors m
      LEFT JOIN monitor_state s ON s.monitor_id = m.id
      LEFT JOIN active_maintenance am ON am.monitor_id = m.id
      WHERE m.is_active = 1
        AND m.show_on_status_page = 1
    ),
    presentation AS (
      SELECT
        vm.*,
        CASE
          WHEN in_maintenance = 1 OR state_status = 'maintenance' THEN 'maintenance'
          WHEN state_status = 'paused' THEN 'paused'
          WHEN state_status = 'down'
            AND last_checked_at IS NOT NULL
            AND ?1 - last_checked_at <= interval_sec * 2
          THEN 'down'
          WHEN state_status = 'up'
            AND last_checked_at IS NOT NULL
            AND ?1 - last_checked_at <= interval_sec * 2
          THEN 'up'
          ELSE 'unknown'
        END AS status
      FROM visible_monitors vm
    ),
    summary AS (
      SELECT
        COUNT(*) AS monitor_count_total,
        COALESCE(SUM(status = 'up'), 0) AS up,
        COALESCE(SUM(status = 'down'), 0) AS down,
        COALESCE(SUM(status = 'maintenance'), 0) AS maintenance,
        COALESCE(SUM(status = 'paused'), 0) AS paused,
        COALESCE(SUM(status = 'unknown'), 0) AS unknown
      FROM presentation
    ),
    overall AS (
      SELECT
        CASE
          WHEN down > 0 THEN 'down'
          WHEN unknown > 0 THEN 'unknown'
          WHEN maintenance > 0 THEN 'maintenance'
          WHEN up > 0 THEN 'up'
          WHEN paused > 0 THEN 'paused'
          ELSE 'unknown'
        END AS overall_status
      FROM summary
    ),
    hb_rows AS (
      SELECT
        cr.monitor_id,
        cr.checked_at,
        cr.latency_ms,
        CASE cr.status
          WHEN 'up' THEN 'u'
          WHEN 'down' THEN 'd'
          WHEN 'maintenance' THEN 'm'
          ELSE 'x'
        END AS code,
        ROW_NUMBER() OVER (
          PARTITION BY cr.monitor_id
          ORDER BY cr.checked_at DESC, cr.id DESC
        ) AS rn
      FROM check_results cr
      WHERE cr.monitor_id IN (SELECT id FROM presentation)
    ),
    hb AS (
      SELECT
        monitor_id,
        json_group_array(checked_at) AS checked_at_json,
        json_group_array(latency_ms) AS latency_json,
        group_concat(code, '') AS status_codes
      FROM (
        SELECT monitor_id, checked_at, latency_ms, code
        FROM hb_rows
        WHERE rn <= 60
        ORDER BY monitor_id, checked_at DESC, rn ASC
      )
      GROUP BY monitor_id
    ),
    rollup_range AS (
      SELECT
        (CAST(?1 / 86400 AS INTEGER) * 86400) AS end_day_start,
        (CAST(?1 / 86400 AS INTEGER) * 86400) - (30 * 86400) AS start_day_start
    ),
    rollup_rows AS (
      SELECT
        r.monitor_id,
        r.day_start_at,
        r.downtime_sec,
        r.unknown_sec,
        CASE
          WHEN r.total_sec > 0
          THEN CAST(round((r.uptime_sec * 100000.0) / r.total_sec) AS INTEGER)
          ELSE NULL
        END AS uptime_pct_milli,
        r.total_sec,
        r.uptime_sec
      FROM monitor_daily_rollups r
      JOIN rollup_range rr
      WHERE r.monitor_id IN (SELECT id FROM presentation)
        AND r.day_start_at >= rr.start_day_start
        AND r.day_start_at < rr.end_day_start
    ),
    rollup AS (
      SELECT
        monitor_id,
        json_group_array(day_start_at) AS day_start_at_json,
        json_group_array(downtime_sec) AS downtime_sec_json,
        json_group_array(unknown_sec) AS unknown_sec_json,
        json_group_array(uptime_pct_milli) AS uptime_pct_milli_json,
        SUM(total_sec) AS total_sec_sum,
        SUM(uptime_sec) AS uptime_sec_sum
      FROM (
        SELECT *
        FROM rollup_rows
        ORDER BY monitor_id, day_start_at
      )
      GROUP BY monitor_id
    ),
    monitor_cards AS (
      SELECT
        p.id,
        json_object(
          'id', p.id,
          'name', p.name,
          'type', CASE WHEN p.type = 'tcp' THEN 'tcp' ELSE 'http' END,
          'group_name',
            CASE
              WHEN p.group_name IS NULL OR trim(p.group_name) = '' THEN NULL
              ELSE trim(p.group_name)
            END,
          'status', p.status,
          'is_stale',
            CASE
              WHEN p.in_maintenance = 1 OR p.state_status IN ('paused', 'maintenance')
              THEN json('false')
              WHEN p.last_checked_at IS NULL THEN json('true')
              WHEN ?1 - p.last_checked_at > p.interval_sec * 2 THEN json('true')
              ELSE json('false')
            END,
          'last_checked_at', p.last_checked_at,
          'heartbeat_strip', json_object(
            'checked_at', json(COALESCE(hb.checked_at_json, '[]')),
            'status_codes', COALESCE(hb.status_codes, ''),
            'latency_ms', json(COALESCE(hb.latency_json, '[]'))
          ),
          'uptime_30d',
            CASE
              WHEN rollup.total_sec_sum IS NULL OR rollup.total_sec_sum = 0 THEN NULL
              ELSE json_object(
                'uptime_pct',
                  (rollup.uptime_sec_sum * 100.0) / rollup.total_sec_sum
              )
            END,
          'uptime_day_strip', json_object(
            'day_start_at', json(COALESCE(rollup.day_start_at_json, '[]')),
            'downtime_sec', json(COALESCE(rollup.downtime_sec_json, '[]')),
            'unknown_sec', json(COALESCE(rollup.unknown_sec_json, '[]')),
            'uptime_pct_milli', json(COALESCE(rollup.uptime_pct_milli_json, '[]'))
          )
        ) AS monitor_json
      FROM presentation p
      LEFT JOIN hb ON hb.monitor_id = p.id
      LEFT JOIN rollup ON rollup.monitor_id = p.id
      ORDER BY
        p.group_sort_order ASC,
        lower(
          CASE
            WHEN p.group_name IS NULL OR trim(p.group_name) = '' THEN 'Ungrouped'
            ELSE trim(p.group_name)
          END
        ) ASC,
        p.sort_order ASC,
        p.id ASC
    ),
    monitors_json AS (
      SELECT json_group_array(json(monitor_json)) AS monitors
      FROM monitor_cards
    ),
    visible_active_incidents AS (
      SELECT
        id,
        title,
        status,
        impact,
        message,
        started_at,
        resolved_at,
        CASE impact
          WHEN 'critical' THEN 3
          WHEN 'major' THEN 2
          WHEN 'minor' THEN 1
          ELSE 0
        END AS impact_rank
      FROM incidents
      WHERE status != 'resolved'
        AND (
          NOT EXISTS (
            SELECT 1
            FROM incident_monitors scoped_links
            WHERE scoped_links.incident_id = incidents.id
          )
          OR EXISTS (
            SELECT 1
            FROM incident_monitors scoped_links
            JOIN monitors scoped_monitors ON scoped_monitors.id = scoped_links.monitor_id
            WHERE scoped_links.incident_id = incidents.id
              AND scoped_monitors.show_on_status_page = 1
          )
        )
      ORDER BY started_at DESC, id DESC
      LIMIT 20
    ),
    active_incidents_json AS (
      SELECT json_group_array(
        json_object(
          'id', id,
          'title', title,
          'status', status,
          'impact', impact,
          'message', message,
          'started_at', started_at,
          'resolved_at', resolved_at
        )
      ) AS incidents
      FROM visible_active_incidents
    ),
    active_maintenance_windows AS (
      SELECT
        mw.id,
        mw.title,
        mw.message,
        mw.starts_at,
        mw.ends_at,
        (
          SELECT json_group_array(mwm.monitor_id)
          FROM maintenance_window_monitors mwm
          JOIN monitors m2 ON m2.id = mwm.monitor_id
          WHERE mwm.maintenance_window_id = mw.id
            AND m2.show_on_status_page = 1
          ORDER BY mwm.monitor_id
        ) AS monitor_ids
      FROM maintenance_windows mw
      WHERE mw.starts_at <= ?1 AND mw.ends_at > ?1
        AND (
          NOT EXISTS (
            SELECT 1
            FROM maintenance_window_monitors scoped_links
            WHERE scoped_links.maintenance_window_id = mw.id
          )
          OR EXISTS (
            SELECT 1
            FROM maintenance_window_monitors scoped_links
            JOIN monitors scoped_monitors ON scoped_monitors.id = scoped_links.monitor_id
            WHERE scoped_links.maintenance_window_id = mw.id
              AND scoped_monitors.show_on_status_page = 1
          )
        )
      ORDER BY mw.starts_at ASC, mw.id ASC
      LIMIT 20
    ),
    active_maintenance_json AS (
      SELECT json_group_array(
        json_object(
          'id', id,
          'title', title,
          'message', message,
          'starts_at', starts_at,
          'ends_at', ends_at,
          'monitor_ids', json(COALESCE(monitor_ids, '[]'))
        )
      ) AS windows
      FROM active_maintenance_windows
    ),
    upcoming_maintenance_windows AS (
      SELECT
        mw.id,
        mw.title,
        mw.message,
        mw.starts_at,
        mw.ends_at,
        (
          SELECT json_group_array(mwm.monitor_id)
          FROM maintenance_window_monitors mwm
          JOIN monitors m2 ON m2.id = mwm.monitor_id
          WHERE mwm.maintenance_window_id = mw.id
            AND m2.show_on_status_page = 1
          ORDER BY mwm.monitor_id
        ) AS monitor_ids
      FROM maintenance_windows mw
      WHERE mw.starts_at > ?1
        AND (
          NOT EXISTS (
            SELECT 1
            FROM maintenance_window_monitors scoped_links
            WHERE scoped_links.maintenance_window_id = mw.id
          )
          OR EXISTS (
            SELECT 1
            FROM maintenance_window_monitors scoped_links
            JOIN monitors scoped_monitors ON scoped_monitors.id = scoped_links.monitor_id
            WHERE scoped_links.maintenance_window_id = mw.id
              AND scoped_monitors.show_on_status_page = 1
          )
        )
      ORDER BY mw.starts_at ASC, mw.id ASC
      LIMIT 20
    ),
    upcoming_maintenance_json AS (
      SELECT json_group_array(
        json_object(
          'id', id,
          'title', title,
          'message', message,
          'starts_at', starts_at,
          'ends_at', ends_at,
          'monitor_ids', json(COALESCE(monitor_ids, '[]'))
        )
      ) AS windows
      FROM upcoming_maintenance_windows
    ),
    resolved_incident_preview_json AS (
      SELECT json_object(
        'id', id,
        'title', title,
        'status', status,
        'impact', impact,
        'message', message,
        'started_at', started_at,
        'resolved_at', resolved_at
      ) AS incident
      FROM incidents
      WHERE status = 'resolved'
        AND (
          NOT EXISTS (
            SELECT 1
            FROM incident_monitors scoped_links
            WHERE scoped_links.incident_id = incidents.id
          )
          OR EXISTS (
            SELECT 1
            FROM incident_monitors scoped_links
            JOIN monitors scoped_monitors ON scoped_monitors.id = scoped_links.monitor_id
            WHERE scoped_links.incident_id = incidents.id
              AND scoped_monitors.show_on_status_page = 1
          )
        )
      ORDER BY id DESC
      LIMIT 1
    ),
    maintenance_history_preview_json AS (
      SELECT json_object(
        'id', mw.id,
        'title', mw.title,
        'message', mw.message,
        'starts_at', mw.starts_at,
        'ends_at', mw.ends_at,
        'monitor_ids', json(COALESCE(mw.monitor_ids, '[]'))
      ) AS window
      FROM (
        SELECT
          mw.id,
          mw.title,
          mw.message,
          mw.starts_at,
          mw.ends_at,
          (
            SELECT json_group_array(mwm.monitor_id)
            FROM maintenance_window_monitors mwm
            JOIN monitors m2 ON m2.id = mwm.monitor_id
            WHERE mwm.maintenance_window_id = mw.id
              AND m2.show_on_status_page = 1
            ORDER BY mwm.monitor_id
          ) AS monitor_ids
        FROM maintenance_windows mw
        WHERE mw.ends_at <= ?1
          AND (
            NOT EXISTS (
              SELECT 1
              FROM maintenance_window_monitors scoped_links
              WHERE scoped_links.maintenance_window_id = mw.id
            )
            OR EXISTS (
              SELECT 1
              FROM maintenance_window_monitors scoped_links
              JOIN monitors scoped_monitors ON scoped_monitors.id = scoped_links.monitor_id
              WHERE scoped_links.maintenance_window_id = mw.id
                AND scoped_monitors.show_on_status_page = 1
            )
          )
        ORDER BY mw.id DESC
        LIMIT 1
      ) mw
    ),
    banner_json AS (
      SELECT
        CASE
          WHEN (SELECT COUNT(*) FROM visible_active_incidents) > 0
          THEN (
            WITH max_rank AS (
              SELECT COALESCE(MAX(impact_rank), 0) AS r
              FROM visible_active_incidents
            ),
            top_inc AS (
              SELECT id, title, status, impact
              FROM visible_active_incidents
              ORDER BY started_at DESC, id DESC
              LIMIT 1
            )
            SELECT json_object(
              'source', 'incident',
              'status',
                CASE
                  WHEN (SELECT r FROM max_rank) >= 2 THEN 'major_outage'
                  WHEN (SELECT r FROM max_rank) = 1 THEN 'partial_outage'
                  ELSE 'operational'
                END,
              'title',
                CASE
                  WHEN (SELECT r FROM max_rank) >= 2 THEN 'Major Outage'
                  WHEN (SELECT r FROM max_rank) = 1 THEN 'Partial Outage'
                  ELSE 'Incident'
                END,
              'incident', (
                SELECT json_object(
                  'id', id,
                  'title', title,
                  'status', status,
                  'impact', impact
                )
                FROM top_inc
              )
            )
          )
          WHEN (SELECT down FROM summary) > 0
          THEN json_object(
            'source', 'monitors',
            'status',
              CASE
                WHEN (SELECT monitor_count_total FROM summary) = 0 THEN 'partial_outage'
                WHEN (CAST((SELECT down FROM summary) AS REAL) / (SELECT monitor_count_total FROM summary)) >= 0.3
                THEN 'major_outage'
                ELSE 'partial_outage'
              END,
            'title',
              CASE
                WHEN (SELECT monitor_count_total FROM summary) > 0
                  AND (CAST((SELECT down FROM summary) AS REAL) / (SELECT monitor_count_total FROM summary)) >= 0.3
                THEN 'Major Outage'
                ELSE 'Partial Outage'
              END,
            'down_ratio',
              CASE
                WHEN (SELECT monitor_count_total FROM summary) = 0 THEN 0
                ELSE (CAST((SELECT down FROM summary) AS REAL) / (SELECT monitor_count_total FROM summary))
              END
          )
          WHEN (SELECT unknown FROM summary) > 0
          THEN json_object(
            'source', 'monitors',
            'status', 'unknown',
            'title', 'Status Unknown'
          )
          WHEN (SELECT COUNT(*) FROM active_maintenance_windows) > 0
          THEN (
            WITH top_mw AS (
              SELECT id, title, starts_at, ends_at
              FROM active_maintenance_windows
              ORDER BY starts_at ASC, id ASC
              LIMIT 1
            )
            SELECT json_object(
              'source', 'maintenance',
              'status', 'maintenance',
              'title', 'Maintenance',
              'maintenance_window', (
                SELECT json_object(
                  'id', id,
                  'title', title,
                  'starts_at', starts_at,
                  'ends_at', ends_at
                )
                FROM top_mw
              )
            )
          )
          WHEN (SELECT maintenance FROM summary) > 0
          THEN json_object(
            'source', 'monitors',
            'status', 'maintenance',
            'title', 'Maintenance'
          )
          ELSE json_object(
            'source', 'monitors',
            'status', 'operational',
            'title', 'All Systems Operational'
          )
        END AS banner
    ),
    homepage_json AS (
      SELECT json_object(
        'generated_at', ?1,
        'bootstrap_mode', 'full',
        'monitor_count_total', (SELECT monitor_count_total FROM summary),
        'site_title', ?2,
        'site_description', ?3,
        'site_locale', ?4,
        'site_timezone', ?5,
        'uptime_rating_level', ?6,
        'overall_status', (SELECT overall_status FROM overall),
        'banner', json((SELECT banner FROM banner_json)),
        'summary', json_object(
          'up', (SELECT up FROM summary),
          'down', (SELECT down FROM summary),
          'maintenance', (SELECT maintenance FROM summary),
          'paused', (SELECT paused FROM summary),
          'unknown', (SELECT unknown FROM summary)
        ),
        'monitors', json(COALESCE((SELECT monitors FROM monitors_json), '[]')),
        'active_incidents', json(COALESCE((SELECT incidents FROM active_incidents_json), '[]')),
        'maintenance_windows', json_object(
          'active', json(COALESCE((SELECT windows FROM active_maintenance_json), '[]')),
          'upcoming', json(COALESCE((SELECT windows FROM upcoming_maintenance_json), '[]'))
        ),
        'resolved_incident_preview', json((SELECT incident FROM resolved_incident_preview_json)),
        'maintenance_history_preview', json((SELECT window FROM maintenance_history_preview_json))
      ) AS body_json
    )
  INSERT INTO public_snapshots (key, generated_at, body_json, updated_at)
  SELECT '${SNAPSHOT_KEY}', ?1, body_json, ?1
  FROM homepage_json
  WHERE 1 = 1
  ON CONFLICT(key) DO UPDATE SET
    generated_at = excluded.generated_at,
    body_json = excluded.body_json,
    updated_at = excluded.updated_at
`;

export const __testOnly_homepageDataSnapshotSql = HOMEPAGE_DATA_SNAPSHOT_SQL;

export async function refreshPublicHomepageSnapshotSqlIfNeeded(opts: {
  db: D1Database;
  now: number;
  settings: HomepageSnapshotSiteSettings;
}): Promise<boolean> {
  const generatedAt = await readHomepageSnapshotGeneratedAt(opts.db);
  if (generatedAt !== null && isSameMinute(generatedAt, opts.now)) {
    return false;
  }

  const acquired = await acquireLease(opts.db, REFRESH_DATA_SQL_LOCK_NAME, opts.now, 55);
  if (!acquired) {
    return false;
  }

  const latestGeneratedAt = await readHomepageSnapshotGeneratedAt(opts.db);
  if (latestGeneratedAt !== null && isSameMinute(latestGeneratedAt, opts.now)) {
    return false;
  }

  await opts.db
    .prepare(HOMEPAGE_DATA_SNAPSHOT_SQL)
    .bind(
      opts.now,
      opts.settings.site_title,
      opts.settings.site_description,
      opts.settings.site_locale,
      opts.settings.site_timezone,
      opts.settings.uptime_rating_level,
    )
    .run();

  return true;
}
