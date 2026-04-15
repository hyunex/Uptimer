// D1-backed settings helpers.
//
// - Storage: `settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)`
// - Values are stored as strings; this module parses them into typed values.

import { settingsPatchInputSchema, type SettingsPatchInput } from './schemas/settings';

import { AppError } from './middleware/errors';

export type SettingsResponse = {
  settings: {
    site_title: string;
    site_description: string;
    site_locale: 'auto' | 'en' | 'zh-CN' | 'zh-TW' | 'ja' | 'es';
    site_timezone: string;

    retention_check_results_days: number;

    state_failures_to_down_from_up: number;
    state_successes_to_up_from_down: number;

    admin_default_overview_range: '24h' | '7d';
    admin_default_monitor_range: '24h' | '7d' | '30d' | '90d';

    uptime_rating_level: 1 | 2 | 3 | 4 | 5;
  };
};

const DEFAULTS: SettingsResponse['settings'] = {
  site_title: 'Uptimer',
  site_description: '',
  site_locale: 'auto',
  site_timezone: 'UTC',

  retention_check_results_days: 7,

  state_failures_to_down_from_up: 2,
  state_successes_to_up_from_down: 2,

  admin_default_overview_range: '24h',
  admin_default_monitor_range: '24h',

  uptime_rating_level: 3,
};

type SettingsRow = { key: string; value: string };

const READ_SETTINGS_SQL = 'SELECT key, value FROM settings';
const UPSERT_SETTING_SQL = `
  INSERT INTO settings (key, value)
  VALUES (?1, ?2)
  ON CONFLICT(key) DO UPDATE SET value = excluded.value
`;

const readSettingsStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const patchSettingsStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();

const SETTINGS_CACHE_TTL_MS = 60_000;
const settingsCacheByDb = new WeakMap<
  D1Database,
  { fetchedAtMs: number; settings: SettingsResponse['settings'] }
>();

function parseIntSetting(
  raw: string | undefined,
  opts: { min: number; max: number },
): number | null {
  if (raw === undefined) return null;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n)) return null;
  if (n < opts.min || n > opts.max) return null;
  return n;
}

function parseStringSetting(
  raw: string | undefined,
  opts: { max: number; allowEmpty?: boolean },
): string | null {
  if (raw === undefined) return null;
  const s = raw;
  if (!opts.allowEmpty && s.length === 0) return null;
  if (s.length > opts.max) return null;
  return s;
}

function parseEnumSetting<T extends string>(
  raw: string | undefined,
  allowed: readonly T[],
): T | null {
  if (raw === undefined) return null;
  return (allowed as readonly string[]).includes(raw) ? (raw as T) : null;
}

export function parseSettingsPatch(rawBody: unknown): SettingsPatchInput {
  const r = settingsPatchInputSchema.safeParse(rawBody);
  if (!r.success) {
    throw new AppError(400, 'INVALID_ARGUMENT', r.error.message);
  }
  if (Object.keys(r.data).length === 0) {
    throw new AppError(400, 'INVALID_ARGUMENT', 'At least one field must be provided');
  }
  return r.data;
}

export async function readSettings(
  db: D1Database,
  opts: { bypassCache?: boolean } = {},
): Promise<SettingsResponse['settings']> {
  if (!opts.bypassCache) {
    const cachedResult = settingsCacheByDb.get(db);
    if (cachedResult && Date.now() - cachedResult.fetchedAtMs < SETTINGS_CACHE_TTL_MS) {
      return cachedResult.settings;
    }
  }

  const cached = readSettingsStatementByDb.get(db);
  const statement = cached ?? db.prepare(READ_SETTINGS_SQL);
  if (!cached) {
    readSettingsStatementByDb.set(db, statement);
  }

  const { results } = await statement.all<SettingsRow>();

  const map = new Map<string, string>();
  for (const r of results ?? []) {
    if (!r || typeof r.key !== 'string') continue;
    map.set(r.key, r.value);
  }

  const site_title = parseStringSetting(map.get('site_title'), { max: 100 }) ?? DEFAULTS.site_title;
  const site_description =
    parseStringSetting(map.get('site_description'), { max: 500, allowEmpty: true }) ??
    DEFAULTS.site_description;
  const site_locale =
    parseEnumSetting(map.get('site_locale'), [
      'auto',
      'en',
      'zh-CN',
      'zh-TW',
      'ja',
      'es',
    ] as const) ?? DEFAULTS.site_locale;
  const site_timezone =
    parseStringSetting(map.get('site_timezone'), { max: 64 }) ?? DEFAULTS.site_timezone;

  const retention_check_results_days =
    parseIntSetting(map.get('retention_check_results_days'), { min: 1, max: 365 }) ??
    DEFAULTS.retention_check_results_days;

  const state_failures_to_down_from_up =
    parseIntSetting(map.get('state_failures_to_down_from_up'), { min: 1, max: 10 }) ??
    DEFAULTS.state_failures_to_down_from_up;
  const state_successes_to_up_from_down =
    parseIntSetting(map.get('state_successes_to_up_from_down'), { min: 1, max: 10 }) ??
    DEFAULTS.state_successes_to_up_from_down;

  const admin_default_overview_range =
    parseEnumSetting(map.get('admin_default_overview_range'), ['24h', '7d'] as const) ??
    DEFAULTS.admin_default_overview_range;
  const admin_default_monitor_range =
    parseEnumSetting(map.get('admin_default_monitor_range'), [
      '24h',
      '7d',
      '30d',
      '90d',
    ] as const) ?? DEFAULTS.admin_default_monitor_range;

  const uptime_rating_level = (parseIntSetting(map.get('uptime_rating_level'), {
    min: 1,
    max: 5,
  }) ?? DEFAULTS.uptime_rating_level) as 1 | 2 | 3 | 4 | 5;

  const settings: SettingsResponse['settings'] = {
    site_title,
    site_description,
    site_locale,
    site_timezone,

    retention_check_results_days,

    state_failures_to_down_from_up,
    state_successes_to_up_from_down,

    admin_default_overview_range,
    admin_default_monitor_range,

    uptime_rating_level,
  };

  if (!opts.bypassCache) {
    settingsCacheByDb.set(db, { fetchedAtMs: Date.now(), settings });
  }
  return settings;
}

export async function patchSettings(db: D1Database, patch: SettingsPatchInput): Promise<void> {
  const statements: D1PreparedStatement[] = [];

  const cached = patchSettingsStatementByDb.get(db);
  const statement = cached ?? db.prepare(UPSERT_SETTING_SQL);
  if (!cached) {
    patchSettingsStatementByDb.set(db, statement);
  }

  for (const [key, value] of Object.entries(patch)) {
    if (value === undefined) continue;

    const strValue = typeof value === 'string' ? value : String(value);

    statements.push(statement.bind(key, strValue));
  }

  if (statements.length === 0) return;
  await db.batch(statements);
  settingsCacheByDb.delete(db);
}
