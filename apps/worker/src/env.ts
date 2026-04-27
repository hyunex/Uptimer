export interface Env {
  DB: D1Database;
  ADMIN_TOKEN: string;

  // Optional internal service binding to self (configured by CI deploy).
  SELF?: Fetcher;

  // Optional dev-only trace secret. If set, trace headers are honored only when
  // callers present `X-Uptimer-Trace-Token`.
  UPTIMER_TRACE_TOKEN?: string;
  TRACE_TOKEN?: string;
  UPTIMER_HOMEPAGE_RESIDUAL_TRACE?: string;
  UPTIMER_SCHEDULED_STATUS_REFRESH?: string;

  // In-memory, per-instance rate limit for admin endpoints.
  // Keep optional so older deployments don't break.
  ADMIN_RATE_LIMIT_MAX?: string;
  ADMIN_RATE_LIMIT_WINDOW_SEC?: string;
}
