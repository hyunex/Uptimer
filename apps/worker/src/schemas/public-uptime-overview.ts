import { z } from 'zod';

export const publicUptimeOverviewRangeSchema = z.enum(['30d', '90d']);

const uptimeTotalsSchema = z.object({
  total_sec: z.number().int().nonnegative(),
  downtime_sec: z.number().int().nonnegative(),
  unknown_sec: z.number().int().nonnegative(),
  uptime_sec: z.number().int().nonnegative(),
  uptime_pct: z.number().min(0).max(100),
});

const publicUptimeOverviewMonitorSchema = z.object({
  id: z.number().int().positive(),
  name: z.string(),
  type: z.enum(['http', 'tcp']),
  total_sec: z.number().int().nonnegative(),
  downtime_sec: z.number().int().nonnegative(),
  unknown_sec: z.number().int().nonnegative(),
  uptime_sec: z.number().int().nonnegative(),
  uptime_pct: z.number().min(0).max(100),
});

export const publicUptimeOverviewResponseSchema = z.object({
  generated_at: z.number().int().nonnegative(),
  range: publicUptimeOverviewRangeSchema,
  range_start_at: z.number().int().nonnegative(),
  range_end_at: z.number().int().nonnegative(),
  overall: uptimeTotalsSchema,
  monitors: z.array(publicUptimeOverviewMonitorSchema),
});

export type PublicUptimeOverviewRange = z.infer<typeof publicUptimeOverviewRangeSchema>;
export type PublicUptimeOverviewResponse = z.infer<typeof publicUptimeOverviewResponseSchema>;
