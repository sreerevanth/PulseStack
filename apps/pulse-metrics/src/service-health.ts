// ── Inline Types (no external package imports needed for testability) ──────────

type ExecutionRecord = {
  id: string;
  workflow_id: string;
  tenant_id: string;
  correlation_id: string;
  status: string;
  input: Record<string, unknown>;
  output: Record<string, unknown>;
  created_at: string;
  updated_at: string;
};

type PgQueryResult<T> = { rows: T[] };
type ClickHouseQueryResult<T> = { json: () => Promise<T[]> };

type PulseInfraLike = {
  pg: {
    query: <T>(text: string, params?: unknown[]) => Promise<PgQueryResult<T>>;
  };
  clickhouse: {
    query: (opts: { query: string; query_params?: Record<string, unknown>; format: string }) => Promise<ClickHouseQueryResult<Record<string, unknown>>>;
  };
  readMetrics: (tenantId?: string) => Promise<unknown>;
  readRecentEvents: (limit: number, tenantId?: string) => Promise<unknown>;
};

// ── Exported Types ─────────────────────────────────────────────────────────────

export type ServiceHealthStatus = 'healthy' | 'degraded' | 'unhealthy' | 'unknown' | 'down';

export type ServiceHealthIndicator = {
  status: ServiceHealthStatus;
  label: string;
  description: string;
  score: number; // 0-100
};

export type ServiceStatusCard = {
  serviceId: string;
  serviceName: string;
  status: ServiceHealthStatus;
  uptimePercent: number;
  responseTimeMs: number;
  executionCount: number;
  successCount: number;
  failureCount: number;
  lastCheckedAt: string;
  lastFailureAt?: string;
  trends: {
    status24h: ServiceHealthStatus[];
    executionTrend: 'increasing' | 'decreasing' | 'stable';
    responseTimeTrend: 'improving' | 'degrading' | 'stable';
    uptimeTrend: 'improving' | 'degrading' | 'stable';
  };
  metadata: {
    totalExecutions: number;
    avgDuration: number;
    p99Latency: number;
    errorRate: number;
    concurrentExecutions: number;
  };
};

export type ServiceHealthDashboard = {
  tenantId?: string;
  generatedAt: string;
  autoRefreshInterval: number; // seconds
  overallHealth: ServiceHealthIndicator;
  services: ServiceStatusCard[];
  summary: {
    totalServices: number;
    healthyCount: number;
    degradedCount: number;
    unhealthyCount: number;
    downCount: number;
    unknownCount: number;
    overallUptime: number;
    avgResponseTime: number;
    totalExecutions: number;
    errorRate: number;
  };
  filters: {
    search: string;
    statusFilter: ServiceHealthStatus | 'all';
  };
};

export type ServiceHealthSnapshot = {
  timestamp: string;
  services: Array<{
    serviceId: string;
    status: ServiceHealthStatus;
    uptimePercent: number;
    responseTimeMs: number;
    errorRate: number;
  }>;
};

// ── Constants ──────────────────────────────────────────────────────────────────

const HEALTHY_THRESHOLD = 95; // 95% success rate or higher
const DEGRADED_THRESHOLD = 80; // 80-94% success rate
const AUTO_REFRESH_INTERVAL = 30; // seconds
const UPTIME_WINDOW_HOURS = 24;
const STATUS_HISTORY_SLOTS = 24; // hourly slots for 24h trend

// ── Service Health Analyzer ─────────────────────────────────────────────────────

export class ServiceHealthAnalyzer {
  constructor(private infra: PulseInfraLike) {}

  /**
   * Get the full health dashboard with all service status cards
   */
  async getDashboard(tenantId?: string): Promise<ServiceHealthDashboard> {
    const [executions, metricsData] = await Promise.all([
      this.getExecutions(tenantId),
      tenantId ? this.infra.readMetrics(tenantId) : this.infra.readMetrics(),
    ]);

    const services = this.buildServiceStatusCards(executions, tenantId);
    const overallHealth = this.computeOverallHealth(services);
    const summary = this.computeSummary(services);

    return {
      tenantId,
      generatedAt: new Date().toISOString(),
      autoRefreshInterval: AUTO_REFRESH_INTERVAL,
      overallHealth,
      services,
      summary,
      filters: {
        search: '',
        statusFilter: 'all',
      },
    };
  }

  /**
   * Get a specific service's health card
   */
  async getServiceHealth(
    serviceId: string,
    tenantId?: string,
  ): Promise<ServiceStatusCard | null> {
    const executions = await this.getExecutions(tenantId, serviceId);
    if (executions.length === 0) return null;

    const cards = this.buildServiceStatusCards(executions, tenantId);
    return cards.find((c) => c.serviceId === serviceId) ?? null;
  }

  /**
   * Get health snapshot for monitoring/auto-refresh
   */
  async getSnapshot(tenantId?: string): Promise<ServiceHealthSnapshot> {
    const dashboard = await this.getDashboard(tenantId);
    return {
      timestamp: dashboard.generatedAt,
      services: dashboard.services.map((s) => ({
        serviceId: s.serviceId,
        status: s.status,
        uptimePercent: s.uptimePercent,
        responseTimeMs: s.responseTimeMs,
        errorRate: s.metadata.errorRate,
      })),
    };
  }

  /**
   * Search services by name pattern
   */
  async searchServices(
    query: string,
    tenantId?: string,
  ): Promise<ServiceStatusCard[]> {
    const dashboard = await this.getDashboard(tenantId);
    const lowerQuery = query.toLowerCase();
    return dashboard.services.filter(
      (s) =>
        s.serviceId.toLowerCase().includes(lowerQuery) ||
        s.serviceName.toLowerCase().includes(lowerQuery),
    );
  }

  /**
   * Filter services by health status
   */
  async filterByStatus(
    status: ServiceHealthStatus | 'all',
    tenantId?: string,
  ): Promise<ServiceStatusCard[]> {
    const dashboard = await this.getDashboard(tenantId);
    if (status === 'all') return dashboard.services;
    return dashboard.services.filter((s) => s.status === status);
  }

  // ── Data Access ──────────────────────────────────────────────────────────────

  private async getExecutions(
    tenantId?: string,
    workflowId?: string,
  ): Promise<ExecutionRecord[]> {
    let query: string;
    const params: unknown[] = [];

    if (tenantId && workflowId) {
      query =
        'select * from executions where tenant_id = $1 and workflow_id = $2 order by created_at desc';
      params.push(tenantId, workflowId);
    } else if (tenantId) {
      query =
        'select * from executions where tenant_id = $1 order by created_at desc';
      params.push(tenantId);
    } else if (workflowId) {
      query =
        'select * from executions where workflow_id = $1 order by created_at desc';
      params.push(workflowId);
    } else {
      query = 'select * from executions order by created_at desc';
    }

    const result = await this.infra.pg.query<ExecutionRecord>(query, params);
    return result.rows;
  }

  // ── Health Analysis ───────────────────────────────────────────────────────────

  private buildServiceStatusCards(
    executions: ExecutionRecord[],
    tenantId?: string,
  ): ServiceStatusCard[] {
    const workflowMap = this.groupByWorkflow(executions);

    return Array.from(workflowMap.entries()).map(([workflowId, execs]) =>
      this.buildCard(workflowId, execs),
    );
  }

  private groupByWorkflow(
    executions: ExecutionRecord[],
  ): Map<string, ExecutionRecord[]> {
    const map = new Map<string, ExecutionRecord[]>();
    for (const exec of executions) {
      const existing = map.get(exec.workflow_id) ?? [];
      existing.push(exec);
      map.set(exec.workflow_id, existing);
    }
    return map;
  }

  private buildCard(
    workflowId: string,
    executions: ExecutionRecord[],
  ): ServiceStatusCard {
    const now = new Date();
    const total = executions.length;
    const succeeded = executions.filter((e) =>
      ['completed', 'success', 'succeeded'].includes(e.status),
    ).length;
    const failed = executions.filter((e) =>
      ['failed', 'error'].includes(e.status),
    ).length;
    const successRate = total > 0 ? succeeded / total : 1;
    const errorRate = total > 0 ? 1 - successRate : 0;

    // Determine status
    const status = this.determineHealthStatus(successRate);

    // Compute uptime (percentage of succeeded executions in the window)
    const uptimePercent = Math.round(successRate * 100 + Number.EPSILON);

    // Compute average response time
    const durations = executions.map(
      (e) =>
        new Date(e.updated_at).getTime() - new Date(e.created_at).getTime(),
    );
    const avgDuration =
      durations.length > 0
        ? durations.reduce((s, d) => s + d, 0) / durations.length
        : 0;

    // P99 latency
    const sortedDurations = [...durations].sort((a, b) => a - b);
    const p99Index = Math.ceil(sortedDurations.length * 0.99) - 1;
    const p99Latency =
      p99Index >= 0 ? sortedDurations[p99Index] : avgDuration;

    // Last failure
    const lastFailure = executions.find((e) =>
      ['failed', 'error'].includes(e.status),
    );

    // Status history for 24h trend
    const statusHistory = this.computeStatusHistory(executions);

    // Trends
    const executionTrend = this.computeExecutionTrend(executions);
    const responseTimeTrend = this.computeResponseTimeTrend(durations);
    const uptimeTrend = statusHistory.length >= 2 ? this.computeUptimeTrend(executions) : 'stable';

    // Concurrent executions (recent ones within the last minute)
    const recentExecs = executions.filter(
      (e) =>
        new Date(e.created_at).getTime() > now.getTime() - 60 * 1000,
    ).length;

    return {
      serviceId: workflowId,
      serviceName: workflowId,
      status,
      uptimePercent,
      responseTimeMs: Math.round(avgDuration),
      executionCount: total,
      successCount: succeeded,
      failureCount: failed,
      lastCheckedAt: now.toISOString(),
      lastFailureAt: lastFailure?.updated_at,
      trends: {
        status24h: statusHistory.slice(-24),
        executionTrend,
        responseTimeTrend,
        uptimeTrend,
      },
      metadata: {
        totalExecutions: total,
        avgDuration: Math.round(avgDuration),
        p99Latency: Math.round(p99Latency),
        errorRate: Math.round(errorRate * 10000) / 100, // as percentage
        concurrentExecutions: recentExecs,
      },
    };
  }

  private determineHealthStatus(successRate: number): ServiceHealthStatus {
    if (successRate >= HEALTHY_THRESHOLD / 100) return 'healthy';
    if (successRate >= DEGRADED_THRESHOLD / 100) return 'degraded';
    if (successRate > 0) return 'unhealthy';
    return 'down';
  }

  private computeOverallHealth(
    cards: ServiceStatusCard[],
  ): ServiceHealthIndicator {
    if (cards.length === 0) {
      return {
        status: 'unknown',
        label: 'No Data',
        description: 'No services are being monitored',
        score: 0,
      };
    }

    // Weighted score based on uptime and error rates
    const totalScore = cards.reduce((sum, card) => {
      const uptimeWeight = 0.6;
      const errorWeight = 0.4;
      const errorScore = Math.max(0, 100 - card.metadata.errorRate * 10);
      return sum + card.uptimePercent * uptimeWeight + errorScore * errorWeight;
    }, 0);
    const avgScore = Math.round(totalScore / cards.length);

    const status = this.determineHealthStatus(avgScore / 100);

    const healthyCount = cards.filter((c) => c.status === 'healthy').length;
    const degradedCount = cards.filter((c) => c.status === 'degraded').length;
    const unhealthyCount = cards.filter((c) => c.status === 'unhealthy' || c.status === 'down').length;

    let description: string;
    if (status === 'healthy') {
      description = `All systems operational - ${healthyCount}/${cards.length} services healthy`;
    } else if (status === 'degraded') {
      description = `System degraded - ${degradedCount} service(s) degraded, ${unhealthyCount} unhealthy`;
    } else {
      description = `System issues detected - ${unhealthyCount} service(s) unhealthy`;
    }

    return {
      status,
      label: status.charAt(0).toUpperCase() + status.slice(1),
      description,
      score: avgScore,
    };
  }

  private computeSummary(cards: ServiceStatusCard[]) {
    const total = cards.length;
    const healthyCount = cards.filter((c) => c.status === 'healthy').length;
    const degradedCount = cards.filter((c) => c.status === 'degraded').length;
    const unhealthyCount = cards.filter((c) => c.status === 'unhealthy').length;
    const downCount = cards.filter((c) => c.status === 'down').length;
    const unknownCount = cards.filter((c) => c.status === 'unknown').length;
    const overallUptime =
      total > 0
        ? Math.round(
            cards.reduce((s, c) => s + c.uptimePercent, 0) / total,
          )
        : 0;
    const avgResponseTime =
      total > 0
        ? Math.round(
            cards.reduce((s, c) => s + c.responseTimeMs, 0) / total,
          )
        : 0;
    const totalExecs = cards.reduce((s, c) => s + c.executionCount, 0);
    const errorRate =
      total > 0
        ? Math.round(
            (cards.reduce((s, c) => s + c.metadata.errorRate, 0) / total) *
              100,
          ) / 100
        : 0;

    return {
      totalServices: total,
      healthyCount,
      degradedCount,
      unhealthyCount,
      downCount,
      unknownCount,
      overallUptime,
      avgResponseTime,
      totalExecutions: totalExecs,
      errorRate,
    };
  }

  private computeStatusHistory(
    executions: ExecutionRecord[],
  ): ServiceHealthStatus[] {
    const now = new Date();
    const slots: ServiceHealthStatus[] = [];

    for (let i = STATUS_HISTORY_SLOTS - 1; i >= 0; i--) {
      const slotStart = new Date(now.getTime() - (i + 1) * 60 * 60 * 1000);
      const slotEnd = new Date(now.getTime() - i * 60 * 60 * 1000);

      const slotExecs = executions.filter((e) => {
        const t = new Date(e.created_at).getTime();
        return t >= slotStart.getTime() && t < slotEnd.getTime();
      });

      if (slotExecs.length === 0) {
        slots.push('unknown');
      } else {
        const succeeded = slotExecs.filter((e) =>
          ['completed', 'success', 'succeeded'].includes(e.status),
        ).length;
        const rate = succeeded / slotExecs.length;
        slots.push(this.determineHealthStatus(rate));
      }
    }

    return slots;
  }

  private computeExecutionTrend(
    executions: ExecutionRecord[],
  ): 'increasing' | 'decreasing' | 'stable' {
    const now = new Date();
    const midPoint = new Date(now.getTime() - 12 * 60 * 60 * 1000);

    const recent = executions.filter(
      (e) => new Date(e.created_at).getTime() >= midPoint.getTime(),
    ).length;
    const older = executions.filter(
      (e) => new Date(e.created_at).getTime() < midPoint.getTime(),
    ).length;

    if (older === 0 && recent > 0) return 'increasing';
    if (older === 0) return 'stable';

    const ratio = recent / older;
    if (ratio > 1.2) return 'increasing';
    if (ratio < 0.8) return 'decreasing';
    return 'stable';
  }

  private computeResponseTimeTrend(
    durations: number[],
  ): 'improving' | 'degrading' | 'stable' {
    if (durations.length < 4) return 'stable';

    const mid = Math.floor(durations.length / 2);
    const firstHalf =
      durations.slice(0, mid).reduce((s, d) => s + d, 0) / mid;
    const secondHalf =
      durations.slice(mid).reduce((s, d) => s + d, 0) / (durations.length - mid);

    if (secondHalf < firstHalf * 0.9) return 'improving';
    if (secondHalf > firstHalf * 1.1) return 'degrading';
    return 'stable';
  }

  private computeUptimeTrend(
    executions: ExecutionRecord[],
  ): 'improving' | 'degrading' | 'stable' {
    const now = new Date();
    const midPoint = new Date(now.getTime() - 12 * 60 * 60 * 1000);

    const recent = executions.filter(
      (e) => new Date(e.created_at).getTime() >= midPoint.getTime(),
    );
    const older = executions.filter(
      (e) => new Date(e.created_at).getTime() < midPoint.getTime(),
    );

    if (older.length === 0 && recent.length > 0) return 'improving';
    if (older.length === 0) return 'stable';

    const recentRate =
      recent.length > 0
        ? recent.filter((e) =>
            ['completed', 'success', 'succeeded'].includes(e.status),
          ).length / recent.length
        : 0;
    const olderRate =
      older.length > 0
        ? older.filter((e) =>
            ['completed', 'success', 'succeeded'].includes(e.status),
          ).length / older.length
        : 0;

    if (recentRate > olderRate * 1.05) return 'improving';
    if (recentRate < olderRate * 0.95) return 'degrading';
    return 'stable';
  }
}