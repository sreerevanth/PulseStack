import { ReplayEngine, createBaseServer, loadEnv, PulseInfra, tenantIdFromHeaders } from '@pulsestack/core';

const env = loadEnv();
const infra = new PulseInfra();
const replay = new ReplayEngine(infra, 'pulse-trace');
const app = await createBaseServer('pulse-trace');

app.get('/executions/:executionId', async (request) => {
  const tenantId = tenantIdFromHeaders(
    request.headers as Record<string, string | string[] | undefined>,
    env.TENANT_ID,
  );
  return infra.readTrace((request.params as { executionId: string }).executionId, tenantId);
});

app.get('/search', async (request) => {
  const tenantId = tenantIdFromHeaders(
    request.headers as Record<string, string | string[] | undefined>,
    env.TENANT_ID,
  );
  const executionId = (request.query as { executionId?: string }).executionId;
  return executionId ? infra.readTrace(executionId, tenantId) : [];
});

app.get('/executions/:executionId/snapshots', async (request) => {
  const tenantId = tenantIdFromHeaders(
    request.headers as Record<string, string | string[] | undefined>,
    env.TENANT_ID,
  );
  const executionId = (request.params as { executionId: string }).executionId;
  const [spans, snapshots] = await Promise.all([
    infra.readTrace(executionId, tenantId),
    replay.inspectSnapshots(executionId, tenantId),
  ]);
  return { spans, snapshots };
});

await app.listen({ host: '0.0.0.0', port: env.HTTP_PORT });

.catch(err => console.error("Promise.all failed:", err));