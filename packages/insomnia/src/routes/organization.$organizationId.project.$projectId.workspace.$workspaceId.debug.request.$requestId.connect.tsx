import type {
  ChangeBufferEvent,
  CookieJar,
  McpTransportType,
  RequestAuthentication,
  RequestBody,
  RequestHeader,
} from 'insomnia-data';
import { models, services } from 'insomnia-data';
import { href } from 'react-router';

import { invariant } from '~/common/utils/invariant';
import { AnalyticsEvent } from '~/ui/analytics';
import { openRealtimeConnection } from '~/ui/utils/open-realtime-connection';
import { createFetcherSubmitHook } from '~/ui/utils/router';

import type { Route } from './+types/organization.$organizationId.project.$projectId.workspace.$workspaceId.debug.request.$requestId.connect';

const { isRequestMeta } = models.requestMeta;

export interface ConnectActionParams {
  url: string;
  headers: RequestHeader[];
  authentication: RequestAuthentication;
  body?: RequestBody;
  cookieJar: CookieJar;
  suppressUserAgent: boolean;
  transportType?: McpTransportType;
  query?: Record<string, string>;
  path?: string;
  env?: Record<string, string>;
}

export async function clientAction({ params, request }: Route.ClientActionArgs) {
  const { requestId, workspaceId } = params;

  const req = await services.helpers.getRequestById(requestId);
  invariant(req, 'Request not found');
  invariant(workspaceId, 'Workspace ID is required');
  const rendered = (await request.json()) as ConnectActionParams;

  openRealtimeConnection({ req, workspaceId, rendered });
  if (models.mcpRequest.isMcpRequest(req)) {
    window.main.trackAnalyticsEvent({
      event: AnalyticsEvent.requestExecuted,
      properties: { request_type: 'MCP' },
    });
    return window.main.mcp.connect({
      requestId,
      workspaceId,
      transportType: rendered.transportType || models.mcpRequest.TRANSPORT_TYPES.HTTP,
      url: rendered.url,
      headers: rendered.headers,
      authentication: rendered.authentication,
      env: rendered.env || {},
    });
  }
  // HACK: even more elaborate hack to get the request to update
  return new Promise(resolve => {
    const unsubscribe = window.main.on('db.changes', async (_, changes: ChangeBufferEvent[]) => {
      for (const change of changes) {
        const [event, doc] = change;
        if (isRequestMeta(doc) && doc.parentId === requestId && event === 'update') {
          resolve(null);
          unsubscribe();
          return;
        }
      }
    });
  });
}

export const useRequestConnectActionFetcher = createFetcherSubmitHook(
  submit =>
    ({
      organizationId,
      projectId,
      workspaceId,
      requestId,
      connectParams,
    }: {
      organizationId: string;
      projectId: string;
      workspaceId: string;
      requestId: string;
      connectParams: ConnectActionParams;
    }) => {
      const url = href(
        '/organization/:organizationId/project/:projectId/workspace/:workspaceId/debug/request/:requestId/connect',
        {
          organizationId,
          projectId,
          workspaceId,
          requestId,
        },
      );

      return submit(JSON.stringify(connectParams), {
        action: url,
        method: 'POST',
        encType: 'application/json',
      });
    },
  clientAction,
);
