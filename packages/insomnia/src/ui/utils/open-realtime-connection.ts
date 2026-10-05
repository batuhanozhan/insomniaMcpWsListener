import { GRAPHQL_TRANSPORT_WS_PROTOCOL, MessageType } from 'graphql-ws';
import type { GrpcRequest, McpRequest, Request, SocketIORequest, WebSocketRequest } from 'insomnia-data';
import { models } from 'insomnia-data';

import type { RenderedRequest } from '~/common/templating/types';
import { buildQueryStringFromParams, joinUrlAndQueryString } from '~/common/utils/url/querystring';
import type { ConnectActionParams } from '~/routes/organization.$organizationId.project.$projectId.workspace.$workspaceId.debug.request.$requestId.connect';
import { AnalyticsEvent } from '~/ui/analytics';

import type { RenderedRealtimeConnectPayload } from './render-realtime-connect';

const { isGraphqlSubscriptionRequest, isEventStreamRequest } = models.request;

export type RealtimeRequest = WebSocketRequest | SocketIORequest | Request;

/** Turns a rendered request into the parameters the Connect button submits for its request type. */
export const buildRealtimeConnectParams = (
  request: RealtimeRequest,
  rendered: RenderedRealtimeConnectPayload,
): ConnectActionParams => {
  const common = {
    headers: rendered.headers,
    authentication: rendered.authentication,
    cookieJar: rendered.workspaceCookieJar,
    suppressUserAgent: rendered.suppressUserAgent,
  };
  // socket.io uses a separate field (query) for query parameters
  if (models.socketIORequest.isSocketIORequest(request)) {
    const query: Record<string, string> = {};
    rendered.parameters.forEach(({ name, value }) => {
      if (name) {
        query[name] = value;
      }
    });
    return { ...common, url: rendered.url, query, path: request.settingPath };
  }
  return {
    ...common,
    url: joinUrlAndQueryString(rendered.url, buildQueryStringFromParams(rendered.parameters)),
    body: rendered.body,
  };
};

/** Opens the connection of a WebSocket, GraphQL subscription, Event Stream or Socket.IO request. */
export const openRealtimeConnection = ({
  req,
  workspaceId,
  rendered,
  isSignalR,
}: {
  // Any request: only the realtime types open a connection here
  req: RealtimeRequest | GrpcRequest | McpRequest;
  workspaceId: string;
  rendered: ConnectActionParams;
  isSignalR?: boolean;
}) => {
  const requestId = req._id;
  if (models.webSocketRequest.isWebSocketRequestId(requestId)) {
    window.main.webSocket.open({
      requestId,
      workspaceId,
      url: rendered.url,
      headers: rendered.headers,
      authentication: rendered.authentication,
      cookieJar: rendered.cookieJar,
      suppressUserAgent: rendered.suppressUserAgent,
      isSignalR,
    });
    window.main.trackAnalyticsEvent({
      event: AnalyticsEvent.requestExecuted,
      properties: { request_type: 'WebSocket' },
    });
  }
  if (isGraphqlSubscriptionRequest(req)) {
    window.main.webSocket.open({
      requestId,
      workspaceId,
      // replace url with ws/wss for graphql subscriptions
      url: rendered.url.replace('http', 'ws').replace('https', 'wss'),
      headers: [
        ...rendered.headers,
        // add graphql-transport-ws protocol for graphql subscription
        {
          name: 'sec-websocket-protocol',
          value: GRAPHQL_TRANSPORT_WS_PROTOCOL,
        },
      ],
      isGraphqlSubscriptionRequest: true,
      // graphql-ws protocol needs to send ConnectionInit message first. Refer: https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md
      initialPayload: JSON.stringify({
        type: MessageType.ConnectionInit,
      }),
      authentication: rendered.authentication,
      cookieJar: rendered.cookieJar,
      suppressUserAgent: rendered.suppressUserAgent,
    });
    window.main.trackAnalyticsEvent({
      event: AnalyticsEvent.requestExecuted,
      properties: { request_type: 'GraphQL' },
    });
  }
  if (isEventStreamRequest(req)) {
    const renderedRequest = { ...req, ...rendered } as RenderedRequest;
    window.main.curl.open({ workspaceId, renderedRequest });
    window.main.trackAnalyticsEvent({
      event: AnalyticsEvent.requestExecuted,
      properties: { request_type: 'Event Stream' },
    });
  }
  if (models.socketIORequest.isSocketIORequest(req)) {
    window.main.socketIO.open({
      requestId,
      workspaceId,
      url: rendered.url,
      headers: rendered.headers,
      cookieJar: rendered.cookieJar,
      authentication: rendered.authentication,
      query: rendered.query || {},
      path: rendered.path,
      suppressUserAgent: rendered.suppressUserAgent,
    });
    window.main.trackAnalyticsEvent({
      event: AnalyticsEvent.requestExecuted,
      properties: { request_type: 'SocketIO' },
    });
  }
};
