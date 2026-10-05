import { GRAPHQL_TRANSPORT_WS_PROTOCOL } from 'graphql-ws';
import { models } from 'insomnia-data';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { buildRealtimeConnectParams, openRealtimeConnection } from './open-realtime-connection';
import type { RenderedRealtimeConnectPayload } from './render-realtime-connect';

const main = {
  webSocket: { open: vi.fn() },
  socketIO: { open: vi.fn() },
  curl: { open: vi.fn() },
  trackAnalyticsEvent: vi.fn(),
};

const rendered: RenderedRealtimeConnectPayload = {
  url: 'wss://example.com/socket',
  headers: [{ name: 'X-Team', value: 'core' }],
  authentication: {} as RenderedRealtimeConnectPayload['authentication'],
  parameters: [{ name: 'room', value: '1' }],
  workspaceCookieJar: { cookies: [] } as any,
  suppressUserAgent: false,
};

// Ids carry the model prefix (ws-req_, req_, …), which is how WebSocket requests are recognized
const make = (type: string, patch: Record<string, unknown> = {}) =>
  ({
    _id: `${models.getModel(type as any)!.prefix}_1`,
    type,
    name: type,
    url: rendered.url,
    headers: [],
    ...patch,
  }) as any;

describe('open-realtime-connection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Only the bridge methods used here
    vi.stubGlobal('window', { main });
  });

  it('builds the connect params of WebSocket requests with the query string in the url', () => {
    expect(buildRealtimeConnectParams(make(models.webSocketRequest.type), rendered)).toEqual({
      url: 'wss://example.com/socket?room=1',
      headers: rendered.headers,
      authentication: {},
      cookieJar: { cookies: [] },
      suppressUserAgent: false,
      body: undefined,
    });
  });

  it('builds the connect params of Socket.IO requests with a separate query and path', () => {
    expect(
      buildRealtimeConnectParams(make(models.socketIORequest.type, { settingPath: '/io' }), rendered),
    ).toMatchObject({ url: 'wss://example.com/socket', query: { room: '1' }, path: '/io' });
  });

  it('opens WebSocket requests, as a SignalR session when asked', () => {
    const params = buildRealtimeConnectParams(make(models.webSocketRequest.type), rendered);
    openRealtimeConnection({
      req: make(models.webSocketRequest.type),
      workspaceId: 'wrk',
      rendered: params,
      isSignalR: true,
    });
    expect(main.webSocket.open).toHaveBeenCalledWith(
      expect.objectContaining({
        requestId: 'ws-req_1',
        url: 'wss://example.com/socket?room=1',
        isSignalR: true,
      }),
    );
  });

  it('opens GraphQL subscriptions over graphql-ws', () => {
    const req = make(models.request.type, {
      url: 'https://example.com/graphql',
      body: { mimeType: 'application/graphql', text: JSON.stringify({ query: 'subscription { orders { id } }' }) },
    });
    openRealtimeConnection({
      req,
      workspaceId: 'wrk',
      rendered: { ...buildRealtimeConnectParams(req, rendered), url: 'https://example.com/graphql' },
    });
    expect(main.webSocket.open).toHaveBeenCalledWith(
      expect.objectContaining({
        url: 'wss://example.com/graphql',
        isGraphqlSubscriptionRequest: true,
        headers: expect.arrayContaining([{ name: 'sec-websocket-protocol', value: GRAPHQL_TRANSPORT_WS_PROTOCOL }]),
      }),
    );
  });

  it('opens Event Streams with curl and Socket.IO requests with socket.io', () => {
    const stream = make(models.request.type, { headers: [{ name: 'Accept', value: 'text/event-stream' }] });
    openRealtimeConnection({ req: stream, workspaceId: 'wrk', rendered: buildRealtimeConnectParams(stream, rendered) });
    expect(main.curl.open).toHaveBeenCalledWith(
      expect.objectContaining({ workspaceId: 'wrk', renderedRequest: expect.objectContaining({ _id: stream._id }) }),
    );

    const io = make(models.socketIORequest.type);
    openRealtimeConnection({ req: io, workspaceId: 'wrk', rendered: buildRealtimeConnectParams(io, rendered) });
    expect(main.socketIO.open).toHaveBeenCalledWith(
      expect.objectContaining({ requestId: io._id, query: { room: '1' } }),
    );
  });
});
