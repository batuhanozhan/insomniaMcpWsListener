import { randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import http from 'node:http';

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { ipcMain } from 'electron';
import type { ChangeBufferEvent } from 'insomnia-data';
import { models, services } from 'insomnia-data';
import { z } from 'zod';

import { getAppVersion } from '~/common/constants';
import { database as db } from '~/common/database';

import { ipcMainHandle } from './ipc/electron';
import { getSecret, setSecret } from './ipc/secret-storage';
import {
  getEvents,
  getLatestResponse,
  getRealtimeRequest,
  getWorkspaceForRequest,
  listRealtimeRequests,
  type RealtimeEvent,
  type RealtimeRequestInfo,
} from './mcp-server-realtime';
import { redactSecrets, redactUrl } from './mcp-server-redact';
import {
  describeSignalRFrame,
  encodeCancelInvocation,
  encodeInvocation,
  encodeStreamInvocation,
  isHandshakeResponse,
  isPing,
  MessageType,
  parseSignalRFrames,
  type SignalRFrame,
} from './network/signalr';
import {
  addSocketIOListener,
  removeSocketIOListener,
  sendWebSocketEvent as sendSocketIOEvent,
} from './network/socket-io';
import {
  closeWebSocketConnection,
  isSignalRSession,
  sendWebSocketEvent,
  startSignalRSession,
} from './network/websocket';
import { getMainWindow } from './window-utils';

// Local MCP server that lets AI assistants (Claude Code, Codex, …) drive the app's realtime requests
// (WebSocket, GraphQL subscription, Socket.IO and Event Stream).
// Connections are opened through the renderer so templating, auth, cookies and certificates behave exactly like
// pressing "Connect" in the UI, and every event shows up in the request's normal event log.

export interface McpServerStatus {
  running: boolean;
  url: string | null;
  error: string | null;
}

const MCP_PATH = '/mcp';
const MAX_WAIT_MS = 60_000;
const POLL_INTERVAL_MS = 300;
const MAX_MESSAGE_LENGTH = 100_000;

const ACCESS_TOKEN_SECRET_KEY = 'mcpServer.accessToken';
const WRITE_TOOLS = ['realtime_send', 'signalr_invoke', 'signalr_stream', 'signalr_cancel_stream'];

let httpServer: http.Server | null = null;
let accessToken: string | null = null;
let status: McpServerStatus = { running: false, url: null, error: null };

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const json = (value: unknown): CallToolResult => ({
  content: [{ type: 'text', text: JSON.stringify(value, null, 2) }],
});

// Thrown errors are returned to the AI as tool errors instead of protocol errors
const safe =
  <Args>(handler: (args: Args) => Promise<CallToolResult>) =>
  async (args: Args): Promise<CallToolResult> => {
    try {
      return await handler(args);
    } catch (error) {
      return {
        content: [{ type: 'text', text: redactSecrets(error instanceof Error ? error.message : String(error)) }],
        isError: true,
      };
    }
  };

const getRealtimeRequestOrThrow = async (requestId: string) => {
  const info = await getRealtimeRequest(requestId);
  if (!info) {
    throw new Error(`Realtime request ${requestId} not found. Use realtime_list_requests to find request ids.`);
  }
  return info;
};

const assertConnected = async ({ request, adapter }: RealtimeRequestInfo) => {
  if (!(await adapter.isConnected(request._id))) {
    throw new Error('The request is not connected. Call realtime_connect first.');
  }
};

const truncate = (text: string) =>
  text.length > MAX_MESSAGE_LENGTH ? `${text.slice(0, MAX_MESSAGE_LENGTH)}… [truncated]` : text;

const formatData = (data: unknown) =>
  // Binary frames are serialized to the event log as { type: 'Buffer', data: number[] }
  truncate(
    data && typeof data === 'object' && 'type' in data && data.type === 'Buffer' && 'data' in data
      ? `base64:${Buffer.from(data.data as number[]).toString('base64')}`
      : typeof data === 'string'
        ? data
        : JSON.stringify(data),
  );

// Socket.IO arguments are kept as JSON values unless they are too large to return
const formatArgs = (args: unknown) => {
  const text = JSON.stringify(args) ?? '';
  return text.length > MAX_MESSAGE_LENGTH ? truncate(text) : args;
};

const getSignalRFrames = (event: RealtimeEvent) =>
  event.type === 'message' && typeof event.data === 'string' ? parseSignalRFrames(event.data) : null;

const formatEvent = (event: RealtimeEvent, index: number) => {
  const base = { index, type: event.type, timestamp: new Date(event.timestamp).toISOString() };
  // The event types differ per protocol, so only the fields present on the event are returned
  const fields = event as unknown as Record<string, unknown>;
  switch (event.type) {
    case 'message': {
      if ('eventName' in event) {
        return { ...base, direction: event.direction, event: event.eventName, args: formatArgs(event.data) };
      }
      const frames = getSignalRFrames(event);
      if (frames) {
        return {
          ...base,
          direction: event.direction,
          signalR: frames.filter(frame => !isPing(frame)).map(describeSignalRFrame),
        };
      }
      return { ...base, direction: event.direction, data: formatData(event.data) };
    }
    case 'close': {
      return {
        ...base,
        ...(fields.code !== undefined && { code: fields.code }),
        ...(fields.reason !== undefined && { reason: fields.reason }),
        ...(fields.wasClean !== undefined && { wasClean: fields.wasClean }),
        ...(fields.statusCode !== undefined && { statusCode: fields.statusCode }),
      };
    }
    case 'error':
    case 'info': {
      return { ...base, message: redactSecrets(String(fields.message ?? '')) };
    }
    case 'addEvent':
    case 'removeEvent': {
      return { ...base, event: fields.eventName };
    }
    default: {
      return base;
    }
  }
};

// SignalR pings carry no information, so they are hidden from the AI
const isSignalRPingOnly = (event: RealtimeEvent) => getSignalRFrames(event)?.every(isPing) ?? false;

// Waits for an incoming SignalR frame after `afterIndex` that matches the predicate
const waitForSignalRFrame = async ({
  info,
  afterIndex,
  timeoutMs,
  predicate,
}: {
  info: RealtimeRequestInfo;
  afterIndex: number;
  timeoutMs: number;
  predicate: (frame: SignalRFrame) => boolean;
}) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { events } = await getEvents(info);
    for (const event of events.slice(afterIndex)) {
      if (event.type === 'message' && event.direction === 'INCOMING') {
        const frame = getSignalRFrames(event)?.find(predicate);
        if (frame) {
          return frame;
        }
      }
      if (event.type === 'close' || event.type === 'error') {
        throw new Error('The connection was closed while waiting for the SignalR server to respond.');
      }
    }
    await wait(POLL_INTERVAL_MS);
  }
  return null;
};

// websocket.ts sends the handshake when a SignalR connection opens; wait for the hub to accept it
const waitForSignalRHandshake = async (info: RealtimeRequestInfo) => {
  const response = await waitForSignalRFrame({
    info,
    afterIndex: 0,
    timeoutMs: 10_000,
    predicate: isHandshakeResponse,
  });
  if (!response || response.error) {
    closeWebSocketConnection({ requestId: info.request._id });
    throw new Error(
      response
        ? `SignalR handshake failed: ${response.error}`
        : 'SignalR handshake timed out. Is this a SignalR hub url?',
    );
  }
};

// Asks the renderer to render and open the connection, same as the Connect button
const pendingConnectRequests = new Map<string, (result: { error?: string }) => void>();

const requestConnectFromRenderer = (options: { requestId: string; workspaceId: string; isSignalR: boolean }) => {
  const mainWindow = getMainWindow();
  if (!mainWindow) {
    return Promise.resolve({ error: 'The insomniaMcpWsListener window is not open.' });
  }
  const id = randomUUID();
  return new Promise<{ error?: string }>(resolve => {
    const timeout = setTimeout(() => {
      pendingConnectRequests.delete(id);
      resolve({ error: 'Timed out waiting for the app to start the connection.' });
    }, 30_000);
    pendingConnectRequests.set(id, result => {
      clearTimeout(timeout);
      resolve(result);
    });
    mainWindow.webContents.send('mcpServer.connectRequest', id, options);
  });
};

const describeConnection = ({ request, kind }: RealtimeRequestInfo, isSignalR: boolean) => ({
  connected: true,
  type: kind,
  url: redactUrl(request.url),
  ...(isSignalR && { signalR: { handshake: 'ok', keepAlive: true } }),
  // Socket.IO only delivers the events that are listened to
  ...(models.socketIORequest.isSocketIORequest(request) && {
    listeningTo: request.eventListeners.filter(listener => listener.isOpen).map(listener => listener.eventName),
    hint: 'Socket.IO only delivers listened events; use realtime_subscribe to listen to more.',
  }),
});

const connect = async ({
  requestId,
  protocol,
  timeoutMs = 10_000,
}: {
  requestId: string;
  protocol?: 'websocket' | 'signalr';
  timeoutMs?: number;
}) => {
  const info = await getRealtimeRequestOrThrow(requestId);
  const { request, kind, adapter } = info;
  if (protocol === 'signalr' && kind !== 'websocket') {
    throw new Error('protocol "signalr" is only available for WebSocket requests.');
  }
  const isSignalR =
    models.webSocketRequest.isWebSocketRequest(request) && (protocol === 'signalr' || Boolean(request.settingSignalR));

  if (await adapter.isConnected(requestId)) {
    if (isSignalR && !isSignalRSession(requestId)) {
      // Opened without the SignalR request setting, so no handshake or pings yet
      const { events } = await getEvents(info);
      const hasHandshake = events.some(
        event =>
          event.type === 'message' &&
          event.direction === 'INCOMING' &&
          getSignalRFrames(event)?.some(isHandshakeResponse),
      );
      startSignalRSession({ requestId, sendHandshake: !hasHandshake });
      await waitForSignalRHandshake(info);
    }
    return { ...describeConnection(info, isSignalR), message: 'Already connected.' };
  }
  const workspace = await getWorkspaceForRequest(request);
  if (!workspace) {
    throw new Error(`Could not find the workspace of request ${requestId}.`);
  }

  const previousResponse = await getLatestResponse(info);
  const { error } = await requestConnectFromRenderer({ requestId, workspaceId: workspace._id, isSignalR });
  if (error) {
    throw new Error(error);
  }

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await adapter.isConnected(requestId)) {
      if (isSignalR) {
        await waitForSignalRHandshake(info);
      }
      return describeConnection(info, isSignalR);
    }
    const response = await getLatestResponse(info);
    if (response && response._id !== previousResponse?._id && response.error) {
      throw new Error(`Connection failed: ${response.error}`);
    }
    await wait(POLL_INTERVAL_MS);
  }
  const response = await getLatestResponse(info);
  if (response && response._id !== previousResponse?._id && response.statusCode) {
    throw new Error(`Connection failed with HTTP ${response.statusCode} ${response.statusMessage}`);
  }
  throw new Error(`Timed out after ${timeoutMs}ms waiting for the connection to open.`);
};

const readEvents = async ({
  requestId,
  afterIndex = 0,
  limit = 50,
  waitMs = 0,
}: {
  requestId: string;
  afterIndex?: number;
  limit?: number;
  waitMs?: number;
}) => {
  const info = await getRealtimeRequestOrThrow(requestId);
  const deadline = Date.now() + waitMs;
  while (true) {
    const { response, events } = await getEvents(info);
    const page: ReturnType<typeof formatEvent>[] = [];
    let nextAfterIndex = afterIndex;
    while (nextAfterIndex < events.length && page.length < limit) {
      const event = events[nextAfterIndex];
      nextAfterIndex++;
      if (!isSignalRPingOnly(event)) {
        page.push(formatEvent(event, nextAfterIndex));
      }
    }
    if (page.length > 0 || Date.now() >= deadline) {
      return {
        responseId: response?._id ?? null,
        connected: await info.adapter.isConnected(requestId),
        events: page,
        nextAfterIndex,
        hasMore: events.length > nextAfterIndex,
      };
    }
    // Skip the pings already scanned on the next poll
    afterIndex = nextAfterIndex;
    await wait(POLL_INTERVAL_MS);
  }
};

const send = async ({
  requestId,
  message,
  event,
  args,
}: {
  requestId: string;
  message?: string;
  event?: string;
  args?: unknown[];
}) => {
  const info = await getRealtimeRequestOrThrow(requestId);
  await assertConnected(info);
  switch (info.kind) {
    case 'event-stream': {
      throw new Error('Event streams only receive data; nothing can be sent on them.');
    }
    case 'socketio': {
      await sendSocketIOEvent({
        requestId,
        eventName: event || 'message',
        args: args ?? (message === undefined ? [] : [message]),
      });
      break;
    }
    default: {
      if (message === undefined) {
        throw new Error('message is required for WebSocket and GraphQL subscription requests.');
      }
      await sendWebSocketEvent({ requestId, payload: message });
    }
  }
  return { sent: true };
};

const getSocketIORequestOrThrow = async (requestId: string) => {
  const info = await getRealtimeRequestOrThrow(requestId);
  if (info.kind !== 'socketio') {
    throw new Error(
      'Subscribing to events is only available for Socket.IO requests; other types deliver every message.',
    );
  }
  await assertConnected(info);
  return info;
};

const assertSignalRConnected = async (requestId: string) => {
  const info = await getRealtimeRequestOrThrow(requestId);
  if (info.kind !== 'websocket') {
    throw new Error('SignalR tools are only available for WebSocket requests.');
  }
  await assertConnected(info);
  return info;
};

const streamSignalR = async ({
  requestId,
  target,
  args = [],
  waitMs = 3000,
}: {
  requestId: string;
  target: string;
  args?: unknown[];
  waitMs?: number;
}) => {
  const info = await assertSignalRConnected(requestId);
  const invocationId = randomUUID();
  const { events } = await getEvents(info);
  await sendWebSocketEvent({ requestId, payload: encodeStreamInvocation({ target, args, invocationId }) });

  // Wait briefly so an immediate error (unknown method, wrong arguments, not authorized) is reported here
  const completion = await waitForSignalRFrame({
    info,
    afterIndex: events.length,
    timeoutMs: waitMs,
    predicate: frame => frame.type === MessageType.Completion && frame.invocationId === invocationId,
  });
  if (completion?.error) {
    throw new Error(`${target} failed: ${completion.error}`);
  }
  return {
    invocationId,
    completed: Boolean(completion),
    readFromIndex: events.length,
    hint:
      'Read the items with realtime_read_events (afterIndex = readFromIndex): they arrive as streamItem frames ' +
      'with this invocationId and the stream ends with a completion frame. Stop it with signalr_cancel_stream.',
  };
};

const invokeSignalR = async ({
  requestId,
  target,
  args = [],
  waitForResult = true,
  timeoutMs = 10_000,
}: {
  requestId: string;
  target: string;
  args?: unknown[];
  waitForResult?: boolean;
  timeoutMs?: number;
}) => {
  const info = await assertSignalRConnected(requestId);
  if (!waitForResult) {
    await sendWebSocketEvent({ requestId, payload: encodeInvocation({ target, args }) });
    return { sent: true };
  }
  const invocationId = randomUUID();
  const { events } = await getEvents(info);
  await sendWebSocketEvent({ requestId, payload: encodeInvocation({ target, args, invocationId }) });
  const completion = await waitForSignalRFrame({
    info,
    afterIndex: events.length,
    timeoutMs,
    predicate: frame => frame.type === MessageType.Completion && frame.invocationId === invocationId,
  });
  if (!completion) {
    throw new Error(`Timed out after ${timeoutMs}ms waiting for the result of ${target}.`);
  }
  if (completion.error) {
    throw new Error(`${target} failed: ${completion.error}`);
  }
  return { result: completion.result ?? null };
};

const createMcpServer = ({ readOnly }: { readOnly: boolean }) => {
  const server = new McpServer(
    { name: 'insomnia-mcp-ws-listener', version: getAppVersion() },
    {
      instructions: readOnly
        ? 'insomniaMcpWsListener is in read-only mode: you can list, connect to, read, subscribe to and disconnect ' +
          'realtime requests, but not send messages or call SignalR hub methods. If the user wants that, they can ' +
          'turn off "Read-only" in insomniaMcpWsListener > Preferences > AI Settings > MCP Server.'
        : undefined,
    },
  );
  // The SDK's registerTool generics are too deep for TypeScript with zod v3, so type the handler args here instead
  const registerTool = <Shape extends z.ZodRawShape>(
    name: string,
    config: { title: string; description: string; inputSchema: Shape },
    handler: (args: z.infer<z.ZodObject<Shape>>) => Promise<CallToolResult>,
  ) => {
    // Tools that send data to a server are not offered in read-only mode
    if (readOnly && WRITE_TOOLS.includes(name)) {
      return;
    }
    (server.registerTool as (...args: unknown[]) => void)(name, config, handler);
  };

  registerTool(
    'realtime_list_requests',
    {
      title: 'List realtime requests',
      description:
        'List the realtime requests saved in insomniaMcpWsListener (WebSocket, GraphQL subscription, Socket.IO and ' +
        'Event Stream / Server-Sent Events) with their type, workspace, url and whether they are connected.',
      inputSchema: {},
    },
    safe(async () => {
      const result = await Promise.all(
        (await listRealtimeRequests()).map(async ({ request, kind, adapter }) => {
          const workspace = await getWorkspaceForRequest(request);
          return {
            requestId: request._id,
            name: request.name,
            type: kind,
            url: redactUrl(request.url),
            workspaceId: workspace?._id ?? null,
            workspaceName: workspace?.name ?? null,
            connected: await adapter.isConnected(request._id),
            ...(kind === 'websocket' && { signalR: isSignalRSession(request._id) }),
          };
        }),
      );
      return json(result);
    }),
  );

  registerTool(
    'realtime_connect',
    {
      title: 'Connect a realtime request',
      description:
        'Open the connection of a realtime request using its saved url, headers, auth and the active environment, ' +
        'exactly like pressing Connect in insomniaMcpWsListener. Events are then visible in the app and via ' +
        'realtime_read_events. For an ASP.NET Core SignalR hub (a WebSocket request) set protocol to "signalr" (or ' +
        'enable "SignalR hub" in the request settings): the handshake is done and the connection is kept alive with ' +
        'pings automatically.',
      inputSchema: {
        requestId: z.string(),
        protocol: z
          .enum(['websocket', 'signalr'])
          .optional()
          .describe('WebSocket requests only. Use "signalr" for SignalR hubs.'),
        timeoutMs: z.number().int().positive().max(MAX_WAIT_MS).optional().describe('Default 10000'),
      },
    },
    safe(async args => json(await connect(args))),
  );

  registerTool(
    'realtime_read_events',
    {
      title: 'Read realtime events',
      description:
        "Read the event log (open, incoming/outgoing messages, close, error) of the request's latest connection, oldest first. " +
        'Pass nextAfterIndex from the previous call as afterIndex to only get new events, and waitMs to wait for them. ' +
        'Socket.IO messages have an event name and args; SignalR messages are decoded into a signalR array ' +
        '(e.g. { kind: "invocation", target, arguments }) and pings are hidden.',
      inputSchema: {
        requestId: z.string(),
        afterIndex: z.number().int().min(0).optional().describe('Only return events after this index, default 0'),
        limit: z.number().int().min(1).max(500).optional().describe('Default 50'),
        waitMs: z
          .number()
          .int()
          .min(0)
          .max(MAX_WAIT_MS)
          .optional()
          .describe(`If there are no new events, wait up to this many ms for one (max ${MAX_WAIT_MS})`),
      },
    },
    safe(async args => json(await readEvents(args))),
  );

  registerTool(
    'realtime_send',
    {
      title: 'Send a realtime message',
      description:
        'Send a message on a connected request. WebSocket and GraphQL subscription requests take a text message ' +
        '(pass JSON as a string). Socket.IO requests take an event name (default "message") and args. Event Streams ' +
        'only receive data.',
      inputSchema: {
        requestId: z.string(),
        message: z.string().optional().describe('Text to send (WebSocket, GraphQL subscription)'),
        event: z.string().optional().describe('Socket.IO event name, default "message"'),
        args: z.array(z.unknown()).optional().describe('Socket.IO event arguments, default [message]'),
      },
    },
    safe(async args => json(await send(args))),
  );

  registerTool(
    'realtime_subscribe',
    {
      title: 'Listen to a Socket.IO event',
      description:
        'Start listening to a Socket.IO event on a connected Socket.IO request; its messages then appear in ' +
        'realtime_read_events. Other request types already deliver every message.',
      inputSchema: {
        requestId: z.string(),
        event: z.string().describe('Socket.IO event name'),
      },
    },
    safe(async ({ requestId, event }) => {
      await getSocketIORequestOrThrow(requestId);
      addSocketIOListener({ requestId, eventName: event });
      return json({ listening: event });
    }),
  );

  registerTool(
    'realtime_unsubscribe',
    {
      title: 'Stop listening to a Socket.IO event',
      description: 'Stop listening to a Socket.IO event started with realtime_subscribe or in the app.',
      inputSchema: {
        requestId: z.string(),
        event: z.string().describe('Socket.IO event name'),
      },
    },
    safe(async ({ requestId, event }) => {
      await getSocketIORequestOrThrow(requestId);
      removeSocketIOListener({ requestId, eventName: event });
      return json({ stoppedListening: event });
    }),
  );

  registerTool(
    'signalr_invoke',
    {
      title: 'Invoke a SignalR hub method',
      description:
        'Call a hub method on a SignalR connection (opened with realtime_connect protocol "signalr"), like ' +
        'connection.invoke(target, ...arguments). By default waits for and returns its result; set waitForResult ' +
        'to false to only send it, like connection.send.',
      inputSchema: {
        requestId: z.string(),
        target: z.string().describe('Hub method name'),
        arguments: z.array(z.unknown()).optional().describe('Method arguments, default []'),
        waitForResult: z.boolean().optional().describe('Default true'),
        timeoutMs: z.number().int().positive().max(MAX_WAIT_MS).optional().describe('Default 10000'),
      },
    },
    safe(async ({ arguments: args, ...options }) => json(await invokeSignalR({ ...options, args }))),
  );

  registerTool(
    'signalr_stream',
    {
      title: 'Start a SignalR stream',
      description:
        'Call a streaming hub method (one returning IAsyncEnumerable or ChannelReader), like ' +
        'connection.stream(target, ...arguments). Returns an invocationId; the items then arrive as streamItem frames ' +
        'that you read with realtime_read_events, until a completion frame ends the stream.',
      inputSchema: {
        requestId: z.string(),
        target: z.string().describe('Hub method name, e.g. GetProgress'),
        arguments: z.array(z.unknown()).optional().describe('Method arguments, default []'),
        waitMs: z
          .number()
          .int()
          .min(0)
          .max(MAX_WAIT_MS)
          .optional()
          .describe('How long to wait for an immediate error before returning, default 3000'),
      },
    },
    safe(async ({ arguments: args, ...options }) => json(await streamSignalR({ ...options, args }))),
  );

  registerTool(
    'signalr_cancel_stream',
    {
      title: 'Cancel a SignalR stream',
      description: 'Stop a stream started with signalr_stream.',
      inputSchema: {
        requestId: z.string(),
        invocationId: z.string(),
      },
    },
    safe(async ({ requestId, invocationId }) => {
      await assertSignalRConnected(requestId);
      await sendWebSocketEvent({ requestId, payload: encodeCancelInvocation(invocationId) });
      return json({ canceled: true });
    }),
  );

  registerTool(
    'realtime_disconnect',
    {
      title: 'Disconnect a realtime request',
      description: 'Close the connection of a realtime request. Its event log stays readable.',
      inputSchema: {
        requestId: z.string(),
      },
    },
    safe(async ({ requestId }) => {
      const { adapter } = await getRealtimeRequestOrThrow(requestId);
      adapter.close(requestId);
      return json({ disconnected: true });
    }),
  );

  return server;
};

const handleHttpRequest = async (req: http.IncomingMessage, res: http.ServerResponse, port: number) => {
  // Only accept local clients; browsers always send an Origin header, so rejecting it blocks web pages
  // (including DNS rebinding) from reaching the server.
  const allowedHosts = [`127.0.0.1:${port}`, `localhost:${port}`];
  if (!allowedHosts.includes(req.headers.host ?? '') || req.headers.origin) {
    res.writeHead(403).end('Forbidden');
    return;
  }
  if (new URL(req.url ?? '/', `http://${req.headers.host}`).pathname !== MCP_PATH) {
    res.writeHead(404).end('Not found');
    return;
  }
  // Other programs on this computer can reach 127.0.0.1 too, so only clients given the access token are accepted
  if (!(await isAuthorized(req.headers.authorization))) {
    res.writeHead(401, { 'WWW-Authenticate': 'Bearer' }).end('Unauthorized');
    return;
  }

  // Stateless mode: a fresh server and transport per request
  const { mcpServerReadOnly } = await services.settings.get();
  const server = createMcpServer({ readOnly: mcpServerReadOnly });
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
  res.on('close', () => {
    transport.close();
    server.close();
  });
  try {
    await server.connect(transport);
    await transport.handleRequest(req, res);
  } catch (error) {
    console.error('[mcp-server] Failed to handle request', error);
    if (!res.headersSent) {
      res.writeHead(500).end('Internal server error');
    }
  }
};

// The token is kept encrypted with Electron safeStorage and survives restarts, so clients only need to be set up once
const getAccessToken = async () => {
  if (!accessToken) {
    accessToken = await getSecret(ACCESS_TOKEN_SECRET_KEY);
  }
  if (!accessToken) {
    accessToken = await regenerateAccessToken();
  }
  return accessToken;
};

const regenerateAccessToken = async () => {
  const token = randomBytes(32).toString('base64url');
  await setSecret(ACCESS_TOKEN_SECRET_KEY, token);
  accessToken = token;
  return token;
};

const isAuthorized = async (authorization: string | undefined) => {
  const expected = Buffer.from(`Bearer ${await getAccessToken()}`);
  const actual = Buffer.from(authorization ?? '');
  return actual.length === expected.length && timingSafeEqual(actual, expected);
};

const stopServer = async () => {
  const server = httpServer;
  httpServer = null;
  status = { running: false, url: null, error: null };
  if (server) {
    await new Promise<void>(resolve => server.close(() => resolve()));
    server.closeAllConnections();
  }
};

const startServer = async (port: number) => {
  await stopServer();
  const server = http.createServer((req, res) => handleHttpRequest(req, res, port));
  try {
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, '127.0.0.1', () => {
        server.off('error', reject);
        resolve();
      });
    });
    httpServer = server;
    status = { running: true, url: `http://127.0.0.1:${port}${MCP_PATH}`, error: null };
    console.log(`[mcp-server] Listening on ${status.url}`);
  } catch (error) {
    const message =
      (error as NodeJS.ErrnoException).code === 'EADDRINUSE'
        ? `Port ${port} is already in use. Choose another port.`
        : error instanceof Error
          ? error.message
          : String(error);
    console.warn('[mcp-server] Failed to start', error);
    status = { running: false, url: null, error: message };
  }
};

const applySettings = async ({
  mcpServerEnabled,
  mcpServerPort,
}: {
  mcpServerEnabled: boolean;
  mcpServerPort: number;
}) => (mcpServerEnabled ? startServer(mcpServerPort) : stopServer());

export async function watchMcpServerSettings() {
  let old = await services.settings.get();
  await applySettings(old);
  db.onChange(async (changes: ChangeBufferEvent[]) => {
    for (const [event, doc] of changes) {
      if (!models.settings.isSettings(doc) || event !== 'update') {
        continue;
      }
      if (old.mcpServerEnabled !== doc.mcpServerEnabled || old.mcpServerPort !== doc.mcpServerPort) {
        old = doc;
        await applySettings(doc);
      }
    }
  });
}

export interface McpServerBridgeAPI {
  getStatus: () => Promise<McpServerStatus>;
  getAccessToken: () => Promise<string>;
  regenerateAccessToken: () => Promise<string>;
  notifyConnectRequestResult: (id: string, result: { error?: string }) => void;
}

export const registerMcpServerHandlers = () => {
  ipcMainHandle('mcpServer.getStatus', () => status);
  // Only the app's own window may read or change the access token, not plugin or other windows
  const assertMainWindow = (event: Electron.IpcMainInvokeEvent) => {
    if (event.sender !== getMainWindow()?.webContents) {
      throw new Error('Not allowed');
    }
  };
  ipcMainHandle('mcpServer.getAccessToken', async event => {
    assertMainWindow(event);
    return getAccessToken();
  });
  ipcMainHandle('mcpServer.regenerateAccessToken', async event => {
    assertMainWindow(event);
    return regenerateAccessToken();
  });
  ipcMain.on('mcpServer.connectRequestResult', (event, { id, result }: { id: string; result: { error?: string } }) => {
    if (event.sender !== getMainWindow()?.webContents) {
      return;
    }
    const resolve = pendingConnectRequests.get(id);
    if (!resolve) {
      return;
    }
    pendingConnectRequests.delete(id);
    resolve(result);
  });
};
