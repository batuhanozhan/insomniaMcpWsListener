import { randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import http from 'node:http';

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { ipcMain } from 'electron';
import type { ChangeBufferEvent, WebSocketRequest, WebSocketResponse, Workspace } from 'insomnia-data';
import { models, services } from 'insomnia-data';
import { z } from 'zod';

import { getAppVersion } from '~/common/constants';
import { database as db } from '~/common/database';

import { ipcMainHandle } from './ipc/electron';
import { getSecret, setSecret } from './ipc/secret-storage';
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
  closeWebSocketConnection,
  findMany,
  getWebSocketReadyState,
  isSignalRSession,
  sendWebSocketEvent,
  startSignalRSession,
  type WebSocketEvent,
} from './network/websocket';
import { getMainWindow } from './window-utils';

// Local MCP server that lets AI assistants (Claude Code, Codex, …) drive the app's WebSocket requests.
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
const WRITE_TOOLS = ['websocket_send', 'signalr_invoke', 'signalr_stream', 'signalr_cancel_stream'];

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

const getWebSocketRequestOrThrow = async (requestId: string) => {
  const request = await services.webSocketRequest.getById(requestId);
  if (!request) {
    throw new Error(`WebSocket request ${requestId} not found. Use websocket_list_requests to find request ids.`);
  }
  return request;
};

const getWorkspaceForRequest = async (request: WebSocketRequest) => {
  const ancestors = await db.withAncestors<WebSocketRequest | Workspace>(request, [
    models.requestGroup.type,
    models.workspace.type,
  ]);
  return ancestors.find(models.workspace.isWorkspace);
};

const getLatestResponse = (requestId: string) =>
  db.findOne<WebSocketResponse>(models.webSocketResponse.type, { parentId: requestId }, { created: -1 });

const formatData = (data: unknown) => {
  // Binary frames are serialized to the event log as { type: 'Buffer', data: number[] }
  const text =
    data && typeof data === 'object' && 'type' in data && data.type === 'Buffer' && 'data' in data
      ? `base64:${Buffer.from(data.data as number[]).toString('base64')}`
      : typeof data === 'string'
        ? data
        : JSON.stringify(data);
  return text.length > MAX_MESSAGE_LENGTH ? `${text.slice(0, MAX_MESSAGE_LENGTH)}… [truncated]` : text;
};

const getSignalRFrames = (event: WebSocketEvent) =>
  event.type === 'message' && typeof event.data === 'string' ? parseSignalRFrames(event.data) : null;

const formatEvent = (event: WebSocketEvent, index: number) => {
  const base = { index, type: event.type, timestamp: new Date(event.timestamp).toISOString() };
  switch (event.type) {
    case 'message': {
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
      return { ...base, code: event.code, reason: event.reason, wasClean: event.wasClean };
    }
    case 'error': {
      return { ...base, message: event.message };
    }
    default: {
      return base;
    }
  }
};

const getEvents = async (requestId: string) => {
  const response = await getLatestResponse(requestId);
  // findMany returns the newest event first
  const events = response ? (await findMany({ responseId: response._id })).reverse() : [];
  return { response, events };
};

// SignalR pings carry no information, so they are hidden from the AI
const isSignalRPingOnly = (event: WebSocketEvent) => getSignalRFrames(event)?.every(isPing) ?? false;

// Waits for an incoming SignalR frame after `afterIndex` that matches the predicate
const waitForSignalRFrame = async ({
  requestId,
  afterIndex,
  timeoutMs,
  predicate,
}: {
  requestId: string;
  afterIndex: number;
  timeoutMs: number;
  predicate: (frame: SignalRFrame) => boolean;
}) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { events } = await getEvents(requestId);
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
const waitForSignalRHandshake = async (requestId: string) => {
  const response = await waitForSignalRFrame({
    requestId,
    afterIndex: 0,
    timeoutMs: 10_000,
    predicate: isHandshakeResponse,
  });
  if (!response || response.error) {
    closeWebSocketConnection({ requestId });
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
    mainWindow.webContents.send('mcpServer.connectWebSocket', id, options);
  });
};

const connect = async ({
  requestId,
  protocol = 'websocket',
  timeoutMs = 10_000,
}: {
  requestId: string;
  protocol?: 'websocket' | 'signalr';
  timeoutMs?: number;
}) => {
  const request = await getWebSocketRequestOrThrow(requestId);
  const isSignalR = protocol === 'signalr' || Boolean(request.settingSignalR);
  if (await getWebSocketReadyState({ requestId })) {
    if (isSignalR && !isSignalRSession(requestId)) {
      // Opened without the SignalR request setting, so no handshake or pings yet
      const { events } = await getEvents(requestId);
      const hasHandshake = events.some(
        event =>
          event.type === 'message' &&
          event.direction === 'INCOMING' &&
          getSignalRFrames(event)?.some(isHandshakeResponse),
      );
      startSignalRSession({ requestId, sendHandshake: !hasHandshake });
      await waitForSignalRHandshake(requestId);
    }
    return {
      connected: true,
      message: 'Already connected.',
      ...(isSignalR && { signalR: { handshake: 'ok', keepAlive: true } }),
    };
  }
  const workspace = await getWorkspaceForRequest(request);
  if (!workspace) {
    throw new Error(`Could not find the workspace of request ${requestId}.`);
  }

  const previousResponse = await getLatestResponse(requestId);
  const { error } = await requestConnectFromRenderer({ requestId, workspaceId: workspace._id, isSignalR });
  if (error) {
    throw new Error(error);
  }

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await getWebSocketReadyState({ requestId })) {
      if (isSignalR) {
        await waitForSignalRHandshake(requestId);
        return { connected: true, url: redactUrl(request.url), signalR: { handshake: 'ok', keepAlive: true } };
      }
      return { connected: true, url: redactUrl(request.url) };
    }
    const response = await getLatestResponse(requestId);
    if (response && response._id !== previousResponse?._id && response.error) {
      throw new Error(`Connection failed: ${response.error}`);
    }
    await wait(POLL_INTERVAL_MS);
  }
  const response = await getLatestResponse(requestId);
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
  await getWebSocketRequestOrThrow(requestId);
  const deadline = Date.now() + waitMs;
  while (true) {
    const { response, events } = await getEvents(requestId);
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
        connected: await getWebSocketReadyState({ requestId }),
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

const assertSignalRConnected = async (requestId: string) => {
  await getWebSocketRequestOrThrow(requestId);
  if (!(await getWebSocketReadyState({ requestId }))) {
    throw new Error('The request is not connected. Call websocket_connect with protocol "signalr" first.');
  }
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
  await assertSignalRConnected(requestId);
  const invocationId = randomUUID();
  const { events } = await getEvents(requestId);
  await sendWebSocketEvent({ requestId, payload: encodeStreamInvocation({ target, args, invocationId }) });

  // Wait briefly so an immediate error (unknown method, wrong arguments, not authorized) is reported here
  const completion = await waitForSignalRFrame({
    requestId,
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
      'Read the items with websocket_read_messages (afterIndex = readFromIndex): they arrive as streamItem frames ' +
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
  await assertSignalRConnected(requestId);
  if (!waitForResult) {
    await sendWebSocketEvent({ requestId, payload: encodeInvocation({ target, args }) });
    return { sent: true };
  }
  const invocationId = randomUUID();
  const { events } = await getEvents(requestId);
  await sendWebSocketEvent({ requestId, payload: encodeInvocation({ target, args, invocationId }) });
  const completion = await waitForSignalRFrame({
    requestId,
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
        ? 'insomniaMcpWsListener is in read-only mode: you can list, connect to, read and disconnect realtime requests, but not ' +
          'send messages or call SignalR hub methods. If the user wants that, they can turn off "Read-only" in ' +
          'insomniaMcpWsListener > Preferences > AI Settings > MCP Server.'
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
    'websocket_list_requests',
    {
      title: 'List WebSocket requests',
      description:
        'List the WebSocket requests saved in insomniaMcpWsListener with their workspace, url and whether they are connected.',
      inputSchema: {},
    },
    safe(async () => {
      const requests = await services.webSocketRequest.all();
      const result = await Promise.all(
        requests.map(async request => {
          const workspace = await getWorkspaceForRequest(request);
          return {
            requestId: request._id,
            name: request.name,
            url: redactUrl(request.url),
            workspaceId: workspace?._id ?? null,
            workspaceName: workspace?.name ?? null,
            connected: await getWebSocketReadyState({ requestId: request._id }),
            signalR: isSignalRSession(request._id),
          };
        }),
      );
      return json(result);
    }),
  );

  registerTool(
    'websocket_connect',
    {
      title: 'Connect a WebSocket request',
      description:
        'Open the connection of a WebSocket request using its saved url, headers, auth and the active environment, ' +
        'exactly like pressing Connect in insomniaMcpWsListener. Messages are then visible in the app and via websocket_read_messages. ' +
        'For an ASP.NET Core SignalR hub set protocol to "signalr" (or enable "SignalR hub" in the request settings): the handshake is done and the connection is kept alive ' +
        'with pings automatically (the hub url usually needs an access_token query parameter).',
      inputSchema: {
        requestId: z.string(),
        protocol: z
          .enum(['websocket', 'signalr'])
          .optional()
          .describe('Default "websocket". Use "signalr" for SignalR hubs.'),
        timeoutMs: z.number().int().positive().max(MAX_WAIT_MS).optional().describe('Default 10000'),
      },
    },
    safe(async args => json(await connect(args))),
  );

  registerTool(
    'websocket_read_messages',
    {
      title: 'Read WebSocket messages',
      description:
        "Read the event log (open, incoming/outgoing messages, close, error) of the request's latest connection, oldest first. " +
        'Pass nextAfterIndex from the previous call as afterIndex to only get new events, and waitMs to wait for them. ' +
        'SignalR messages are decoded into a signalR array (e.g. { kind: "invocation", target, arguments }) and pings are hidden.',
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
    'websocket_send',
    {
      title: 'Send a WebSocket message',
      description: 'Send a text message on a connected WebSocket request. To send JSON, pass it as a string.',
      inputSchema: {
        requestId: z.string(),
        message: z.string(),
      },
    },
    safe(async ({ requestId, message }) => {
      await getWebSocketRequestOrThrow(requestId);
      if (!(await getWebSocketReadyState({ requestId }))) {
        throw new Error('The request is not connected. Call websocket_connect first.');
      }
      await sendWebSocketEvent({ requestId, payload: message });
      return json({ sent: true });
    }),
  );

  registerTool(
    'signalr_invoke',
    {
      title: 'Invoke a SignalR hub method',
      description:
        'Call a hub method on a SignalR connection (opened with websocket_connect protocol "signalr"), like ' +
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
        'that you read with websocket_read_messages, until a completion frame ends the stream.',
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
    'websocket_disconnect',
    {
      title: 'Disconnect a WebSocket request',
      description: 'Close the connection of a WebSocket request. Its event log stays readable.',
      inputSchema: {
        requestId: z.string(),
      },
    },
    safe(async ({ requestId }) => {
      await getWebSocketRequestOrThrow(requestId);
      closeWebSocketConnection({ requestId });
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
  notifyConnectWebSocketResult: (id: string, result: { error?: string }) => void;
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
  ipcMain.on(
    'mcpServer.connectWebSocketResult',
    (event, { id, result }: { id: string; result: { error?: string } }) => {
      if (event.sender !== getMainWindow()?.webContents) {
        return;
      }
      const resolve = pendingConnectRequests.get(id);
      if (!resolve) {
        return;
      }
      pendingConnectRequests.delete(id);
      resolve(result);
    },
  );
};
