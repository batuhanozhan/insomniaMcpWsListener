import net from 'node:net';

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { ipcMain } from 'electron';
import { services } from 'insomnia-data';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { registerMcpServerHandlers, watchMcpServerSettings } from './mcp-server';
import { HANDSHAKE_REQUEST, parseSignalRFrames, PING_MESSAGE, RECORD_SEPARATOR } from './network/signalr';
import {
  closeWebSocketConnection,
  findMany,
  getWebSocketReadyState,
  sendWebSocketEvent,
  startSignalRSession,
} from './network/websocket';

vi.mock('./network/websocket', () => ({
  getWebSocketReadyState: vi.fn(),
  sendWebSocketEvent: vi.fn(),
  isSignalRSession: vi.fn(() => false),
  startSignalRSession: vi.fn(),
  closeWebSocketConnection: vi.fn(),
  findMany: vi.fn(),
}));

const webContentsSend = vi.fn();
const mainWindow = { webContents: { send: webContentsSend } };
vi.mock('./window-utils', () => ({ getMainWindow: () => mainWindow }));

const getFreePort = () =>
  new Promise<number>(resolve => {
    const server = net.createServer();
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as net.AddressInfo;
      server.close(() => resolve(port));
    });
  });

const waitFor = async (assertion: () => void | Promise<void>, timeoutMs = 3000) => {
  const deadline = Date.now() + timeoutMs;
  while (true) {
    try {
      await assertion();
      return;
    } catch (error) {
      if (Date.now() > deadline) {
        throw error;
      }
      await new Promise(resolve => setTimeout(resolve, 50));
    }
  }
};

const parse = (result: Awaited<ReturnType<Client['callTool']>>) =>
  JSON.parse((result.content as { type: 'text'; text: string }[])[0].text);

describe('mcp-server', () => {
  let port: number;
  let url: string;
  let client: Client;
  let requestId: string;
  let workspaceId: string;
  let connected = false;

  beforeAll(async () => {
    registerMcpServerHandlers();
    port = await getFreePort();
    url = `http://127.0.0.1:${port}/mcp`;
    await services.settings.patch({ mcpServerEnabled: true, mcpServerPort: port });
    await watchMcpServerSettings();

    const workspace = await services.workspace.create({ name: 'Chat API', scope: 'collection' });
    workspaceId = workspace._id;
    const request = await services.webSocketRequest.create({
      parentId: workspaceId,
      name: 'Chat socket',
      url: 'wss://example.com/chat',
    });
    requestId = request._id;

    client = new Client({ name: 'test', version: '1.0.0' });
    await client.connect(new StreamableHTTPClientTransport(new URL(url)));
  });

  afterAll(async () => {
    await client.close();
    await services.settings.patch({ mcpServerEnabled: false });
  });

  beforeEach(() => {
    vi.mocked(getWebSocketReadyState).mockImplementation(async () => connected);
  });

  it('exposes the WebSocket tools', async () => {
    const { tools } = await client.listTools();
    expect(tools.map(tool => tool.name).sort()).toEqual(
      [
        'signalr_cancel_stream',
        'signalr_invoke',
        'signalr_stream',
        'websocket_connect',
        'websocket_disconnect',
        'websocket_list_requests',
        'websocket_read_messages',
        'websocket_send',
      ].sort(),
    );
  });

  it('lists WebSocket requests with their workspace', async () => {
    const result = parse(await client.callTool({ name: 'websocket_list_requests', arguments: {} }));
    expect(result).toEqual([
      {
        requestId,
        name: 'Chat socket',
        url: 'wss://example.com/chat',
        workspaceId,
        workspaceName: 'Chat API',
        connected: false,
        signalR: false,
      },
    ]);
  });

  it('connects through the renderer and waits for the connection to open', async () => {
    const ipcListener = vi
      .mocked(ipcMain.on)
      .mock.calls.find(([channel]) => channel === 'mcpServer.connectWebSocketResult')![1];
    webContentsSend.mockImplementationOnce((channel: string, id: string, options: unknown) => {
      expect(channel).toBe('mcpServer.connectWebSocket');
      expect(options).toEqual({ requestId, workspaceId, isSignalR: false });
      connected = true;
      ipcListener({ sender: mainWindow.webContents } as any, { id, result: {} });
    });

    const result = await client.callTool({ name: 'websocket_connect', arguments: { requestId } });
    expect(parse(result)).toEqual({ connected: true, url: 'wss://example.com/chat' });
  });

  it('reads events oldest first and pages with afterIndex', async () => {
    const response = await services.webSocketResponse.create({ parentId: requestId });
    vi.mocked(findMany).mockImplementation(async ({ responseId }) => {
      expect(responseId).toBe(response._id);
      // findMany returns the newest event first
      return [
        { _id: '3', requestId, type: 'message', direction: 'INCOMING', data: 'hello', timestamp: 3000 },
        { _id: '2', requestId, type: 'message', direction: 'OUTGOING', data: 'hi', timestamp: 2000 },
        { _id: '1', requestId, type: 'open', timestamp: 1000 },
      ] as any;
    });

    const first = parse(await client.callTool({ name: 'websocket_read_messages', arguments: { requestId, limit: 2 } }));
    expect(first).toMatchObject({
      responseId: response._id,
      connected: true,
      nextAfterIndex: 2,
      hasMore: true,
      events: [
        { index: 1, type: 'open' },
        { index: 2, type: 'message', direction: 'OUTGOING', data: 'hi' },
      ],
    });

    const second = parse(
      await client.callTool({ name: 'websocket_read_messages', arguments: { requestId, afterIndex: 2 } }),
    );
    expect(second).toMatchObject({
      nextAfterIndex: 3,
      hasMore: false,
      events: [{ index: 3, type: 'message', direction: 'INCOMING', data: 'hello' }],
    });
  });

  it('sends messages and disconnects', async () => {
    const sent = await client.callTool({ name: 'websocket_send', arguments: { requestId, message: '{"a":1}' } });
    expect(parse(sent)).toEqual({ sent: true });
    expect(sendWebSocketEvent).toHaveBeenCalledWith({ requestId, payload: '{"a":1}' });

    await client.callTool({ name: 'websocket_disconnect', arguments: { requestId } });
    expect(closeWebSocketConnection).toHaveBeenCalledWith({ requestId });
  });

  it('refuses to send when the request is not connected', async () => {
    connected = false;
    const result = await client.callTool({ name: 'websocket_send', arguments: { requestId, message: 'x' } });
    expect(result.isError).toBe(true);
  });

  it('connects to a SignalR hub and invokes hub methods', async () => {
    const hub = await services.webSocketRequest.create({
      parentId: workspaceId,
      name: 'Tasks hub',
      url: 'wss://example.com/hub',
    });
    await services.webSocketResponse.create({ parentId: hub._id });

    // A fake SignalR server answering the handshake and invocations
    const events: any[] = [];
    const push = (direction: 'INCOMING' | 'OUTGOING', data: string) =>
      events.push({ _id: String(events.length), requestId: hub._id, type: 'message', direction, data, timestamp: 1 });
    vi.mocked(findMany).mockImplementation(async () => [...events].reverse());
    vi.mocked(sendWebSocketEvent).mockImplementation(async ({ payload }) => {
      push('OUTGOING', payload);
      const [frame] = parseSignalRFrames(payload) ?? [];
      if (frame?.protocol) {
        push('INCOMING', '{}' + RECORD_SEPARATOR);
      }
      if (frame?.type === 4) {
        push(
          'INCOMING',
          JSON.stringify({ type: 2, invocationId: frame.invocationId, item: { progress: 50 } }) + RECORD_SEPARATOR,
        );
        push('INCOMING', JSON.stringify({ type: 3, invocationId: frame.invocationId }) + RECORD_SEPARATOR);
      } else if (frame?.type === 1 && frame.invocationId) {
        push(
          'INCOMING',
          JSON.stringify({ type: 3, invocationId: frame.invocationId, result: { ok: true } }) + RECORD_SEPARATOR,
        );
      }
    });

    const ipcListener = vi
      .mocked(ipcMain.on)
      .mock.calls.find(([channel]) => channel === 'mcpServer.connectWebSocketResult')![1];
    connected = false;
    // The app opens the connection and websocket.ts sends the handshake when it is a SignalR session
    webContentsSend.mockImplementationOnce((_channel: string, id: string, options: unknown) => {
      expect(options).toEqual({ requestId: hub._id, workspaceId, isSignalR: true });
      connected = true;
      sendWebSocketEvent({ requestId: hub._id, payload: HANDSHAKE_REQUEST });
      ipcListener({ sender: mainWindow.webContents } as any, { id, result: {} });
    });

    const result = await client.callTool({
      name: 'websocket_connect',
      arguments: { requestId: hub._id, protocol: 'signalr' },
    });
    expect(parse(result)).toEqual({
      connected: true,
      url: 'wss://example.com/hub',
      signalR: { handshake: 'ok', keepAlive: true },
    });

    // Pings are hidden, invocations are decoded
    push('INCOMING', PING_MESSAGE);
    push(
      'INCOMING',
      JSON.stringify({ type: 1, target: 'ReceiveMessage', arguments: ['task-1', 50] }) + RECORD_SEPARATOR,
    );
    const read = parse(
      await client.callTool({ name: 'websocket_read_messages', arguments: { requestId: hub._id, afterIndex: 2 } }),
    );
    expect(read).toMatchObject({
      nextAfterIndex: 4,
      hasMore: false,
      events: [
        {
          index: 4,
          direction: 'INCOMING',
          signalR: [{ kind: 'invocation', target: 'ReceiveMessage', arguments: ['task-1', 50] }],
        },
      ],
    });

    const invoked = await client.callTool({
      name: 'signalr_invoke',
      arguments: { requestId: hub._id, target: 'Subscribe', arguments: ['task-1'] },
    });
    expect(parse(invoked)).toEqual({ result: { ok: true } });
    expect(parseSignalRFrames(events.at(-2).data)).toEqual([
      { type: 1, target: 'Subscribe', arguments: ['task-1'], invocationId: expect.any(String) },
    ]);

    const stream = parse(
      await client.callTool({
        name: 'signalr_stream',
        arguments: { requestId: hub._id, target: 'GetProgress', arguments: ['work-1'] },
      }),
    );
    expect(stream).toMatchObject({ invocationId: expect.any(String), completed: true });
    const items = parse(
      await client.callTool({
        name: 'websocket_read_messages',
        arguments: { requestId: hub._id, afterIndex: stream.readFromIndex },
      }),
    );
    expect(items.events.flatMap((event: any) => event.signalR)).toEqual([
      { kind: 'streamInvocation', invocationId: stream.invocationId, target: 'GetProgress', arguments: ['work-1'] },
      { kind: 'streamItem', invocationId: stream.invocationId, item: { progress: 50 } },
      { kind: 'completion', invocationId: stream.invocationId },
    ]);

    await client.callTool({
      name: 'signalr_cancel_stream',
      arguments: { requestId: hub._id, invocationId: stream.invocationId },
    });
    expect(parseSignalRFrames(events.at(-1).data)).toEqual([{ type: 5, invocationId: stream.invocationId }]);

    await client.callTool({ name: 'websocket_disconnect', arguments: { requestId: hub._id } });
    expect(closeWebSocketConnection).toHaveBeenCalledWith({ requestId: hub._id });

    // A connection opened with the Connect button gets the handshake it is missing
    events.length = 0;
    vi.mocked(startSignalRSession).mockImplementationOnce(({ requestId: id }) => {
      sendWebSocketEvent({ requestId: id, payload: HANDSHAKE_REQUEST });
    });
    const reconnected = await client.callTool({
      name: 'websocket_connect',
      arguments: { requestId: hub._id, protocol: 'signalr' },
    });
    expect(parse(reconnected)).toMatchObject({ signalR: { handshake: 'ok', keepAlive: true } });
    expect(startSignalRSession).toHaveBeenCalledWith({ requestId: hub._id, sendHandshake: true });
    await client.callTool({ name: 'websocket_disconnect', arguments: { requestId: hub._id } });
  });

  it('rejects requests from web pages', async () => {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Origin': 'https://evil.example' },
      body: '{}',
    });
    expect(response.status).toBe(403);
  });

  it('stops when the setting is turned off', async () => {
    await services.settings.patch({ mcpServerEnabled: false });
    await waitFor(async () => {
      await expect(fetch(url, { method: 'POST' })).rejects.toThrow();
    });
  });
});
