import fs from 'node:fs';
import type { AddressInfo } from 'node:net';
import os from 'node:os';
import path from 'node:path';

import { services } from 'insomnia-data';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { WebSocketServer } from 'ws';

import { electronMock } from '../../__mocks__/electron';
import { HANDSHAKE_REQUEST, KEEP_ALIVE_INTERVAL_MS, PING_MESSAGE, RECORD_SEPARATOR } from './signalr';
import { closeWebSocketConnection, findMany, getWebSocketReadyState, registerWebSocketHandlers } from './websocket';

const { handlers, windowSend, notificationShow, NotificationMock } = vi.hoisted(() => {
  const notificationShow = vi.fn();
  const NotificationMock = Object.assign(
    vi.fn().mockImplementation(() => ({ show: notificationShow })),
    { isSupported: () => true },
  );
  return {
    handlers: new Map<string, (...args: any[]) => any>(),
    windowSend: vi.fn(),
    notificationShow,
    NotificationMock,
  };
});
const dataPath = fs.mkdtempSync(path.join(os.tmpdir(), 'websocket-test-'));
fs.mkdirSync(path.join(dataPath, 'responses'));

vi.mock('electron', () => {
  const electron = {
    app: { getPath: vi.fn(), on: vi.fn() },
    ipcMain: {
      handle: (channel: string, listener: (...args: any[]) => any) => handlers.set(channel, listener),
      on: (channel: string, listener: (...args: any[]) => any) => handlers.set(channel, listener),
    },
    BrowserWindow: {
      getAllWindows: () => [{ isDestroyed: () => false, webContents: { isDestroyed: () => false, send: windowSend } }],
      // The app is in the background, so a desktop notification is shown too
      getFocusedWindow: () => null,
    },
    Notification: NotificationMock,
  };
  return { default: electron, ...electron };
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
      await new Promise(resolve => setTimeout(resolve, 20));
    }
  }
};

const getToasts = () => windowSend.mock.calls.filter(([channel]) => channel === 'show-toast').map(([, toast]) => toast);

describe('websocket', () => {
  let server: WebSocketServer;
  let url: string;
  let workspaceId: string;
  const received: string[] = [];

  const originalDataPath = process.env.INSOMNIA_DATA_PATH;

  beforeAll(async () => {
    // Event logs and timelines are written to <data path>/responses
    process.env.INSOMNIA_DATA_PATH = dataPath;
    registerWebSocketHandlers();
    // ../ipc/electron is loaded by the test setup, so it registers handlers on the global electron mock
    for (const [channel, listener] of vi.mocked(electronMock.ipcMain.handle).mock.calls) {
      handlers.set(channel, listener);
    }
    // A SignalR hub that accepts the handshake and pings back
    server = new WebSocketServer({ port: 0, host: '127.0.0.1' });
    await new Promise(resolve => server.once('listening', resolve));
    url = `ws://127.0.0.1:${(server.address() as AddressInfo).port}`;
    server.on('connection', socket => {
      socket.on('message', data => {
        const message = data.toString();
        received.push(message);
        if (message === HANDSHAKE_REQUEST) {
          socket.send('{}' + RECORD_SEPARATOR);
          socket.send(PING_MESSAGE);
          socket.send(JSON.stringify({ type: 1, target: 'ReceiveMessage', arguments: ['hi'] }) + RECORD_SEPARATOR);
        }
      });
    });
    const workspace = await services.workspace.create({ name: 'Hubs', scope: 'collection' });
    workspaceId = workspace._id;
  });

  afterAll(async () => {
    if (originalDataPath === undefined) {
      delete process.env.INSOMNIA_DATA_PATH;
    } else {
      process.env.INSOMNIA_DATA_PATH = originalDataPath;
    }
    await new Promise(resolve => server.close(resolve));
  });

  beforeEach(() => {
    received.length = 0;
    windowSend.mockClear();
    notificationShow.mockClear();
    NotificationMock.mockClear();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  const open = async (request: { _id: string }) => {
    await handlers.get('webSocket.open')!(
      {},
      { requestId: request._id, workspaceId, url, headers: [], authentication: {}, cookieJar: { cookies: [] } },
    );
    await waitFor(async () => expect(await getWebSocketReadyState({ requestId: request._id })).toBe(true));
  };

  const getEvents = async (requestId: string) => {
    const response = await services.webSocketResponse.getLatestForRequestId(requestId, null);
    return response ? (await findMany({ responseId: response._id })).reverse() : [];
  };

  it('sends the SignalR handshake, keeps the connection alive and leaves pings out of the event log', async () => {
    const request = await services.webSocketRequest.create({ parentId: workspaceId, url, settingSignalR: true });
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    await open(request);

    await waitFor(() => expect(received).toEqual([HANDSHAKE_REQUEST]));
    vi.advanceTimersByTime(KEEP_ALIVE_INTERVAL_MS);
    await waitFor(() => expect(received).toEqual([HANDSHAKE_REQUEST, PING_MESSAGE]));

    await waitFor(async () => {
      const messages = (await getEvents(request._id)).filter(event => event.type === 'message');
      expect(messages.map(event => 'data' in event && event.data)).toEqual([
        HANDSHAKE_REQUEST,
        '{}' + RECORD_SEPARATOR,
        JSON.stringify({ type: 1, target: 'ReceiveMessage', arguments: ['hi'] }) + RECORD_SEPARATOR,
      ]);
    });

    closeWebSocketConnection({ requestId: request._id });
    await waitFor(async () => expect(await getWebSocketReadyState({ requestId: request._id })).toBe(false));
  });

  it('does not talk SignalR to plain WebSocket requests', async () => {
    const request = await services.webSocketRequest.create({ parentId: workspaceId, url });
    await open(request);
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(received).toEqual([]);

    closeWebSocketConnection({ requestId: request._id });
    await waitFor(async () => expect(await getWebSocketReadyState({ requestId: request._id })).toBe(false));
  });

  it('notifies when the server drops the connection', async () => {
    const request = await services.webSocketRequest.create({ parentId: workspaceId, name: 'Tasks hub', url });
    await open(request);

    for (const client of server.clients) {
      client.close(1011, 'Server restarting');
    }

    await waitFor(() =>
      expect(getToasts()).toEqual([
        {
          content: {
            title: 'Tasks hub disconnected',
            description: 'The server closed the connection with code 1011: Server restarting',
            status: 'error',
          },
        },
      ]),
    );
    expect(NotificationMock).toHaveBeenCalledWith({
      title: 'Tasks hub disconnected',
      body: 'The server closed the connection with code 1011: Server restarting',
    });
    expect(notificationShow).toHaveBeenCalled();
  });

  it('does not notify when the developer disconnects', async () => {
    const request = await services.webSocketRequest.create({ parentId: workspaceId, name: 'Quiet', url });
    await open(request);

    closeWebSocketConnection({ requestId: request._id });
    await waitFor(async () => expect(await getWebSocketReadyState({ requestId: request._id })).toBe(false));
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(getToasts()).toEqual([]);
  });

  it('notifies when the connection fails', async () => {
    const request = await services.webSocketRequest.create({ parentId: workspaceId, name: 'Offline hub', url });
    // Nothing listens on port 1
    await handlers.get('webSocket.open')!(
      {},
      {
        requestId: request._id,
        workspaceId,
        url: 'ws://127.0.0.1:1',
        headers: [],
        authentication: {},
        cookieJar: { cookies: [] },
      },
    );

    await waitFor(() => expect(getToasts()).toHaveLength(1));
    expect(getToasts()[0].content).toMatchObject({ title: 'Offline hub connection error', status: 'error' });
  });
});
