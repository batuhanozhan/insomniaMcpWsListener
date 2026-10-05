import fs from 'node:fs';
import type { AddressInfo } from 'node:net';
import os from 'node:os';
import path from 'node:path';

import { services } from 'insomnia-data';
import { Server } from 'socket.io';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { electronMock } from '../../__mocks__/electron';
import { closeSocketIOConnection, getSocketIOReadyState, registerSocketIOHandlers } from './socket-io';

const { handlers, windowSend, NotificationMock } = vi.hoisted(() => ({
  handlers: new Map<string, (...args: any[]) => any>(),
  windowSend: vi.fn(),
  NotificationMock: Object.assign(
    vi.fn().mockImplementation(() => ({ show: vi.fn() })),
    { isSupported: () => true },
  ),
}));
const dataPath = fs.mkdtempSync(path.join(os.tmpdir(), 'socket-io-test-'));
fs.mkdirSync(path.join(dataPath, 'responses'));

vi.mock('electron', () => {
  const electron = {
    app: { getPath: vi.fn(), on: vi.fn() },
    ipcMain: { handle: vi.fn(), on: vi.fn() },
    BrowserWindow: {
      getAllWindows: () => [{ isDestroyed: () => false, webContents: { isDestroyed: () => false, send: windowSend } }],
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

describe('socket-io', () => {
  let io: Server;
  let url: string;
  let workspaceId: string;
  const originalDataPath = process.env.INSOMNIA_DATA_PATH;

  beforeAll(async () => {
    // Event logs and timelines are written to <data path>/responses
    process.env.INSOMNIA_DATA_PATH = dataPath;
    registerSocketIOHandlers();
    // ../ipc/electron is loaded by the test setup, so it registers handlers on the global electron mock
    for (const [channel, listener] of vi.mocked(electronMock.ipcMain.handle).mock.calls) {
      handlers.set(channel, listener);
    }
    io = new Server(0);
    url = `http://127.0.0.1:${(io.httpServer.address() as AddressInfo).port}`;
    const workspace = await services.workspace.create({ name: 'Socket.IO', scope: 'collection' });
    workspaceId = workspace._id;
  });

  afterAll(async () => {
    if (originalDataPath === undefined) {
      delete process.env.INSOMNIA_DATA_PATH;
    } else {
      process.env.INSOMNIA_DATA_PATH = originalDataPath;
    }
    await new Promise(resolve => io.close(resolve));
  });

  beforeEach(() => {
    windowSend.mockClear();
    NotificationMock.mockClear();
  });

  const open = async (request: { _id: string }, requestUrl = url) => {
    await handlers.get('socketIO.open')!(
      {},
      {
        requestId: request._id,
        workspaceId,
        url: requestUrl,
        headers: [],
        authentication: {},
        cookieJar: { cookies: [] },
        query: {},
      },
    );
  };

  it('notifies when the server drops the connection', async () => {
    const request = await services.socketIORequest.create({ parentId: workspaceId, name: 'Chat IO', url });
    await open(request);
    await waitFor(async () => expect(await getSocketIOReadyState({ requestId: request._id })).toBe(true));

    io.disconnectSockets(true);

    await waitFor(() =>
      expect(getToasts()).toEqual([
        {
          content: {
            title: 'Chat IO disconnected',
            description: 'The connection was closed: io server disconnect',
            status: 'error',
          },
        },
      ]),
    );
    expect(NotificationMock).toHaveBeenCalledWith({
      title: 'Chat IO disconnected',
      body: 'The connection was closed: io server disconnect',
    });
  });

  it('does not notify when the developer disconnects', async () => {
    const request = await services.socketIORequest.create({ parentId: workspaceId, name: 'Quiet IO', url });
    await open(request);
    await waitFor(async () => expect(await getSocketIOReadyState({ requestId: request._id })).toBe(true));

    closeSocketIOConnection({ requestId: request._id });
    await waitFor(async () => expect(await getSocketIOReadyState({ requestId: request._id })).toBe(false));
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(getToasts()).toEqual([]);
  });

  it('notifies when the connection fails', async () => {
    const request = await services.socketIORequest.create({ parentId: workspaceId, name: 'Offline IO', url });
    // Nothing listens on port 1
    await open(request, 'http://127.0.0.1:1');

    await waitFor(() => expect(getToasts()).toHaveLength(1));
    expect(getToasts()[0].content).toMatchObject({ title: 'Offline IO connection error', status: 'error' });
  });
});
