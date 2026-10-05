import type { AllTypes, BaseModel, Request, SocketIORequest, WebSocketRequest, Workspace } from 'insomnia-data';
import { models, services } from 'insomnia-data';

import { database as db } from '~/common/database';

import { closeCurlConnection, type CurlEvent, findMany as findCurlEvents, getCurlReadyState } from './network/curl';
import {
  closeSocketIOConnection,
  findMany as findSocketIOEvents,
  getSocketIOReadyState,
  type SocketIOEvent,
} from './network/socket-io';
import {
  closeWebSocketConnection,
  findMany as findWebSocketEvents,
  getWebSocketReadyState,
  type WebSocketEvent,
} from './network/websocket';

// One place that knows how each realtime request type is stored and connected, so the MCP tools don't have to.
// GraphQL subscriptions run on the WebSocket code (with graphql-ws) and Event Streams (SSE) on the curl code.

export type RealtimeKind = 'websocket' | 'graphql-subscription' | 'socketio' | 'event-stream';
export type RealtimeRequest = WebSocketRequest | SocketIORequest | Request;
export type RealtimeEvent = WebSocketEvent | SocketIOEvent | CurlEvent;

interface RealtimeAdapter {
  responseType: AllTypes;
  isConnected: (requestId: string) => Promise<boolean>;
  // Newest event first, like the app's event log
  findEvents: (responseId: string) => Promise<RealtimeEvent[]>;
  close: (requestId: string) => void;
}

const webSocketAdapter: RealtimeAdapter = {
  responseType: models.webSocketResponse.type,
  isConnected: requestId => getWebSocketReadyState({ requestId }),
  findEvents: responseId => findWebSocketEvents({ responseId }),
  close: requestId => closeWebSocketConnection({ requestId }),
};

export const adapters: Record<RealtimeKind, RealtimeAdapter> = {
  'websocket': webSocketAdapter,
  'graphql-subscription': webSocketAdapter,
  'socketio': {
    responseType: models.socketIOResponse.type,
    isConnected: requestId => getSocketIOReadyState({ requestId }),
    findEvents: responseId => findSocketIOEvents({ responseId }),
    close: requestId => closeSocketIOConnection({ requestId }),
  },
  'event-stream': {
    responseType: models.response.type,
    isConnected: requestId => getCurlReadyState({ requestId }),
    findEvents: responseId => findCurlEvents({ responseId }),
    close: requestId => closeCurlConnection(null, { requestId }),
  },
};

const getRequestKind = (request: RealtimeRequest): RealtimeKind | null => {
  if (models.webSocketRequest.isWebSocketRequest(request)) {
    return 'websocket';
  }
  if (models.socketIORequest.isSocketIORequest(request)) {
    return 'socketio';
  }
  if (models.request.isGraphqlSubscriptionRequest(request)) {
    return 'graphql-subscription';
  }
  if (models.request.isEventStreamRequest(request)) {
    return 'event-stream';
  }
  return null;
};

export interface RealtimeRequestInfo {
  request: RealtimeRequest;
  kind: RealtimeKind;
  adapter: RealtimeAdapter;
}

const toInfo = (request: RealtimeRequest | null | undefined): RealtimeRequestInfo | null => {
  const kind = request && getRequestKind(request);
  return kind ? { request, kind, adapter: adapters[kind] } : null;
};

export const getRealtimeRequest = async (requestId: string) =>
  toInfo(
    (await services.webSocketRequest.getById(requestId)) ??
      (await services.socketIORequest.getById(requestId)) ??
      (await services.request.getById(requestId)),
  );

export const listRealtimeRequests = async () => {
  const requests: RealtimeRequest[] = [
    ...(await services.webSocketRequest.all()),
    ...(await db.find<SocketIORequest>(models.socketIORequest.type)),
    ...(await db.find<Request>(models.request.type)),
  ];
  return requests.map(toInfo).filter((info): info is RealtimeRequestInfo => info !== null);
};

export const getWorkspaceForRequest = async (request: RealtimeRequest) => {
  const ancestors = await db.withAncestors<RealtimeRequest | Workspace>(request, [
    models.requestGroup.type,
    models.workspace.type,
  ]);
  return ancestors.find(models.workspace.isWorkspace);
};

export interface RealtimeResponse extends BaseModel {
  error?: string;
  statusCode?: number;
  statusMessage?: string;
}

export const getLatestResponse = ({ request, adapter }: RealtimeRequestInfo) =>
  db.findOne<RealtimeResponse>(adapter.responseType, { parentId: request._id }, { created: -1 });

/** The events of the request's latest connection, oldest first. */
export const getEvents = async (info: RealtimeRequestInfo) => {
  const response = await getLatestResponse(info);
  const events = response ? (await info.adapter.findEvents(response._id)).reverse() : [];
  return { response, events };
};
