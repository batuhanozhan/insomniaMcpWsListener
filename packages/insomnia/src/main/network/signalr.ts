// SignalR JSON hub protocol over WebSocket, see https://github.com/dotnet/aspnetcore/blob/main/src/SignalR/docs/specs/HubProtocol.md
// Every message is a JSON object terminated by the ASCII record separator, and one WebSocket frame may carry several.

export const RECORD_SEPARATOR = '\u001E';
export const HANDSHAKE_REQUEST = JSON.stringify({ protocol: 'json', version: 1 }) + RECORD_SEPARATOR;
export const PING_MESSAGE = JSON.stringify({ type: 6 }) + RECORD_SEPARATOR;
// Servers drop clients they have not heard from for 30s (ClientTimeoutInterval), the official clients ping every 15s
export const KEEP_ALIVE_INTERVAL_MS = 15_000;

export const MessageType = {
  Invocation: 1,
  StreamItem: 2,
  Completion: 3,
  StreamInvocation: 4,
  CancelInvocation: 5,
  Ping: 6,
  Close: 7,
} as const;

export type SignalRFrame = Record<string, unknown>;

/** Splits a WebSocket message into SignalR frames, or returns null when it is not SignalR. */
export const parseSignalRFrames = (data: string): SignalRFrame[] | null => {
  if (!data.endsWith(RECORD_SEPARATOR)) {
    return null;
  }
  try {
    const frames = data
      .split(RECORD_SEPARATOR)
      .slice(0, -1)
      .map(part => JSON.parse(part));
    return frames.every(frame => frame && typeof frame === 'object' && !Array.isArray(frame)) ? frames : null;
  } catch {
    return null;
  }
};

export const isHandshakeRequest = (frame: SignalRFrame) => 'protocol' in frame && !('type' in frame);

export const isHandshakeResponse = (frame: SignalRFrame) => !('type' in frame) && !isHandshakeRequest(frame);

export const isPing = (frame: SignalRFrame) => frame.type === MessageType.Ping;

/** Turns a frame into a readable shape, e.g. { kind: 'invocation', target: 'ReceiveMessage', arguments: [...] }. */
export const describeSignalRFrame = (frame: SignalRFrame) => {
  if (isHandshakeRequest(frame)) {
    return { kind: 'handshakeRequest', protocol: frame.protocol, version: frame.version };
  }
  if (isHandshakeResponse(frame)) {
    return frame.error ? { kind: 'handshake', error: frame.error } : { kind: 'handshake', ok: true };
  }
  const { type, ...rest } = frame;
  switch (type) {
    case MessageType.Invocation: {
      return { kind: 'invocation', ...rest };
    }
    case MessageType.StreamItem: {
      return { kind: 'streamItem', ...rest };
    }
    case MessageType.Completion: {
      return { kind: 'completion', ...rest };
    }
    case MessageType.StreamInvocation: {
      return { kind: 'streamInvocation', ...rest };
    }
    case MessageType.CancelInvocation: {
      return { kind: 'cancelInvocation', ...rest };
    }
    case MessageType.Ping: {
      return { kind: 'ping' };
    }
    case MessageType.Close: {
      return { kind: 'close', ...rest };
    }
    default: {
      return frame;
    }
  }
};

export const encodeInvocation = ({
  target,
  args,
  invocationId,
}: {
  target: string;
  args: unknown[];
  invocationId?: string;
}) =>
  JSON.stringify({ type: MessageType.Invocation, target, arguments: args, ...(invocationId && { invocationId }) }) +
  RECORD_SEPARATOR;

/** Calls a streaming hub method (IAsyncEnumerable / ChannelReader), like connection.stream(target, ...args). */
export const encodeStreamInvocation = ({
  target,
  args,
  invocationId,
}: {
  target: string;
  args: unknown[];
  invocationId: string;
}) => JSON.stringify({ type: MessageType.StreamInvocation, invocationId, target, arguments: args }) + RECORD_SEPARATOR;

export const encodeCancelInvocation = (invocationId: string) =>
  JSON.stringify({ type: MessageType.CancelInvocation, invocationId }) + RECORD_SEPARATOR;
