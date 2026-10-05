import { describe, expect, it } from 'vitest';

import {
  describeSignalRFrame,
  encodeCancelInvocation,
  encodeInvocation,
  encodeStreamInvocation,
  parseSignalRFrames,
  RECORD_SEPARATOR,
} from './signalr';

describe('signalr', () => {
  it('splits a message carrying several frames', () => {
    const data = `{}${RECORD_SEPARATOR}{"type":6}${RECORD_SEPARATOR}`;
    expect(parseSignalRFrames(data)).toEqual([{}, { type: 6 }]);
  });

  it('returns null for messages that are not SignalR', () => {
    expect(parseSignalRFrames('{"type":1}')).toBeNull();
    expect(parseSignalRFrames(`not json${RECORD_SEPARATOR}`)).toBeNull();
    expect(parseSignalRFrames(`[1]${RECORD_SEPARATOR}`)).toBeNull();
  });

  it('describes frames', () => {
    expect(describeSignalRFrame({ protocol: 'json', version: 1 })).toEqual({
      kind: 'handshakeRequest',
      protocol: 'json',
      version: 1,
    });
    expect(describeSignalRFrame({})).toEqual({ kind: 'handshake', ok: true });
    expect(describeSignalRFrame({ error: 'Requested protocol is not available' })).toEqual({
      kind: 'handshake',
      error: 'Requested protocol is not available',
    });
    expect(describeSignalRFrame({ type: 1, target: 'ReceiveMessage', arguments: [1] })).toEqual({
      kind: 'invocation',
      target: 'ReceiveMessage',
      arguments: [1],
    });
    expect(describeSignalRFrame({ type: 3, invocationId: '1', error: 'boom' })).toEqual({
      kind: 'completion',
      invocationId: '1',
      error: 'boom',
    });
    expect(describeSignalRFrame({ type: 7, error: 'Server shutting down', allowReconnect: true })).toEqual({
      kind: 'close',
      error: 'Server shutting down',
      allowReconnect: true,
    });
  });

  it('encodes invocations', () => {
    expect(encodeInvocation({ target: 'Join', args: ['a'] })).toBe(
      `{"type":1,"target":"Join","arguments":["a"]}${RECORD_SEPARATOR}`,
    );
    expect(encodeInvocation({ target: 'Join', args: [], invocationId: '7' })).toBe(
      `{"type":1,"target":"Join","arguments":[],"invocationId":"7"}${RECORD_SEPARATOR}`,
    );
    expect(encodeStreamInvocation({ target: 'GetProgress', args: ['w'], invocationId: '8' })).toBe(
      `{"type":4,"invocationId":"8","target":"GetProgress","arguments":["w"]}${RECORD_SEPARATOR}`,
    );
    expect(encodeCancelInvocation('8')).toBe(`{"type":5,"invocationId":"8"}${RECORD_SEPARATOR}`);
  });
});
