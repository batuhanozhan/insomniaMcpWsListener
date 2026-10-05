import { describe, expect, it } from 'vitest';

import { REDACTED, redactSecrets, redactUrl } from './mcp-server-redact';

describe('mcp-server-redact', () => {
  it('hides sensitive query parameters and keeps the others', () => {
    expect(
      redactUrl('wss://example.com/hub?client_id=abc&access_token=eyJhbGciOi.payload.sig&api_key=123&page=2#top'),
    ).toBe(`wss://example.com/hub?client_id=abc&access_token=${REDACTED}&api_key=${REDACTED}&page=2#top`);
  });

  it('hides credentials in the user info', () => {
    expect(redactUrl('wss://admin:hunter2@example.com/socket')).toBe(`wss://${REDACTED}@example.com/socket`);
  });

  it('keeps template references, which are not secrets', () => {
    expect(redactUrl('wss://example.com/hub?access_token={{ _.token }}')).toBe(
      'wss://example.com/hub?access_token={{ _.token }}',
    );
    expect(redactUrl('wss://example.com/hub?access_token=%7B%7B%20_.token%20%7D%7D')).toBe(
      'wss://example.com/hub?access_token=%7B%7B%20_.token%20%7D%7D',
    );
  });

  it('leaves urls without credentials untouched', () => {
    expect(redactUrl('ws://127.0.0.1:8080/chat?room=1')).toBe('ws://127.0.0.1:8080/chat?room=1');
    expect(redactUrl('{{ _.baseUrl }}/hub')).toBe('{{ _.baseUrl }}/hub');
  });

  it('hides credentials in free text', () => {
    expect(
      redactSecrets(
        'GET /hub?client_id=1&access_token=abc.def failed, Authorization: Bearer abc.def== sent to https://u:p@host',
      ),
    ).toBe(
      `GET /hub?client_id=1&access_token=${REDACTED} failed, Authorization: Bearer ${REDACTED} sent to https://${REDACTED}@host`,
    );
  });
});
