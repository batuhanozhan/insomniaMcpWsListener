import clsx from 'clsx';
import React, { useEffect, useState } from 'react';
import { Button } from 'react-aria-components';

import type { McpServerStatus } from '~/main/mcp-server';
import { useRootLoaderData } from '~/root';

import { Icon } from '../icon';
import { BooleanSetting } from './boolean-setting';
import { NumberSetting } from './number-setting';

const MASKED_TOKEN = '••••••••';

// `display` lets the access token stay hidden on screen while the copied value contains it
const CopyableValue = ({ label, value, display = value }: { label: string; value: string; display?: string }) => (
  <div className="flex flex-col gap-1">
    <span className="text-sm text-(--hl)">{label}</span>
    <div className="flex items-center gap-2 rounded-xs border border-solid border-(--hl-sm) bg-(--hl-xs) px-2 py-1">
      <code className="flex-1 overflow-x-auto font-mono text-sm whitespace-pre">{display}</code>
      <Button
        aria-label={`Copy ${label}`}
        className="flex h-6 w-6 shrink-0 items-center justify-center rounded-xs text-(--hl) transition-colors hover:bg-(--hl-sm) hover:text-(--color-font)"
        onPress={() => window.clipboard.writeText(value)}
      >
        <Icon icon="copy" />
      </Button>
    </div>
  </div>
);

export const McpServerSettings = () => {
  const { settings } = useRootLoaderData()!;
  const [status, setStatus] = useState<McpServerStatus>({ running: false, url: null, error: null });
  const [accessToken, setAccessToken] = useState<string | null>(null);

  useEffect(() => {
    if (status.running) {
      window.main.mcpServer.getAccessToken().then(setAccessToken);
    }
  }, [status.running]);

  // The server (re)starts asynchronously in the main process after a settings change, so keep polling while visible
  useEffect(() => {
    let isMounted = true;
    const refresh = async () => {
      const nextStatus = await window.main.mcpServer.getStatus();
      if (isMounted) {
        setStatus(nextStatus);
      }
    };
    refresh();
    const interval = setInterval(refresh, 1000);
    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, [settings.mcpServerEnabled, settings.mcpServerPort]);

  return (
    <div className="flex flex-col gap-4 rounded-md border border-solid border-(--hl-sm) bg-(--hl-xs) p-4">
      <h3 className="text-lg font-semibold text-(--color-font)">MCP Server</h3>
      <p className="text-sm text-(--hl)">
        Let AI assistants such as Claude Code, Codex or Claude Desktop use your WebSocket requests: list them, connect,
        listen to incoming messages and send messages. ASP.NET Core SignalR hubs are supported too. Everything also
        shows up in the request&apos;s event log in the app. The server only accepts connections from this computer that
        use its access token.
      </p>
      <p className="flex items-start gap-2 rounded-xs border border-solid border-(--color-warning) p-2 text-sm text-(--color-font)">
        <Icon icon="triangle-exclamation" className="mt-0.5 text-(--color-warning)" />
        <span>
          What the assistant reads, such as the messages of a connection, is sent to its AI provider. Tokens and
          passwords in request URLs are hidden from it, but message contents are not. Only connect requests whose data
          you are allowed to share.
        </span>
      </p>
      <BooleanSetting label="Enable MCP server" setting="mcpServerEnabled" />
      <NumberSetting label="Port" setting="mcpServerPort" min={1024} max={65_535} />
      <BooleanSetting
        label="Read-only"
        setting="mcpServerReadOnly"
        help="The assistant can list, connect, read and disconnect, but cannot send messages or call SignalR hub methods. Turn this off only if you want the assistant to send data to your servers."
      />

      {settings.mcpServerEnabled && (
        <div className="flex items-center gap-2 text-sm">
          <span
            className={clsx(
              'h-2 w-2 rounded-full',
              status.running ? 'bg-(--color-success)' : status.error ? 'bg-(--color-danger)' : 'bg-(--hl)',
            )}
          />
          {status.running ? 'Running' : status.error ? status.error : 'Starting…'}
        </div>
      )}

      {status.running && status.url && accessToken && (
        <div className="flex flex-col gap-3">
          <CopyableValue label="Server URL" value={status.url} />
          <CopyableValue
            label="Add to Claude Code"
            value={`claude mcp add --transport http insomnia-mcp-ws-listener ${status.url} --header "Authorization: Bearer ${accessToken}"`}
            display={`claude mcp add --transport http insomnia-mcp-ws-listener ${status.url} --header "Authorization: Bearer ${MASKED_TOKEN}"`}
          />
          <CopyableValue
            label="Add to Codex (~/.codex/config.toml)"
            value={`[mcp_servers.insomnia-mcp-ws-listener]\nurl = "${status.url}"\nhttp_headers = { "Authorization" = "Bearer ${accessToken}" }`}
            display={`[mcp_servers.insomnia-mcp-ws-listener]\nurl = "${status.url}"\nhttp_headers = { "Authorization" = "Bearer ${MASKED_TOKEN}" }`}
          />
          <CopyableValue
            label="Other MCP clients (JSON config)"
            value={JSON.stringify(
              {
                mcpServers: {
                  'insomnia-mcp-ws-listener': {
                    type: 'http',
                    url: status.url,
                    headers: { Authorization: `Bearer ${accessToken}` },
                  },
                },
              },
              null,
              2,
            )}
            display={JSON.stringify(
              {
                mcpServers: {
                  'insomnia-mcp-ws-listener': {
                    type: 'http',
                    url: status.url,
                    headers: { Authorization: `Bearer ${MASKED_TOKEN}` },
                  },
                },
              },
              null,
              2,
            )}
          />
          <div className="flex items-center justify-between gap-2">
            <p className="text-sm text-(--hl)">
              The copied commands contain the access token; keep them private. Regenerating it disconnects every
              assistant set up with the old one.
            </p>
            <Button
              className="shrink-0 rounded-xs border border-solid border-(--hl-md) px-3 py-1 text-sm text-(--color-font) transition-colors hover:bg-(--hl-sm)"
              onPress={async () => setAccessToken(await window.main.mcpServer.regenerateAccessToken())}
            >
              Regenerate token
            </Button>
          </div>
          <p className="text-sm text-(--hl)">
            Keep insomniaMcpWsListener open while the assistant uses it. Tools: websocket_list_requests,
            websocket_connect, websocket_read_messages, websocket_disconnect
            {settings.mcpServerReadOnly
              ? ' (read-only mode)'
              : ', websocket_send, signalr_invoke, signalr_stream, signalr_cancel_stream'}
            .
          </p>
        </div>
      )}
    </div>
  );
};
