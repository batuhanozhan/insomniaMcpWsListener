import clsx from 'clsx';
import React, { useEffect, useState } from 'react';
import { Button } from 'react-aria-components';

import type { McpServerStatus } from '~/main/mcp-server';
import { useRootLoaderData } from '~/root';

import { Icon } from '../icon';
import { BooleanSetting } from './boolean-setting';
import { NumberSetting } from './number-setting';

const CopyableValue = ({ label, value }: { label: string; value: string }) => (
  <div className="flex flex-col gap-1">
    <span className="text-sm text-(--hl)">{label}</span>
    <div className="flex items-center gap-2 rounded-xs border border-solid border-(--hl-sm) bg-(--hl-xs) px-2 py-1">
      <code className="flex-1 overflow-x-auto font-mono text-sm whitespace-pre select-text">{value}</code>
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
        shows up in the request&apos;s event log in the app. The server only accepts connections from this computer.
      </p>
      <BooleanSetting label="Enable MCP server" setting="mcpServerEnabled" />
      <NumberSetting label="Port" setting="mcpServerPort" min={1024} max={65_535} />

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

      {status.running && status.url && (
        <div className="flex flex-col gap-3">
          <CopyableValue label="Server URL" value={status.url} />
          <CopyableValue
            label="Add to Claude Code"
            value={`claude mcp add --transport http geckopulse ${status.url}`}
          />
          <CopyableValue label="Add to Codex" value={`codex mcp add geckopulse --url ${status.url}`} />
          <CopyableValue
            label="Other MCP clients (JSON config)"
            value={JSON.stringify({ mcpServers: { geckopulse: { type: 'http', url: status.url } } }, null, 2)}
          />
          <p className="text-sm text-(--hl)">
            Available tools: websocket_list_requests, websocket_connect, websocket_read_messages, websocket_send,
            signalr_invoke, signalr_stream, signalr_cancel_stream, websocket_disconnect. Keep GeckoPulse open while the
            assistant uses them.
          </p>
        </div>
      )}
    </div>
  );
};
