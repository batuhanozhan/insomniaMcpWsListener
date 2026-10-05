import { services } from 'insomnia-data';

import { buildQueryStringFromParams, joinUrlAndQueryString } from '~/common/utils/url/querystring';
import { type RAToastContent, showToast } from '~/ui/components/toast-notification';
import * as themes from '~/ui/plugins/misc';
import { plugins } from '~/ui/plugins/renderer-bridge';
import * as templating from '~/ui/templating/renderer-safe';
import { renderRealtimeConnectPayload } from '~/ui/utils/render-realtime-connect';

import { dispatchEditorUndo } from './components/.client/codemirror/editor-undo';
import { showModal } from './components/modals';
import { SettingsModal } from './components/modals/settings-modal';

// Edit-menu Undo/Redo route here so one handler reconciles CodeMirror's history
// with the native undo stack based on what's focused (see editor-undo.ts).
window.main.on('edit:undo', () => dispatchEditorUndo('undo'));
window.main.on('edit:redo', () => dispatchEditorUndo('redo'));

window.main.on('toggle-preferences', () => {
  showModal(SettingsModal);
});

window.main.on('reload-plugins', async () => {
  const settings = await services.settings.get();
  await plugins.reloadPlugins();
  await themes.applyColorScheme(settings);
  templating.reload();
  console.log('[plugins] reloaded');
});

window.main.on('toggle-preferences-shortcuts', () => {
  showModal(SettingsModal, { tab: 'keyboard' });
});

window.main.on('show-toast', (_, options: { content: RAToastContent; options?: { timeout?: number } }) => {
  showToast(options.content, options.options);
});

window.main.on('plugins.uiAlert', (_, options: Record<string, any>) => {
  window.showAlert?.(options);
});

window.main.on('plugins.uiDialog', (_, options: Record<string, any>) => {
  window.showWrapper?.(options);
});

window.main.on('ui.prompt', (_, id: string, options: Record<string, any>) => {
  window.showPrompt?.({
    ...options,
    onComplete: (value: string) => {
      window.main.notifyPromptResult(id, value);
    },
    onHide: () => {
      window.main.notifyPromptResult(id, null);
    },
  });
});

// The local MCP server asks us to connect a WebSocket request, rendering it like the Connect button does
window.main.on(
  'mcpServer.connectWebSocket',
  async (_, id: string, options: { requestId: string; workspaceId: string; isSignalR: boolean }) => {
    try {
      const request = await services.webSocketRequest.getById(options.requestId);
      if (!request) {
        window.main.mcpServer.notifyConnectWebSocketResult(id, { error: 'Request not found.' });
        return;
      }
      const workspaceMeta = await services.workspaceMeta.getOrCreateByParentId(options.workspaceId);
      const activeEnvironment =
        workspaceMeta.activeEnvironmentId && (await services.environment.getById(workspaceMeta.activeEnvironmentId));
      const environment = activeEnvironment || (await services.environment.getOrCreateForParentId(options.workspaceId));
      const rendered = await renderRealtimeConnectPayload({
        request,
        environmentId: environment._id,
        workspaceId: options.workspaceId,
      });
      if (!rendered) {
        window.main.mcpServer.notifyConnectWebSocketResult(id, {
          error: 'Failed to render the request (template error). Check the request in GeckoPulse.',
        });
        return;
      }
      await window.main.webSocket.open({
        requestId: request._id,
        workspaceId: options.workspaceId,
        url: joinUrlAndQueryString(rendered.url, buildQueryStringFromParams(rendered.parameters)),
        headers: rendered.headers,
        authentication: rendered.authentication,
        cookieJar: rendered.workspaceCookieJar,
        suppressUserAgent: rendered.suppressUserAgent,
        isSignalR: options.isSignalR,
      });
      window.main.mcpServer.notifyConnectWebSocketResult(id, {});
    } catch (error) {
      window.main.mcpServer.notifyConnectWebSocketResult(id, {
        error: error instanceof Error ? error.message : String(error),
      });
    }
  },
);
