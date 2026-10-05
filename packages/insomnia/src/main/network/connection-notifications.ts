import { BrowserWindow, Notification } from 'electron';

// Lets the developer know a realtime connection dropped or failed, also when the app is in the background
export const notifyConnectionProblem = (title: string, description: string) => {
  for (const window of BrowserWindow.getAllWindows()) {
    if (window.isDestroyed() || window.webContents.isDestroyed()) {
      continue;
    }
    window.webContents.send('show-toast', { content: { title, description, status: 'error' } });
  }
  if (!BrowserWindow.getFocusedWindow() && Notification.isSupported()) {
    new Notification({ title, body: description }).show();
  }
};
