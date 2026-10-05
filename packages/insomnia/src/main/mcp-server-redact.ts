// Whatever the MCP server returns is sent to the AI assistant's provider, so credentials are hidden from it.
// Connections still use the real values; only the text handed to the assistant is redacted.

export const REDACTED = '<redacted>';

const SENSITIVE_NAME_PARTS = /token|secret|passw|pwd|api[-_]?key|auth|signature|session|credential|jwt|bearer|cookie/i;
const SENSITIVE_NAMES = new Set(['code', 'key', 'sig', 'pass']);

export const isSensitiveName = (name: string) =>
  SENSITIVE_NAME_PARTS.test(name) || SENSITIVE_NAMES.has(name.toLowerCase());

// A template such as {{ _.token }} only names a variable, it is not the secret itself
const isTemplate = (value: string) => /^\s*(\{\{[^}]*\}\}|\{%[^%]*%\})\s*$/.test(decodeURIComponentSafe(value));

const decodeURIComponentSafe = (value: string) => {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
};

const redactQueryString = (query: string) =>
  query
    .split('&')
    .map(pair => {
      const separator = pair.indexOf('=');
      if (separator === -1) {
        return pair;
      }
      const name = pair.slice(0, separator);
      const value = pair.slice(separator + 1);
      return isSensitiveName(decodeURIComponentSafe(name)) && value && !isTemplate(value)
        ? `${name}=${REDACTED}`
        : pair;
    })
    .join('&');

/** Hides credentials in a url: the user info (user:password@) and sensitive query parameters. */
export const redactUrl = (url: string) => {
  const withoutUserInfo = url.replace(/^([a-z][a-z\d+.-]*:\/\/)[^/?#@\s]*@/i, `$1${REDACTED}@`);
  const queryStart = withoutUserInfo.indexOf('?');
  if (queryStart === -1) {
    return withoutUserInfo;
  }
  const hashStart = withoutUserInfo.indexOf('#', queryStart);
  const queryEnd = hashStart === -1 ? withoutUserInfo.length : hashStart;
  return (
    withoutUserInfo.slice(0, queryStart + 1) +
    redactQueryString(withoutUserInfo.slice(queryStart + 1, queryEnd)) +
    withoutUserInfo.slice(queryEnd)
  );
};

/** Hides credentials that may appear in free text such as error messages. */
export const redactSecrets = (text: string) =>
  text
    .replace(/\b(bearer|basic)\s+[\w\-.~+/]+=*/gi, `$1 ${REDACTED}`)
    .replace(/([a-z][a-z\d+.-]*:\/\/)[^/?#@\s]*@/gi, `$1${REDACTED}@`)
    .replace(/([?&])([^=&\s"']+)=([^&\s"'#]+)/g, (match, prefix: string, name: string, value: string) =>
      isSensitiveName(decodeURIComponentSafe(name)) && !isTemplate(value) ? `${prefix}${name}=${REDACTED}` : match,
    );
