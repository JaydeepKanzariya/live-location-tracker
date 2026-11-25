// Tiny structured logger: one JSON line per log so output is greppable in a demo or in production.
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
const minLevel = LEVELS[process.env.LOG_LEVEL ?? 'info'] ?? LEVELS.info;

function write(level, component, message, extra) {
  if (LEVELS[level] < minLevel) return;
  const line = { t: new Date().toISOString(), level, component, msg: message, ...extra };
  const out = level === 'error' || level === 'warn' ? console.error : console.log;
  out(JSON.stringify(line));
}

export function createLogger(component) {
  return {
    debug: (msg, extra) => write('debug', component, msg, extra),
    info: (msg, extra) => write('info', component, msg, extra),
    warn: (msg, extra) => write('warn', component, msg, extra),
    error: (msg, extra) => write('error', component, msg, extra),
  };
}
