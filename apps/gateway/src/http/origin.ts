/** Reduces a URL or origin string to `scheme://host[:port]`, or null if it is not a valid URL. */
export function normalizeOrigin(value: string): string | null {
  try {
    const url = new URL(value);
    return url.origin === 'null' ? null : url.origin;
  } catch {
    return null;
  }
}

export function buildAllowedOrigins(list: string[], appBaseUrl: string): Set<string> {
  const out = new Set<string>();
  for (const item of [...list, appBaseUrl]) {
    const origin = normalizeOrigin(item);
    if (origin) out.add(origin);
  }
  return out;
}

/**
 * Browsers always send Origin on WebSocket upgrades and cross-site POSTs, and cookies ride
 * along with them. Checking it against an allow-list stops other sites from using a logged-in
 * user's cookie (cross-site WebSocket hijacking / CSRF). A missing Origin is refused.
 */
export function isAllowedOrigin(origin: string | undefined, allowed: Set<string>): boolean {
  if (!origin) return false;
  const normalized = normalizeOrigin(origin);
  return normalized !== null && allowed.has(normalized);
}

/** Only allow redirects back to a path on this site (blocks open-redirect via ?returnTo=). */
export function safeReturnTo(value: unknown): string {
  if (typeof value !== 'string') return '/';
  if (!value.startsWith('/') || value.startsWith('//') || value.startsWith('/\\')) return '/';
  return value;
}
