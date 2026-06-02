/**
 * Formats an ISO date string to a local time string including milliseconds.
 * E.g. "14:05:30.123"
 */
export function formatTimeWithMs(isoString: string): string {
  const d = new Date(isoString);
  const pad = (n: number, len = 2) => String(n).padStart(len, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}
