import { describe, it, expect } from 'vitest';
import { formatTimeWithMs } from './format';

describe('formatTimeWithMs', () => {
  it('formats ISO string correctly in local time', () => {
    // We mock the local timezone to UTC for predictable tests, 
    // but JS date formatting uses local time by default.
    // Instead of forcing timezone, we check the relative components.
    const iso = '2024-01-01T14:30:45.123Z';
    const d = new Date(iso);
    const expected = `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}.123`;
    expect(formatTimeWithMs(iso)).toBe(expected);
  });

  it('pads single digits', () => {
    const d = new Date();
    d.setHours(9, 5, 2, 7);
    const expected = `09:05:02.007`;
    expect(formatTimeWithMs(d.toISOString())).toBe(expected);
  });
});
