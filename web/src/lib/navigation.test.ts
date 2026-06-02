import { describe, it, expect } from 'vitest';
import { cleanupForView } from './navigation';
import type { View } from './navigation';

describe('cleanupForView', () => {
  it('resets selected when leaving messages', () => {
    const result = cleanupForView('messages');
    expect(result).toEqual({ selected: null });
  });

  it('resets trace state when leaving traces', () => {
    const result = cleanupForView('traces');
    expect(result).toEqual({ traceMessages: [], traceCorrelationId: '' });
  });

  it('returns empty object for views without cleanup', () => {
    const noCleanupViews: View[] = ['dashboard', 'destinations', 'topology', 'settings'];
    for (const view of noCleanupViews) {
      expect(cleanupForView(view)).toEqual({});
    }
  });
});
