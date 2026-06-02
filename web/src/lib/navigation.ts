export type View = 'dashboard' | 'destinations' | 'messages' | 'traces' | 'topology' | 'settings';

export type ViewState = {
  selected: null;
  traceMessages: unknown[];
  traceCorrelationId: string;
};

/**
 * Returns the state resets needed when leaving a given view.
 * TypeScript's Record<View, ...> enforces exhaustiveness.
 */
export function cleanupForView(view: View): Partial<ViewState> {
  const cleanups: Record<View, Partial<ViewState>> = {
    dashboard: {},
    destinations: {},
    messages: { selected: null },
    traces: { traceMessages: [], traceCorrelationId: '' },
    topology: {},
    settings: {},
  };
  return cleanups[view];
}
