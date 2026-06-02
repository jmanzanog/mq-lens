export function connectEvents(onEvent: (type: string, payload: unknown) => void): EventSource {
  const source = new EventSource('/api/events');
  for (const type of ['message.captured', 'broker.status.changed', 'topology.changed', 'error']) {
    source.addEventListener(type, (event) => {
      try {
        onEvent(type, JSON.parse((event as MessageEvent).data));
      } catch {
        onEvent(type, (event as MessageEvent).data);
      }
    });
  }
  source.onerror = () => onEvent('sse.error', { message: 'SSE disconnected' });
  return source;
}
