import type { BrokerSnapshot, CapturedMessage, DestinationSnapshot, Health, SendTestMessageRequest, Topology } from './types';

async function getJSON<T>(path: string): Promise<T> {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

async function postJSON<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

export const api = {
  health: () => getJSON<Health>('/api/health'),
  broker: () => getJSON<BrokerSnapshot>('/api/broker/status'),
  destinations: () => getJSON<DestinationSnapshot[]>('/api/destinations'),
  topology: () => getJSON<Topology>('/api/topology'),
  messages: (query = '') => getJSON<CapturedMessage[]>(`/api/messages${query}`),
  message: (id: string) => getJSON<CapturedMessage>(`/api/messages/${id}`),
  sendTestMessage: (request: SendTestMessageRequest) => postJSON<{ status: string }>('/api/dev/send-test-message', request)
};
