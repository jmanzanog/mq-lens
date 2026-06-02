export type Health = {
  status: string;
  brokerConnected: boolean;
  jolokiaAvailable: boolean;
  stompConnected: boolean;
  mode: string;
};

export type DestinationSnapshot = {
  name: string;
  type: 'queue' | 'topic';
  queueSize: number;
  enqueueCount: number;
  dequeueCount: number;
  dispatchCount: number;
  consumerCount: number;
  producerCount: number;
  expiredCount: number;
};

export type BrokerSnapshot = {
  brokerName: string;
  brokerVersion: string;
  uptime: string;
  memoryPercent: number;
  storePercent: number;
  tempPercent: number;
  queues: DestinationSnapshot[];
  topics: DestinationSnapshot[];
  collectedAt: string;
  available: boolean;
  error?: string;
};

export type CapturedMessage = {
  id: string;
  capturedAt: string;
  broker: string;
  originalDestination: string;
  auditDestination: string;
  destinationType: 'queue' | 'topic';
  messageId?: string;
  correlationId?: string;
  replyTo?: string;
  type?: string;
  persistent: boolean;
  priority: number;
  headers: Record<string, string>;
  properties: Record<string, string>;
  bodyFormat: string;
  bodyText?: string;
  bodyBytes?: string;
  bodySize: number;
  bodySha256: string;
  truncated: boolean;
  redacted: boolean;
};

export type Topology = {
  nodes: Array<{ id: string; type: string; label: string; meta?: unknown }>;
  edges: Array<{ source: string; target: string; type: string }>;
};

export type SendTestMessageRequest = {
  destination: string;
  destinationType: 'queue' | 'topic';
  contentType: string;
  body: string;
  headers: Record<string, string>;
};
