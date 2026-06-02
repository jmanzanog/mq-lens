<script lang="ts">
  import cytoscape, { type Core, type ElementDefinition } from 'cytoscape';
  import { api } from './lib/api';
  import { connectEvents } from './lib/events';
  import type { BrokerSnapshot, CapturedMessage, DestinationSnapshot, Health, Topology } from './lib/types';

  type View = 'dashboard' | 'destinations' | 'messages' | 'traces' | 'topology' | 'settings';

  let view = $state<View>('dashboard');
  let health = $state<Health | null>(null);
  let broker = $state<BrokerSnapshot | null>(null);
  let destinations = $state<DestinationSnapshot[]>([]);
  let messages = $state<CapturedMessage[]>([]);
  let traceMessages = $state<CapturedMessage[]>([]);
  let traceCorrelationId = $state('');
  let topology = $state<Topology>({ nodes: [], edges: [] });
  let selected = $state<CapturedMessage | null>(null);
  let query = $state('');
  let destinationFilter = $state('');
  let livePaused = $state(false);
  let error = $state('');
  let eventStatus = $state('SSE pending');
  let devDestination = $state('ORDER.CREATED');
  let devBody = $state('{"orderId":"local-1","status":"created"}');
  let devStatus = $state('');
  let topologyElement = $state<HTMLDivElement | null>(null);
  let topologyInstance: Core | null = null;

  type GraphNode = Topology['nodes'][number];
  type GraphEdge = Topology['edges'][number];

  const queueCount = $derived(destinations.filter((item) => item.type === 'queue').length);
  const topicCount = $derived(destinations.filter((item) => item.type === 'topic').length);
  const consumerCount = $derived(destinations.reduce((sum, item) => sum + item.consumerCount, 0));
  const producerCount = $derived(destinations.reduce((sum, item) => sum + item.producerCount, 0));

  async function refresh() {
    try {
      error = '';
      const params = new URLSearchParams();
      if (query) params.set('contains', query);
      if (destinationFilter) params.set('destination', destinationFilter);
      const messageQuery = params.toString() ? `?${params.toString()}` : '';
      const [nextHealth, nextBroker, nextDestinations, nextMessages, nextTopology] = await Promise.all([
        api.health(),
        api.broker(),
        api.destinations(),
        api.messages(messageQuery),
        api.topology()
      ]);
      health = nextHealth;
      broker = nextBroker;
      destinations = nextDestinations;
      messages = nextMessages;
      topology = nextTopology;
      if (selected) {
        selected = nextMessages.find((item) => item.id === selected?.id) ?? selected;
      }
    } catch (err) {
      error = err instanceof Error ? err.message : 'Request failed';
    }
  }

  async function openMessage(id: string) {
    selected = await api.message(id);
    view = 'messages';
  }

  async function loadTrace(correlationId: string) {
    const id = correlationId.trim();
    if (!id) return;
    traceCorrelationId = id;
    view = 'traces';
    try {
      traceMessages = await api.messages(`?correlationId=${encodeURIComponent(id)}&sort=asc`);
    } catch (err) {
      error = err instanceof Error ? err.message : 'Trace load failed';
    }
  }

  function downloadSelected() {
    if (selected) {
      window.location.href = `/api/messages/${selected.id}/download`;
    }
  }

  async function sendDevMessage() {
    try {
      devStatus = 'Sending';
      await api.sendTestMessage({
        destination: devDestination,
        destinationType: 'queue',
        contentType: 'application/json',
        body: devBody,
        headers: { 'correlation-id': `mq-lens-${Date.now()}` }
      });
      devStatus = 'Sent';
    } catch (err) {
      devStatus = err instanceof Error ? err.message : 'Send failed';
    }
  }

  $effect(() => {
    refresh();
    const timer = window.setInterval(refresh, 5000);
    const source = connectEvents((type) => {
      eventStatus = type === 'sse.error' ? 'SSE reconnecting' : 'SSE live';
      if (!livePaused && (type === 'message.captured' || type === 'broker.status.changed')) {
        refresh();
      }
    });
    return () => {
      window.clearInterval(timer);
      source.close();
    };
  });

  $effect(() => {
    if (view !== 'traces') {
      traceMessages = [];
      traceCorrelationId = '';
    }
  });

  $effect(() => {
    if (view !== 'topology' || !topologyElement) {
      topologyInstance?.destroy();
      topologyInstance = null;
      return;
    }
    if (topologyInstance) {
      return;
    }
    topologyInstance = cytoscape({
      container: topologyElement,
      elements: [],
      style: [
        {
          selector: 'node',
          style: {
            label: 'data(label)',
            'background-color': '#fffdf3',
            'border-width': 2,
            'border-color': '#253028',
            color: '#17201b',
            'font-family': 'Aptos, Segoe UI, sans-serif',
            'font-size': 12,
            'font-weight': 700,
            'text-valign': 'center',
            'text-halign': 'center',
            'text-wrap': 'wrap',
            'text-max-width': 148,
            width: 'data(width)',
            height: 'data(height)',
            shape: 'round-rectangle',
            'overlay-opacity': 0,
            'transition-property': 'background-color, border-color, width',
            'transition-duration': '160ms'
          }
        },
        {
          selector: 'node[role = "cluster"]',
          style: {
            label: 'data(label)',
            'background-color': '#eef1ec',
            'background-opacity': 0.42,
            'border-color': '#c3cbc1',
            'border-width': 1,
            'border-style': 'dashed',
            color: '#667266',
            'font-size': 11,
            'font-weight': 800,
            'text-transform': 'uppercase',
            'text-valign': 'top',
            'text-halign': 'center',
            'text-margin-y': -8,
            padding: 26
          }
        },
        { selector: 'node[type = "broker"]', style: { 'background-color': '#d5e8d0', width: 150, height: 54 } },
        { selector: 'node[type = "queue"]', style: { 'background-color': '#f3e5a6', 'border-color': '#5c4f17' } },
        { selector: 'node[type = "audit"]', style: { 'background-color': '#ffe4d8', 'border-color': '#a34f3c' } },
        { selector: 'node[type = "topic"]', style: { 'background-color': '#d8e7ef', 'border-color': '#35596a' } },
        { selector: 'node[type = "virtual-topic"]', style: { 'background-color': '#e8d8ef', 'border-color': '#60356a' } },
        { selector: 'node[type = "consumer-queue"]', style: { 'background-color': '#f3e5a6', 'border-color': '#5c4f17', 'border-style': 'dotted' } },
        { selector: 'node[type = "consumer"]', style: { 'background-color': '#f8faf5', 'border-color': '#526055' } },
        { selector: 'node[type = "inspector"]', style: { 'background-color': '#17201b', color: '#f7f8f3', width: 150, height: 56 } },
        {
          selector: 'edge',
          style: {
            width: 2.2,
            'line-color': '#7d897e',
            'target-arrow-color': '#7d897e',
            'target-arrow-shape': 'triangle',
            'curve-style': 'taxi',
            'taxi-direction': 'rightward',
            'taxi-turn-min-distance': 18,
            label: 'data(label)',
            'font-size': 10,
            'font-weight': 700,
            color: '#667266',
            'text-background-color': '#fbfcf8',
            'text-background-opacity': 0.92,
            'text-background-padding': 3
          }
        },
        {
          selector: 'edge[type = "audit-copy"]',
          style: {
            'line-style': 'dashed',
            'line-color': '#c55d4b',
            'target-arrow-color': '#c55d4b',
            color: '#9c4b3a'
          }
        },
        { selector: 'edge[type = "observes"]', style: { width: 2.8, 'line-color': '#17201b', 'target-arrow-color': '#17201b', color: '#17201b' } },
        {
          selector: 'edge[type = "owns"]',
          style: {
            width: 1.4,
            opacity: 0.28,
            label: '',
            'line-color': '#9aa59b',
            'target-arrow-color': '#9aa59b'
          }
        },
        {
          selector: 'edge[type = "consumes"]',
          style: {
            width: 1.7,
            opacity: 0.55,
            label: '',
            'line-color': '#8d9a8f',
            'target-arrow-color': '#8d9a8f'
          }
        }
      ],
      layout: { name: 'preset', padding: 42, fit: true },
      minZoom: 0.35,
      maxZoom: 1.8,
      wheelSensitivity: 0.18
    });
    queueMicrotask(() => syncTopologyGraph(topology));
    return () => {
      topologyInstance?.destroy();
      topologyInstance = null;
    };
  });

  $effect(() => {
    if (view !== 'topology' || !topologyInstance) {
      return;
    }
    syncTopologyGraph(topology);
  });

  function syncTopologyGraph(topology: Topology) {
    if (!topologyInstance) {
      return;
    }
    const nextElements = topologyElements(topology);
    const nextIds = new Set(nextElements.map((element) => String(element.data?.id)));

    topologyInstance.batch(() => {
      topologyInstance?.elements().forEach((element) => {
        if (!nextIds.has(element.id())) {
          element.animate({ style: { opacity: 0 } }, { duration: 140, complete: () => element.remove() });
        }
      });

      for (const element of nextElements) {
        const id = String(element.data?.id);
        const existing = topologyInstance?.getElementById(id);
        if (existing?.length) {
          existing.data(element.data ?? {});
          if (element.position && existing.isNode()) {
            existing.animate({ position: element.position }, { duration: 420, easing: 'ease-in-out' });
          }
          continue;
        }

        const added = topologyInstance?.add(element);
        added?.style('opacity', 0);
        added?.animate({ style: { opacity: 1 } }, { duration: 220 });
      }
    });
  }

  function topologyElements(topology: Topology): ElementDefinition[] {
    const columns = topologyColumns(topology.nodes);
    const elements: ElementDefinition[] = [
      { data: { id: 'cluster:broker', role: 'cluster', label: 'Broker' } },
      { data: { id: 'cluster:business', role: 'cluster', label: 'Business queues' } },
      { data: { id: 'cluster:audit', role: 'cluster', label: 'Audit mirror' } },
      { data: { id: 'cluster:advisory', role: 'cluster', label: 'Advisory topics' } },
      { data: { id: 'cluster:runtime', role: 'cluster', label: 'Runtime' } }
    ];

    for (const column of columns) {
      const yStart = 120 - ((column.nodes.length - 1) * column.gap) / 2;
      column.nodes.forEach((node, index) => {
        const fullLabel = node.label;
        const shortLabel = compactTopologyLabel(node);
        elements.push({
          data: {
            id: node.id,
            label: shortLabel,
            shortLabel,
            fullLabel,
            type: visualNodeType(node),
            parent: column.cluster,
            width: nodeWidth(node),
            height: node.type === 'consumer' ? 46 : 54
          },
          position: { x: column.x, y: yStart + index * column.gap }
        });
      });
    }

    elements.push(
      ...topology.edges.map((edge, index) => ({
        data: {
          id: `edge:${index}:${edge.source}:${edge.target}`,
          source: edge.source,
          target: edge.target,
          type: edge.type,
          label: edgeLabel(edge)
        }
      }))
    );
    return elements;
  }

  function topologyColumns(nodes: GraphNode[]) {
    const broker = nodes.filter((node) => node.type === 'broker');
    const business = nodes.filter((node) => node.type === 'queue' && !isAuditQueue(node));
    const audit = nodes.filter((node) => node.type === 'queue' && isAuditQueue(node));
    const advisory = nodes.filter((node) => node.type === 'topic');
    const runtime = nodes.filter((node) => node.type === 'inspector' || node.type === 'consumer');
    return [
      { cluster: 'cluster:broker', x: 80, gap: 86, nodes: broker },
      { cluster: 'cluster:business', x: 330, gap: 86, nodes: business },
      { cluster: 'cluster:audit', x: 610, gap: 86, nodes: audit },
      { cluster: 'cluster:advisory', x: 915, gap: 84, nodes: advisory },
      { cluster: 'cluster:runtime', x: 1255, gap: 94, nodes: runtime }
    ];
  }

  function visualNodeType(node: GraphNode) {
    if (isAuditQueue(node)) return 'audit';
    if (node.type === 'topic' && node.label.startsWith('VirtualTopic.')) return 'virtual-topic';
    if (node.type === 'queue' && node.label.startsWith('Consumer.')) return 'consumer-queue';
    return node.type;
  }

  function isAuditQueue(node: GraphNode) {
    return node.type === 'queue' && node.label.startsWith('LENS.AUDIT.');
  }

  function compactTopologyLabel(node: GraphNode) {
    if (node.type === 'consumer') return 'Consumers';
    if (node.type === 'inspector') return 'MQ Lens';
    if (node.label.startsWith('ActiveMQ.Advisory.Consumer.Queue.')) {
      return `Consumer\n${compactDestination(node.label.replace('ActiveMQ.Advisory.Consumer.Queue.', ''))}`;
    }
    if (node.label.startsWith('LENS.AUDIT.')) return node.label.replace('LENS.AUDIT.', 'AUDIT\n');
    if (node.label.startsWith('VirtualTopic.')) return node.label.replace('VirtualTopic.', 'VirtualTopic\n');
    if (node.label.startsWith('Consumer.')) {
      const parts = node.label.split('.');
      if (parts.length >= 3) return `Consumer\n${parts[1]}`;
    }
    if (node.label.startsWith('ActiveMQ.Advisory.')) return node.label.replace('ActiveMQ.Advisory.', 'Advisory\n');
    return node.label.length > 24 ? `${node.label.slice(0, 21)}...` : node.label;
  }

  function compactDestination(value: string) {
    const normalized = value.replace('LENS.AUDIT.', 'AUDIT.');
    return normalized.length > 22 ? `${normalized.slice(0, 19)}...` : normalized;
  }

  function nodeWidth(node: GraphNode) {
    if (node.type === 'broker' || node.type === 'inspector') return 150;
    if (node.type === 'topic') return 190;
    if (isAuditQueue(node)) return 172;
    return 156;
  }

  function edgeLabel(edge: GraphEdge) {
    switch (edge.type) {
      case 'audit-copy':
        return 'audit copy';
      case 'observes':
        return 'observes';
      case 'consumes':
        return 'consumes';
      default:
        return '';
    }
  }
</script>

<main class="shell">
  <aside class="sidebar">
    <div>
      <p class="eyebrow">com.jmanzano.mqlens</p>
      <h1>MQ Lens</h1>
    </div>
    <nav>
      <button class:active={view === 'dashboard'} onclick={() => (view = 'dashboard')}>Dashboard</button>
      <button class:active={view === 'destinations'} onclick={() => (view = 'destinations')}>Destinations</button>
      <button class:active={view === 'messages'} onclick={() => (view = 'messages')}>Messages</button>
      <button class:active={view === 'traces'} onclick={() => (view = 'traces')}>Traces</button>
      <button class:active={view === 'topology'} onclick={() => (view = 'topology')}>Topology</button>
      <button class:active={view === 'settings'} onclick={() => (view = 'settings')}>Settings</button>
    </nav>
    <div class="status-block">
      <span class:ok={health?.brokerConnected} class="dot"></span>
      <div>
        <strong>{health?.brokerConnected ? 'Broker connected' : 'Broker offline'}</strong>
        <small>{health?.mode ?? 'hybrid'} mode</small>
      </div>
    </div>
  </aside>

  <section class="workspace">
    <header class="topbar">
      <div>
        <p class="eyebrow">ActiveMQ Classic inspector</p>
        <h2>{view}</h2>
      </div>
      <div class="actions">
        <button class="icon-button" aria-label="Refresh" title="Refresh" onclick={refresh}>↻</button>
        <label class="toggle"><input type="checkbox" bind:checked={livePaused} /> Pause live</label>
        <span class="event-status">{eventStatus}</span>
      </div>
    </header>

    {#if error}
      <div class="notice">{error}</div>
    {/if}

    {#if view === 'dashboard'}
      <section class="metrics">
        <article><span>Messages</span><strong>{messages.length}</strong></article>
        <article><span>Queues</span><strong>{queueCount}</strong></article>
        <article><span>Topics</span><strong>{topicCount}</strong></article>
        <article><span>Consumers</span><strong>{consumerCount}</strong></article>
        <article><span>Producers</span><strong>{producerCount}</strong></article>
      </section>
      <section class="split">
        <div class="panel">
          <h3>Broker</h3>
          <dl>
            <dt>Name</dt><dd>{broker?.brokerName || 'Unavailable'}</dd>
            <dt>Version</dt><dd>{broker?.brokerVersion || '-'}</dd>
            <dt>Uptime</dt><dd>{broker?.uptime || '-'}</dd>
            <dt>Memory</dt><dd>{broker?.memoryPercent ?? 0}%</dd>
            <dt>Store</dt><dd>{broker?.storePercent ?? 0}%</dd>
          </dl>
        </div>
        <div class="panel">
          <h3>Last captured</h3>
          {@render MessageList(messages, openMessage)}
        </div>
      </section>
    {:else if view === 'destinations'}
      {@render DestinationTable(destinations)}
    {:else if view === 'messages'}
      <section class="message-tools">
        <input placeholder="Search body" bind:value={query} onkeydown={(event) => event.key === 'Enter' && refresh()} />
        <select bind:value={destinationFilter} onchange={refresh} aria-label="Destination filter">
          <option value="">All destinations</option>
          {#each destinations as destination (`filter:${destination.type}:${destination.name}`)}
            <option value={destination.name}>{destination.name}</option>
          {/each}
        </select>
        <button onclick={refresh}>Search</button>
        {#if selected}<button onclick={downloadSelected}>Download JSON</button>{/if}
      </section>
      <section class="message-layout">
        {@render MessageList(messages, openMessage)}
        {@render MessageDetail(selected)}
      </section>
    {:else if view === 'traces'}
      <section class="message-tools">
        <input placeholder="Search correlationId" bind:value={traceCorrelationId} onkeydown={(event) => event.key === 'Enter' && loadTrace(traceCorrelationId)} />
        <button onclick={() => loadTrace(traceCorrelationId)}>Trace</button>
      </section>
      <section class="trace-layout">
        {#if traceMessages.length === 0}
          <div class="panel"><h3>No traces found</h3></div>
        {:else}
          <div class="timeline panel">
            <h3>Timeline: {traceCorrelationId}</h3>
            {#each traceMessages as tm (tm.id)}
              <div class="timeline-event" onclick={() => openMessage(tm.id)}>
                <strong>{tm.originalDestination}</strong> <span class="muted">{new Date(tm.capturedAt).toLocaleTimeString()}</span>
                <div>{tm.type || 'Event'}</div>
              </div>
            {/each}
          </div>
          {@render MessageDetail(selected)}
        {/if}
      </section>
    {:else if view === 'topology'}
      <section class="topology-note">
        <div>
          <strong>Live topology</strong>
          <span>Updated from Jolokia polling and ActiveMQ advisory events.</span>
        </div>
        <button class="help-button" aria-label="Explain topology updates">?</button>
        <div class="help-popover" role="tooltip">
          Advisory topics are broker metadata. ActiveMQ emits them when consumers, producers, queues or connections change. MQ Lens listens to them, so this graph can update while the broker is active.
        </div>
      </section>
      {@render TopologyGraph(topology)}
    {:else}
      <section class="panel">
        <h3>Runtime</h3>
        <p class="muted">Observe mode does not capture message bodies unless audit queues are configured.</p>
        <dl>
          <dt>Jolokia</dt><dd>{health?.jolokiaAvailable ? 'available' : 'unavailable'}</dd>
          <dt>STOMP</dt><dd>{health?.stompConnected ? 'connected' : 'disconnected'}</dd>
          <dt>Redaction</dt><dd>enabled by default via backend config</dd>
        </dl>
        <h3>Dev Send</h3>
        <div class="dev-send">
          <input bind:value={devDestination} aria-label="Dev destination" />
          <textarea bind:value={devBody} aria-label="Dev body"></textarea>
          <button onclick={sendDevMessage}>Send</button>
          {#if devStatus}<span>{devStatus}</span>{/if}
        </div>
      </section>
    {/if}
  </section>
</main>

{#snippet MessageList(messages: CapturedMessage[], openMessage: (id: string) => void)}
  <div class="table-wrap">
    <table>
      <thead><tr><th>Captured</th><th>Destination</th><th>Format</th><th>Size</th><th>Flags</th></tr></thead>
      <tbody>
        {#each messages as message (message.id)}
          <tr onclick={() => openMessage(message.id)}>
            <td>{new Date(message.capturedAt).toLocaleTimeString()}</td>
            <td>{message.originalDestination}</td>
            <td>{message.bodyFormat}</td>
            <td>{message.bodySize}</td>
            <td>{message.truncated ? 'truncated ' : ''}{message.redacted ? 'redacted' : ''}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/snippet}

{#snippet DestinationTable(destinations: DestinationSnapshot[])}
  <div class="table-wrap panel">
    <table>
      <thead><tr><th>Name</th><th>Type</th><th>Queue size</th><th>Enqueue</th><th>Dequeue</th><th>Consumers</th><th>Producers</th></tr></thead>
      <tbody>
        {#each destinations as destination (`${destination.type}:${destination.name}`)}
          <tr>
            <td>{destination.name}</td>
            <td>{destination.type}</td>
            <td>{destination.queueSize}</td>
            <td>{destination.enqueueCount}</td>
            <td>{destination.dequeueCount}</td>
            <td>{destination.consumerCount}</td>
            <td>{destination.producerCount}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/snippet}

{#snippet MessageDetail(message: CapturedMessage | null)}
  <div class="panel detail">
    {#if message}
      <h3>{message.originalDestination}</h3>
      <p class="muted">{message.messageId || message.id}</p>
      <div class="badges">
        <span>{message.bodyFormat}</span>
        {#if message.truncated}<span>truncated</span>{/if}
        {#if message.redacted}<span>redacted</span>{/if}
      </div>
      {#if message.correlationId}
        <button onclick={() => loadTrace(message.correlationId)}>View Trace</button>
      {/if}
      <h4>Body</h4>
      <pre>{message.bodyText || JSON.stringify(message.bodyBytes)}</pre>
      <h4>Headers</h4>
      <pre>{JSON.stringify(message.headers, null, 2)}</pre>
      <h4>Properties</h4>
      <pre>{JSON.stringify(message.properties, null, 2)}</pre>
    {:else}
      <h3>Select a message</h3>
      <p class="muted">Captured audit messages appear here with body, headers and properties.</p>
    {/if}
  </div>
{/snippet}

{#snippet TopologyGraph(topology: Topology)}
  <section class="panel graph-panel">
    <div class="cy-graph" bind:this={topologyElement} aria-label={`Topology graph with ${topology.nodes.length} nodes`}></div>
  </section>
{/snippet}
