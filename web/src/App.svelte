<script lang="ts">
  import cytoscape, { type Core, type ElementDefinition } from 'cytoscape';
  import { api } from './lib/api';
  import { connectEvents } from './lib/events';
  import type { BrokerSnapshot, CapturedMessage, DestinationSnapshot, Health, Topology } from './lib/types';

  type View = 'dashboard' | 'destinations' | 'messages' | 'topology' | 'settings';

  let view = $state<View>('dashboard');
  let health = $state<Health | null>(null);
  let broker = $state<BrokerSnapshot | null>(null);
  let destinations = $state<DestinationSnapshot[]>([]);
  let messages = $state<CapturedMessage[]>([]);
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
    if (view !== 'topology' || !topologyElement) {
      return;
    }
    topologyInstance?.destroy();
    topologyInstance = cytoscape({
      container: topologyElement,
      elements: topologyElements(topology),
      style: [
        {
          selector: 'node',
          style: {
            label: 'data(label)',
            'background-color': '#fbfcf8',
            'border-width': 1,
            'border-color': '#17201b',
            color: '#17201b',
            'font-size': 11,
            'text-valign': 'center',
            'text-halign': 'center',
            width: 118,
            height: 48,
            shape: 'round-rectangle'
          }
        },
        { selector: 'node[type = "broker"]', style: { 'background-color': '#d5e8d0', width: 138 } },
        { selector: 'node[type = "queue"]', style: { 'background-color': '#f1e7ba' } },
        { selector: 'node[type = "topic"]', style: { 'background-color': '#d8e7ef' } },
        { selector: 'node[type = "inspector"]', style: { 'background-color': '#17201b', color: '#f7f8f3' } },
        {
          selector: 'edge',
          style: {
            width: 2,
            'line-color': '#7d897e',
            'target-arrow-color': '#7d897e',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
            label: 'data(type)',
            'font-size': 9,
            color: '#667266',
            'text-background-color': '#eef1ec',
            'text-background-opacity': 1
          }
        },
        { selector: 'edge[type = "audit-copy"]', style: { 'line-style': 'dashed', 'line-color': '#c55d4b', 'target-arrow-color': '#c55d4b' } }
      ],
      layout: { name: 'breadthfirst', directed: true, padding: 28, spacingFactor: 1.35 }
    });
    return () => {
      topologyInstance?.destroy();
      topologyInstance = null;
    };
  });

  function topologyElements(topology: Topology): ElementDefinition[] {
    return [
      ...topology.nodes.map((node) => ({ data: { id: node.id, label: node.label, type: node.type } })),
      ...topology.edges.map((edge, index) => ({
        data: { id: `edge:${index}:${edge.source}:${edge.target}`, source: edge.source, target: edge.target, type: edge.type }
      }))
    ];
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
    {:else if view === 'topology'}
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
