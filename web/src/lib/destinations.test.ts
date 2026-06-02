import { describe, it, expect } from 'vitest';
import { sortDestinations } from './destinations';
import type { DestinationSnapshot } from './types';

function dest(name: string, type: 'queue' | 'topic' = 'queue'): DestinationSnapshot {
  return {
    name,
    type,
    queueSize: 0,
    enqueueCount: 0,
    dequeueCount: 0,
    dispatchCount: 0,
    consumerCount: 0,
    producerCount: 0,
    expiredCount: 0,
  };
}

describe('sortDestinations', () => {
  it('sorts by type first (queue before topic)', () => {
    const input = [dest('Z', 'topic'), dest('A', 'queue')];
    const result = sortDestinations(input);
    expect(result[0].name).toBe('A');
    expect(result[1].name).toBe('Z');
  });

  it('sorts by name within the same type', () => {
    const input = [dest('ORDER.UPDATED'), dest('AUDIT.ALL'), dest('ORDER.CREATED')];
    const result = sortDestinations(input);
    expect(result.map((d) => d.name)).toEqual([
      'AUDIT.ALL',
      'ORDER.CREATED',
      'ORDER.UPDATED',
    ]);
  });

  it('returns a new array without mutating the original', () => {
    const input = [dest('B'), dest('A')];
    const result = sortDestinations(input);
    expect(result).not.toBe(input);
    expect(input[0].name).toBe('B');
  });

  it('handles empty array', () => {
    expect(sortDestinations([])).toEqual([]);
  });

  it('handles mixed types sorted by type then name', () => {
    const input = [
      dest('Z.TOPIC', 'topic'),
      dest('B.QUEUE', 'queue'),
      dest('A.TOPIC', 'topic'),
      dest('A.QUEUE', 'queue'),
    ];
    const result = sortDestinations(input);
    expect(result.map((d) => d.name)).toEqual([
      'A.QUEUE',
      'B.QUEUE',
      'A.TOPIC',
      'Z.TOPIC',
    ]);
  });
});
