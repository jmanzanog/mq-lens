import type { DestinationSnapshot } from './types';

/**
 * Sort destinations deterministically by type then name.
 * Prevents row reordering in the UI when live data is refreshed.
 */
export function sortDestinations(items: DestinationSnapshot[]): DestinationSnapshot[] {
  return [...items].sort((a, b) => {
    const typeOrder = a.type.localeCompare(b.type);
    return typeOrder !== 0 ? typeOrder : a.name.localeCompare(b.name);
  });
}
