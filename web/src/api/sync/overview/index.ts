import type { R } from '@/api/types';
import request from '@/api/request';
import type { SyncOverviewVO } from './types';

/** Dashboard counters computed over every row, not over the first page. */
export function getSyncOverview() {
  return request<R<SyncOverviewVO>>({
    url: '/sync/overview',
    method: 'get'
  });
}
