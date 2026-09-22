import type { ActionType } from '@ant-design/pro-components';
import { useCallback, type RefObject } from 'react';

export function useSearchReset(actionRef: RefObject<ActionType | undefined>, resetExtras?: () => void) {
  return useCallback(() => {
    resetExtras?.();
    setTimeout(() => actionRef.current?.reloadAndRest?.(), 0);
  }, [actionRef, resetExtras]);
}
