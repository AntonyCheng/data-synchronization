import { Button, Checkbox, Empty, Input, Space, Tag, theme, Tooltip, Typography } from 'antd';
import { useMemo, useState } from 'react';

export interface ColumnOption {
  name: string;
  typeName?: string;
}

export interface ColumnSelectorProps {
  columns: ColumnOption[];
  /** Sync-key columns: always selected and not unselectable, because excluding one breaks upsert. */
  lockedColumns?: string[];
  /** Supplied by Form.Item. */
  value?: string[];
  onChange?: (value: string[]) => void;
}

/**
 * Picks the columns a task synchronises. A plain flat Checkbox.Group is unusable past a handful
 * of columns - a wide table fills the whole dialog and there is no way to say "everything except
 * these two" - so this adds search, select-all / invert and a live count, and keeps the sync-key
 * columns pinned and disabled with the reason attached.
 */
export default function ColumnSelector({ columns, lockedColumns = [], value, onChange }: ColumnSelectorProps) {
  const { token } = theme.useToken();
  const [keyword, setKeyword] = useState('');

  const locked = useMemo(() => new Set(lockedColumns.filter(Boolean)), [lockedColumns]);
  const selected = useMemo(() => new Set(value || []), [value]);
  const visible = useMemo(() => {
    const needle = keyword.trim().toLowerCase();
    if (!needle) return columns;
    return columns.filter(
      column => column.name.toLowerCase().includes(needle) || (column.typeName || '').toLowerCase().includes(needle)
    );
  }, [columns, keyword]);

  /** Locked columns survive every bulk action; order follows the source table. */
  const commit = (next: Set<string>) => {
    locked.forEach(name => next.add(name));
    onChange?.(columns.map(column => column.name).filter(name => next.has(name)));
  };

  const toggle = (name: string, checked: boolean) => {
    const next = new Set(selected);
    if (checked) next.add(name);
    else next.delete(name);
    commit(next);
  };

  // Bulk actions apply to what the search currently shows, so "select all" after filtering by
  // "created_" means "add those", not "throw the rest away".
  const selectAllVisible = () => {
    const next = new Set(selected);
    visible.forEach(column => next.add(column.name));
    commit(next);
  };

  const clearVisible = () => {
    const next = new Set(selected);
    visible.forEach(column => next.delete(column.name));
    commit(next);
  };

  const invertVisible = () => {
    const next = new Set(selected);
    visible.forEach(column => (next.has(column.name) ? next.delete(column.name) : next.add(column.name)));
    commit(next);
  };

  return (
    <div
      style={{
        border: `1px solid ${token.colorBorder}`,
        borderRadius: token.borderRadius,
        padding: token.paddingSM
      }}
    >
      <Space wrap style={{ width: '100%', justifyContent: 'space-between', marginBottom: token.marginXS }}>
        <Space wrap size={4}>
          <Input.Search
            allowClear
            size="small"
            placeholder="搜索字段名或类型"
            aria-label="搜索字段"
            value={keyword}
            onChange={event => setKeyword(event.target.value)}
            style={{ width: 200 }}
          />
          <Button size="small" onClick={selectAllVisible}>
            全选
          </Button>
          <Button size="small" onClick={invertVisible}>
            反选
          </Button>
          <Button size="small" onClick={clearVisible}>
            清空
          </Button>
        </Space>
        <Tag color={selected.size ? 'processing' : 'default'}>
          已选 {selected.size} / 共 {columns.length}
          {keyword.trim() && ` · 筛出 ${visible.length}`}
        </Tag>
      </Space>
      <div
        style={{
          maxHeight: 216,
          overflowY: 'auto',
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
          gap: token.marginXXS
        }}
      >
        {visible.map(column => {
          const isLocked = locked.has(column.name);
          const checkbox = (
            <Checkbox
              checked={selected.has(column.name) || isLocked}
              disabled={isLocked}
              onChange={event => toggle(column.name, event.target.checked)}
            >
              {column.name}{' '}
              <Typography.Text type="secondary" style={{ fontSize: token.fontSizeSM }}>
                {column.typeName || '-'}
              </Typography.Text>
            </Checkbox>
          );
          return (
            <div key={column.name}>
              {isLocked ? <Tooltip title="同步键字段不能排除">{checkbox}</Tooltip> : checkbox}
            </div>
          );
        })}
        {visible.length === 0 && (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={`没有匹配「${keyword}」的字段`} />
        )}
      </div>
    </div>
  );
}
