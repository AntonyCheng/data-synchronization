import type { FormInstance } from 'antd';
import type { NamePath } from 'antd/es/form/interface';
import { LoadingOutlined } from '@ant-design/icons';
import { Alert, Button, Form, message, Modal, Space, Steps, Typography } from 'antd';
import { useState } from 'react';
import type { SourceProbe } from '@/components/sync/useSourceProbe';

/**
 * The shell shared by the single-table wizard and the task-group wizard, so both entry points
 * look and behave the same: step header with the live probe banner, footer buttons, the source
 * probe verdict, the "discard what you typed?" guard and the jump back to a step that is still
 * missing something.
 */

export interface WizardStepsProps {
  steps: string[];
  current: number;
  /** What the probe is doing right now; shown as a spinner banner under the steps. */
  probePhase?: string;
}

export function WizardSteps({ steps, current, probePhase }: WizardStepsProps) {
  return (
    <>
      <Steps
        current={current}
        size="small"
        style={{ marginBottom: probePhase ? 12 : 24 }}
        items={steps.map(title => ({ title }))}
      />
      {probePhase && (
        <Alert type="info" showIcon icon={<LoadingOutlined />} title={probePhase} style={{ marginBottom: 16 }} />
      )}
    </>
  );
}

export interface WizardFooterProps {
  step: number;
  lastStep: number;
  /** Introspection still running: 下一步 turns into a disabled 正在探查. */
  busy: boolean;
  submitting: boolean;
  submitText: string;
  onCancel: () => void;
  onPrev: () => void;
  onNext: () => void;
  onSubmit: () => void;
}

export function WizardFooter({
  step,
  lastStep,
  busy,
  submitting,
  submitText,
  onCancel,
  onPrev,
  onNext,
  onSubmit
}: WizardFooterProps) {
  return (
    <Space>
      <Button onClick={onCancel}>取消</Button>
      {step > 0 && <Button onClick={onPrev}>上一步</Button>}
      {step < lastStep ? (
        <Button type="primary" loading={busy} disabled={busy} onClick={onNext}>
          {busy ? '正在探查' : '下一步'}
        </Button>
      ) : (
        <Button type="primary" loading={submitting} onClick={onSubmit}>
          {submitText}
        </Button>
      )}
    </Space>
  );
}

export interface SourceProbeResultProps {
  probe: SourceProbe;
  /** Appended to "已探查 N 个数据库，" when the account sees more than one database. */
  databaseNote: string;
}

/** The probe's verdict under the source picker: connection, binlog / CDC precheck, database. */
export function SourceProbeResult({ probe, databaseNote }: SourceProbeResultProps) {
  const { connectionTest, cdcPrecheck, sourceDatabase, sourceDatabases } = probe;
  return (
    <>
      {connectionTest && (
        <Alert
          type={connectionTest.success ? 'success' : 'error'}
          showIcon
          title={connectionTest.success ? `源端连接可用（${connectionTest.latencyMs} ms）` : '源端连接失败'}
          description={connectionTest.message}
          style={{ marginBottom: 12 }}
        />
      )}
      {cdcPrecheck && (
        <Alert
          type={cdcPrecheck.passed ? 'success' : 'warning'}
          showIcon
          title={cdcPrecheck.passed ? 'CDC 前置检查通过' : 'CDC 前置检查未通过'}
          description={cdcPrecheck.message}
          style={{ marginBottom: 12 }}
        />
      )}
      <Form.Item label="源数据库">
        <Typography.Text>{sourceDatabase || '选择源数据源后自动读取'}</Typography.Text>
        {sourceDatabases.length > 1 && (
          <Typography.Text type="secondary">
            {' '}
            已探查 {sourceDatabases.length} 个数据库，{databaseNote}
          </Typography.Text>
        )}
      </Form.Item>
    </>
  );
}

/**
 * Closing a half-filled wizard throws the work away, so ask first once anything was typed.
 *
 * Call `setDirty(true)` from onValuesChange, which fires for user edits but not for the
 * programmatic prefill of an edit form - isFieldsTouched() is unusable here because each wizard
 * step unmounts the previous step's controls.
 *
 * Spread `modalProps` into the ModalForm and do not wire onOpenChange to confirmClose: ModalForm
 * reports its own close after a successful submit too, which used to pop the discard prompt over
 * the freshly saved list, and the X button reported the close twice (two stacked prompts).
 */
export function useCloseGuard(close: () => void) {
  const [dirty, setDirty] = useState(false);

  const confirmClose = () => {
    if (!dirty) {
      close();
      return;
    }
    Modal.confirm({
      title: '放弃本次填写？',
      content: '关闭后已填写的内容不会保留。',
      okText: '放弃',
      okButtonProps: { danger: true },
      cancelText: '继续填写',
      onOk: close
    });
  };

  return {
    dirty,
    setDirty,
    confirmClose,
    modalProps: {
      destroyOnHidden: true,
      // A multi-step form is easy to lose: Esc and a stray mask click used to discard everything
      // typed so far, because destroyOnHidden tears the form down with it.
      mask: { closable: false },
      keyboard: false,
      onCancel: confirmClose
    }
  };
}

/** A required value that is still empty, and the wizard step that owns it. */
export interface MissingField {
  /** Index into the wizard's steps; -1 when no step owns the field. */
  step: number;
  field: NamePath;
  label: string;
}

/**
 * Sends the operator to the step that owns a missing field instead of asking them to walk back
 * through the wizard looking for it: switches step, names step and field in the toast, and
 * scrolls to the field once that step has mounted.
 */
export function jumpToMissing(
  form: Pick<FormInstance, 'scrollToField'>,
  steps: string[],
  goToStep: (step: number) => void,
  { step, field, label }: MissingField
) {
  if (step >= 0) goToStep(step);
  message.error(step >= 0 ? `第 ${step + 1} 步「${steps[step]}」还缺少必填项：${label}` : `还缺少必填项：${label}`);
  // The panel for that step has to mount before its field can be scrolled to.
  window.setTimeout(() => form.scrollToField(field, { behavior: 'smooth', block: 'center' }), 120);
}
