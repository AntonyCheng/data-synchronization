import { Descriptions, Tag } from 'antd';
import type { TargetCompatibilityCheckItemVO, TargetCompatibilityVO } from '@/api/sync/task/types';

/**
 * Checks of a passed result that still did not pass: they never block a start (the Kafka record
 * size is one - a row only fails once it really is that large), so the operator has to see and
 * confirm them instead of reading a green "passed".
 */
export function compatibilityWarnings(result?: Pick<TargetCompatibilityVO, 'passed' | 'checks'>) {
  return result?.passed ? (result.checks || []).filter(check => !check.required && !check.passed) : [];
}

/** Alert type of a result: passed with open warnings is a warning, not a success. */
export function compatibilityAlertType(result: TargetCompatibilityVO): 'success' | 'warning' | 'error' {
  if (!result.passed) return 'error';
  return compatibilityWarnings(result).length ? 'warning' : 'success';
}

/** The result's own summary, plus how many warnings are left to confirm. */
export function compatibilityTitle(result: TargetCompatibilityVO) {
  const warnings = compatibilityWarnings(result);
  return warnings.length
    ? `${result.message}；另有 ${warnings.length} 项需要确认：${warnings.map(check => check.label).join('、')}`
    : result.message;
}

/** One line per warning, for a confirmation dialog. */
export function warningLines(warnings: TargetCompatibilityCheckItemVO[]) {
  return warnings.map(
    check =>
      `${check.label}：${check.actual ? `${check.actual}。` : ''}${check.message}${check.suggestion ? `。处理建议：${check.suggestion}` : ''}`
  );
}

/**
 * Every check of a target compatibility result, one per line: a blocking failure is red, a
 * warning amber ("需确认"), and a suggestion is only shown for a check that did not pass.
 */
export default function CompatibilityChecks({ result }: { result: TargetCompatibilityVO }) {
  return (
    <Descriptions size="small" column={1}>
      <Descriptions.Item label="源表">{result.sourceTable}</Descriptions.Item>
      <Descriptions.Item label="目标表">{result.targetTable}</Descriptions.Item>
      {result.checks?.map(item => (
        <Descriptions.Item key={item.code} label={item.label}>
          <Tag color={item.passed ? 'success' : item.required ? 'error' : 'warning'}>
            {item.passed ? '通过' : item.required ? '未通过' : '需确认'}
          </Tag>{' '}
          {item.actual || '-'} {item.message}
          {!item.passed && item.suggestion ? `；处理建议：${item.suggestion}` : ''}
        </Descriptions.Item>
      ))}
    </Descriptions>
  );
}
