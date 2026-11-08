/**
 * The Kafka bridge's stop record as the task and task-group pages show it
 * (docs/kafka-event-formats.md §10: the bridge holds at an event it cannot publish).
 */

/** Prefix of every reason the bridge records for a stop (KafkaTaskBridgeService.STOPPED_PREFIX). */
export const BRIDGE_STOPPED_PREFIX = 'Kafka 桥接已暂停发布：';

/**
 * A RUNNING Kafka task or table whose bridge stopped publishing: its engine job is healthy and keeps
 * writing the raw topic, but nothing reaches the output topic until the cause is fixed.
 */
export function publishPaused(owner?: { status?: string; kafkaBridgeError?: string }) {
  return owner?.status === 'RUNNING' && Boolean(owner.kafkaBridgeError);
}

/** The stop reason without its prefix, for a place whose title already says publishing stopped. */
export function bridgeStopReason(error?: string) {
  return error?.startsWith(BRIDGE_STOPPED_PREFIX) ? error.slice(BRIDGE_STOPPED_PREFIX.length) : error || '';
}
