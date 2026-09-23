/** Naming defaults shared by the single-table wizard and the task-group form. */

/**
 * The topic a table lands in when the operator does not name one. Mirrors what whole-database
 * Kafka groups do server-side (topic = source table name), with the characters Kafka rejects
 * folded to '_' and the length capped at Kafka's limit.
 */
export function defaultKafkaTopic(database: string, table: string) {
  return `${database || 'source'}_${table}`.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 249);
}

/** Same-name is the overwhelmingly common case for a relational target; Kafka gets a topic name. */
export function defaultTargetName(database: string, table: string, kafkaTarget: boolean) {
  return kafkaTarget ? defaultKafkaTopic(database, table) : table;
}
