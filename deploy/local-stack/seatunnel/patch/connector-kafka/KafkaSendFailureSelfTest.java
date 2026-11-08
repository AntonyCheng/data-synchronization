import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.sink.KafkaNoTransactionSender;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Self-test of the patched {@code KafkaNoTransactionSender}, run by build-patched-connector.ps1 before
 * the jar is published. No broker is needed: with nothing listening and {@code max.block.ms = 1} the
 * producer fails the send on its metadata wait - an ApiException handed to the send callback, the
 * path a RecordTooLargeException takes too - which the stock sender dropped without a trace.
 */
public class KafkaSendFailureSelfTest {

    private static int failures;

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:1");
        properties.put("max.block.ms", "1");
        properties.put("key.serializer", ByteArraySerializer.class.getName());
        properties.put("value.serializer", ByteArraySerializer.class.getName());
        final KafkaNoTransactionSender<byte[], byte[]> sender = new KafkaNoTransactionSender<>(properties);
        try {
            if (sender.snapshotState(1L).isEmpty()) {
                System.out.println("  ok    a checkpoint with nothing sent passes");
            } else {
                fail("a checkpoint with nothing sent returned state");
            }
            sender.send(record("lost"));
            expectFailure("the checkpoint after a failed send", new Runnable() {
                public void run() {
                    sender.snapshotState(2L);
                }
            });
            expectFailure("a send after a failed send", new Runnable() {
                public void run() {
                    sender.send(record("next"));
                }
            });
        } finally {
            expectFailure("closing after a failed send", new Runnable() {
                public void run() {
                    sender.close();
                }
            });
        }
        if (failures > 0) {
            System.out.println("KafkaSendFailureSelfTest FAILED: " + failures + " check(s)");
            System.exit(1);
        }
        System.out.println("KafkaSendFailureSelfTest passed");
    }

    private static ProducerRecord<byte[], byte[]> record(String value) {
        return new ProducerRecord<>("self-test", value.getBytes(StandardCharsets.UTF_8));
    }

    private static void expectFailure(String what, Runnable action) {
        try {
            action.run();
            fail(what + " did not report the lost record");
        } catch (RuntimeException expected) {
            System.out.println("  ok    " + what + " fails: " + expected.getMessage());
        }
    }

    private static void fail(String message) {
        failures++;
        System.out.println("  FAIL  " + message);
    }
}
