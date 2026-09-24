package org.dromara.sync.e2e;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Polling with failure messages that say what was last seen. The engine is asynchronous
 * (checkpoint-driven sink flushes, savepoints, cold job start), so every "eventually" in the
 * suite goes through here instead of fixed sleeps.
 */
final class Await {

    static final Duration POLL_INTERVAL = Duration.ofMillis(1500);

    private Await() {
    }

    /**
     * Polls {@code probe} until {@code done} holds and returns that value. A probe that throws
     * counts as "not yet" (e.g. the target table does not exist before the first flush).
     * {@code abort} ends the wait early with a failure (a terminal state that can no longer turn
     * into the expected one). Distinct observations are kept as a trail for the message.
     */
    static <T> T until(String what, Duration timeout, Supplier<T> probe, Predicate<T> done,
                       Predicate<T> abort, Function<T, String> describe) {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<String> trail = new ArrayList<>();
        String last = "<nothing observed>";
        while (true) {
            T value = null;
            boolean observed = false;
            try {
                value = probe.get();
                observed = true;
                last = describe.apply(value);
            } catch (RuntimeException | AssertionError ex) {
                last = "probe failed: " + ex.getMessage();
            }
            note(trail, last);
            if (observed && done.test(value)) return value;
            if (observed && abort != null && abort.test(value)) {
                throw new AssertionError("Gave up waiting for " + what + ": reached a state it cannot leave on its own.\n"
                    + "  last observed: " + last + "\n  trail: " + trail);
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out after " + timeout.toSeconds() + "s waiting for " + what + ".\n"
                    + "  last observed: " + last + "\n  trail: " + trail);
            }
            PlatformClient.sleep(POLL_INTERVAL);
        }
    }

    static <T> T until(String what, Duration timeout, Supplier<T> probe, Predicate<T> done) {
        return until(what, timeout, probe, done, null, Objects::toString);
    }

    /**
     * Asserts {@code holds} stays true for the whole {@code period} (e.g. "the row written while
     * paused must NOT reach the target"). Fails on the first violation.
     */
    static <T> void holdsFor(String what, Duration period, Supplier<T> probe, Predicate<T> holds) {
        long deadline = System.nanoTime() + period.toNanos();
        while (true) {
            T value = probe.get();
            if (!holds.test(value)) {
                throw new AssertionError("Expected " + what + " for " + period.toSeconds() + "s, but observed: " + value);
            }
            if (System.nanoTime() > deadline) return;
            PlatformClient.sleep(POLL_INTERVAL);
        }
    }

    private static void note(List<String> trail, String observation) {
        String compact = observation.length() > 240 ? observation.substring(0, 240) + "..." : observation;
        if (trail.isEmpty() || !trail.get(trail.size() - 1).equals(compact)) trail.add(compact);
        if (trail.size() > 12) trail.remove(0);
    }
}
