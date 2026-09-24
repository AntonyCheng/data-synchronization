package org.dromara.sync.e2e;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * LIFO undo log. Steps are registered as soon as the thing they undo exists (or is about to),
 * so a scenario that dies half-way still unwinds: platform objects first (registered last),
 * then target tables / topics, then the source fixture. A failing step never stops the rest.
 */
final class Cleanup {

    @FunctionalInterface
    interface Step {
        void run() throws Exception;
    }

    private record Entry(String description, Step step) {
    }

    private final Deque<Entry> entries = new ArrayDeque<>();

    synchronized void add(String description, Step step) {
        entries.push(new Entry(description, step));
    }

    /** Runs and forgets every step; returns one line per failed step. */
    synchronized List<String> runAll() {
        List<String> failures = new ArrayList<>();
        while (!entries.isEmpty()) {
            Entry entry = entries.pop();
            try {
                entry.step().run();
            } catch (Throwable ex) {
                failures.add(entry.description() + ": " + ex.getMessage());
            }
        }
        return failures;
    }
}
