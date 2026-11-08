package io.github.firemq.examples;

import io.github.firemq.Message;
import io.github.firemq.Subscription;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Shared helpers for the examples. */
final class Examples {
    private Examples() {}

    /** The broker URL: FIREMQ_URL, or a local broker with the default guest user. */
    static String url() {
        String url = System.getenv("FIREMQ_URL");
        return url == null || url.isEmpty() ? "fire://guest:guest@localhost:5673" : url;
    }

    /** Takes {@code count} messages, acknowledging each, or fails after five seconds without one. */
    static List<String> take(Subscription sub, int count) {
        List<String> bodies = new ArrayList<>();
        while (bodies.size() < count) {
            Message m = sub.next(Duration.ofSeconds(5));
            if (m == null) {
                throw new IllegalStateException("expected " + count + " messages, got " + bodies);
            }
            bodies.add(m.text());
            m.ack();
        }
        return bodies;
    }
}
