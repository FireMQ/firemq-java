package io.github.firemq;

import io.github.firemq.protocol.Op;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Tracks credit for a subscription and grants it back in batches of half the window. */
final class Credit {
    private final Client client;
    private final int sub;
    private final int window;
    private final AtomicInteger released = new AtomicInteger();
    private volatile boolean active = true;

    Credit(Client client, int sub, int window) {
        this.client = client;
        this.sub = sub;
        this.window = window;
    }

    int window() {
        return window;
    }

    /** Stops granting credit, once the subscription has ended. */
    void stop() {
        active = false;
    }

    /** Records handled messages; grants credit once half the window has been handled. */
    void release(int n) {
        if (!active) {
            return;
        }
        int total = released.addAndGet(n);
        int batch = Math.max(1, window / 2);
        if (total >= batch) {
            int granted = released.getAndSet(0);
            if (granted > 0) {
                Map<String, Object> h = new LinkedHashMap<>();
                h.put("sub", sub);
                h.put("credit", granted);
                client.sendQuietly(Op.CREDIT, h);
            }
        }
    }
}
