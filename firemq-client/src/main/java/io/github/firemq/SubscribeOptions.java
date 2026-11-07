package io.github.firemq;

import java.util.LinkedHashMap;
import java.util.Map;

/** Options for {@link Client#subscribe(String, SubscribeOptions)}. */
public final class SubscribeOptions {
    private int prefetch = 100;
    private boolean noAck;
    private boolean exclusive;
    private String tag = "";
    private final Map<String, Object> args = new LinkedHashMap<>();

    public int prefetch() {
        return prefetch;
    }

    /**
     * How many messages the broker may send before the client grants more credit. Credit is
     * topped up in batches of half this window as messages are acknowledged (or, with
     * {@code noAck}, as the application takes them). Default 100.
     */
    public SubscribeOptions prefetch(int prefetch) {
        if (prefetch < 1) {
            throw new IllegalArgumentException("prefetch must be >= 1");
        }
        this.prefetch = prefetch;
        return this;
    }

    public boolean noAck() {
        return noAck;
    }

    /**
     * Treat messages as acknowledged when sent. Faster, but messages are lost if the consumer
     * crashes.
     */
    public SubscribeOptions noAck(boolean noAck) {
        this.noAck = noAck;
        return this;
    }

    public boolean exclusive() {
        return exclusive;
    }

    /** Fail with 405 if the queue has other consumers, and block new ones. */
    public SubscribeOptions exclusive(boolean exclusive) {
        this.exclusive = exclusive;
        return this;
    }

    public String tag() {
        return tag;
    }

    /** Consumer tag shown in the management UI. Defaults to {@code fire-<id>}. */
    public SubscribeOptions tag(String tag) {
        this.tag = tag == null ? "" : tag;
        return this;
    }

    public Map<String, Object> args() {
        return args;
    }

    /** Adds a consumer argument. */
    public SubscribeOptions arg(String key, Object value) {
        args.put(key, value);
        return this;
    }
}
