package io.github.firemq;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Options for {@link Client#publish(String, String, byte[], PublishOptions)}. Messages are
 * persistent and confirmed by default.
 */
public final class PublishOptions {
    private boolean persistent = true;
    private boolean mandatory;
    private boolean confirm = true;
    private MessageProperties properties = new MessageProperties();
    private final Map<String, Object> headers = new LinkedHashMap<>();

    public boolean persistent() {
        return persistent;
    }

    /** Write the message to disk. Survives a restart when the queue is durable. Default true. */
    public PublishOptions persistent(boolean persistent) {
        this.persistent = persistent;
        return this;
    }

    public boolean mandatory() {
        return mandatory;
    }

    /** Fail with {@link UnroutableException} when no queue receives the message. */
    public PublishOptions mandatory(boolean mandatory) {
        this.mandatory = mandatory;
        return this;
    }

    public boolean confirm() {
        return confirm;
    }

    /**
     * Wait for the broker's confirm. Default true. When false, {@code publish} returns once the
     * frame is queued for sending, and nacks and returns are not reported.
     */
    public PublishOptions confirm(boolean confirm) {
        this.confirm = confirm;
        return this;
    }

    public MessageProperties properties() {
        return properties;
    }

    public PublishOptions properties(MessageProperties properties) {
        this.properties = Objects.requireNonNull(properties, "properties");
        return this;
    }

    public Map<String, Object> headers() {
        return headers;
    }

    /** Adds an application header. Values can be any type {@link io.github.firemq.protocol.MsgPack} encodes. */
    public PublishOptions header(String key, Object value) {
        headers.put(key, value);
        return this;
    }

    public PublishOptions headers(Map<String, ?> headers) {
        if (headers != null) {
            this.headers.putAll(headers);
        }
        return this;
    }
}
