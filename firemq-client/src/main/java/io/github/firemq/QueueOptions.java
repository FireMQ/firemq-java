package io.github.firemq;

import java.util.LinkedHashMap;
import java.util.Map;

/** Options for {@link Client#declareQueue(String, QueueOptions)}. Queues are durable by default. */
public final class QueueOptions {
    private boolean durable = true;
    private boolean exclusive;
    private boolean autoDelete;
    private boolean passive;
    private final Map<String, Object> args = new LinkedHashMap<>();

    /** A durable queue. */
    public QueueOptions() {}

    /** A queue that lives only as long as this connection: not durable, exclusive and auto-delete. */
    public static QueueOptions temporary() {
        return new QueueOptions().durable(false).exclusive(true).autoDelete(true);
    }

    public boolean durable() {
        return durable;
    }

    /** Survives a broker restart. Default true. */
    public QueueOptions durable(boolean durable) {
        this.durable = durable;
        return this;
    }

    public boolean exclusive() {
        return exclusive;
    }

    /** Used only by this connection and deleted when it closes. */
    public QueueOptions exclusive(boolean exclusive) {
        this.exclusive = exclusive;
        return this;
    }

    public boolean autoDelete() {
        return autoDelete;
    }

    /** Deleted when its last subscription is removed. */
    public QueueOptions autoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
        return this;
    }

    public boolean passive() {
        return passive;
    }

    /** Only check that the queue exists; fails with 404 when it does not. */
    public QueueOptions passive(boolean passive) {
        this.passive = passive;
        return this;
    }

    public Map<String, Object> args() {
        return args;
    }

    /** Adds a queue argument with RabbitMQ's name, for example {@code x-message-ttl}. */
    public QueueOptions arg(String key, Object value) {
        args.put(key, value);
        return this;
    }

    public QueueOptions args(Map<String, ?> args) {
        if (args != null) {
            this.args.putAll(args);
        }
        return this;
    }
}
