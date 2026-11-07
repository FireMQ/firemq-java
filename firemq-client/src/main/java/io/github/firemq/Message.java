package io.github.firemq;

import io.github.firemq.protocol.Op;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** A message from a subscription or from {@link Client#get(String, boolean)}. */
public final class Message {
    private final byte[] body;
    private final String exchange;
    private final String routingKey;
    private final MessageProperties properties;
    private final Map<String, Object> headers;
    private final boolean redelivered;
    private final long deliveryTag;
    private final Client client;
    private final Credit credit;
    private final AtomicBoolean settled = new AtomicBoolean();

    Message(byte[] body, String exchange, String routingKey, MessageProperties properties,
            Map<String, Object> headers, boolean redelivered, long deliveryTag, Client client, Credit credit) {
        this.body = body;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.properties = properties;
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        this.redelivered = redelivered;
        this.deliveryTag = deliveryTag;
        this.client = client;
        this.credit = credit;
    }

    /** The body. The array is not copied. */
    public byte[] body() {
        return body;
    }

    /** The body decoded as UTF-8. */
    public String text() {
        return new String(body, StandardCharsets.UTF_8);
    }

    /** The exchange the message was published to; empty for the default exchange. */
    public String exchange() {
        return exchange;
    }

    /** The routing key the message was published with. */
    public String routingKey() {
        return routingKey;
    }

    public MessageProperties properties() {
        return properties;
    }

    /**
     * Application headers. Integers decode as {@link Long}, floats as {@link Double} or
     * {@link Float}, binary as {@code byte[]}, arrays as {@link java.util.List} and maps as
     * {@link Map}.
     */
    public Map<String, Object> headers() {
        return headers;
    }

    /** True when the message was delivered before and not acknowledged. */
    public boolean redelivered() {
        return redelivered;
    }

    /** The delivery tag, unique on the connection. */
    public long deliveryTag() {
        return deliveryTag;
    }

    /**
     * Acknowledges the message. Does nothing for {@code noAck} messages or when the message was
     * already acknowledged or rejected.
     *
     * @throws ConnectionClosedException when the connection is closed
     */
    public void ack() {
        settle(Op.ACK, false);
    }

    /**
     * Rejects the message. With {@code requeue} it is delivered again; otherwise it is
     * dead-lettered when the queue has a dead-letter exchange, and dropped if not. Does nothing for
     * {@code noAck} messages or when the message was already settled.
     *
     * @throws ConnectionClosedException when the connection is closed
     */
    public void nack(boolean requeue) {
        settle(Op.NACK, requeue);
    }

    private void settle(int op, boolean requeue) {
        if (client == null || !settled.compareAndSet(false, true)) {
            return;
        }
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("tag", deliveryTag);
        if (op == Op.NACK && requeue) {
            h.put("requeue", true);
        }
        client.send(op, h, null);
        if (credit != null) {
            credit.release(1);
        }
    }

    /** True when {@link #ack()} or {@link #nack(boolean)} was called, or no acknowledgement is needed. */
    public boolean settled() {
        return client == null || settled.get();
    }

    @Override
    public String toString() {
        return "Message{exchange='" + exchange + "', routingKey='" + routingKey + "', deliveryTag=" + deliveryTag
                + ", redelivered=" + redelivered + ", body=" + body.length + " bytes}";
    }
}
