package io.github.firemq;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Message properties, mirroring AMQP's basic properties. Unset properties are {@code null}. */
public final class MessageProperties {
    private String contentType;
    private String contentEncoding;
    private String correlationId;
    private String replyTo;
    private Long expiration;
    private String messageId;
    private Long timestamp;
    private String type;
    private String userId;
    private String appId;
    private Integer priority;

    public String contentType() {
        return contentType;
    }

    public MessageProperties contentType(String v) {
        this.contentType = v;
        return this;
    }

    public String contentEncoding() {
        return contentEncoding;
    }

    public MessageProperties contentEncoding(String v) {
        this.contentEncoding = v;
        return this;
    }

    public String correlationId() {
        return correlationId;
    }

    public MessageProperties correlationId(String v) {
        this.correlationId = v;
        return this;
    }

    public String replyTo() {
        return replyTo;
    }

    public MessageProperties replyTo(String v) {
        this.replyTo = v;
        return this;
    }

    /** Time to live in milliseconds. */
    public Long expiration() {
        return expiration;
    }

    public MessageProperties expiration(Long millis) {
        this.expiration = millis;
        return this;
    }

    public String messageId() {
        return messageId;
    }

    public MessageProperties messageId(String v) {
        this.messageId = v;
        return this;
    }

    /** Seconds since the Unix epoch. */
    public Long timestamp() {
        return timestamp;
    }

    public MessageProperties timestamp(Long seconds) {
        this.timestamp = seconds;
        return this;
    }

    public String type() {
        return type;
    }

    public MessageProperties type(String v) {
        this.type = v;
        return this;
    }

    /** When set, it must equal the connection's user. */
    public String userId() {
        return userId;
    }

    public MessageProperties userId(String v) {
        this.userId = v;
        return this;
    }

    public String appId() {
        return appId;
    }

    public MessageProperties appId(String v) {
        this.appId = v;
        return this;
    }

    /** 0 to 255. */
    public Integer priority() {
        return priority;
    }

    public MessageProperties priority(Integer v) {
        if (v != null && (v < 0 || v > 255)) {
            throw new IllegalArgumentException("priority must be between 0 and 255");
        }
        this.priority = v;
        return this;
    }

    public boolean isEmpty() {
        return toMap().isEmpty();
    }

    /** The properties as a protocol {@code props} map, with only the set keys. */
    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        put(m, "content_type", contentType);
        put(m, "content_encoding", contentEncoding);
        put(m, "correlation_id", correlationId);
        put(m, "reply_to", replyTo);
        put(m, "expiration", expiration);
        put(m, "message_id", messageId);
        put(m, "timestamp", timestamp);
        put(m, "type", type);
        put(m, "user_id", userId);
        put(m, "app_id", appId);
        put(m, "priority", priority);
        return m;
    }

    /** Reads a protocol {@code props} map. Unknown keys and mistyped values are ignored. */
    public static MessageProperties fromMap(Map<String, ?> m) {
        MessageProperties p = new MessageProperties();
        if (m == null) {
            return p;
        }
        p.contentType = str(m.get("content_type"));
        p.contentEncoding = str(m.get("content_encoding"));
        p.correlationId = str(m.get("correlation_id"));
        p.replyTo = str(m.get("reply_to"));
        p.expiration = num(m.get("expiration"));
        p.messageId = str(m.get("message_id"));
        p.timestamp = num(m.get("timestamp"));
        p.type = str(m.get("type"));
        p.userId = str(m.get("user_id"));
        p.appId = str(m.get("app_id"));
        Long prio = num(m.get("priority"));
        p.priority = prio == null ? null : (int) (long) prio;
        return p;
    }

    private static void put(Map<String, Object> m, String k, Object v) {
        if (v != null) {
            m.put(k, v);
        }
    }

    private static String str(Object v) {
        return v instanceof String ? (String) v : null;
    }

    private static Long num(Object v) {
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        if (v instanceof String) {
            try {
                return Long.parseLong((String) v);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof MessageProperties && toMap().equals(((MessageProperties) o).toMap());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(toMap());
    }

    @Override
    public String toString() {
        return "MessageProperties" + toMap();
    }
}
