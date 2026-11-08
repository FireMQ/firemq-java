package io.github.firemq.protocol;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * One protocol frame: an op code, a decoded MessagePack header and a raw payload.
 *
 * <p>The getters return the protocol's zero value for absent or mistyped fields, as the spec
 * requires: {@code 0}, {@code false}, the empty string or an empty map.
 */
public final class Frame {
    private static final byte[] EMPTY = new byte[0];

    private final int op;
    private final Map<String, Object> header;
    private final byte[] payload;

    public Frame(int op, Map<String, Object> header, byte[] payload) {
        this.op = op;
        this.header = header == null ? Collections.emptyMap() : header;
        this.payload = payload == null ? EMPTY : payload;
    }

    public int op() {
        return op;
    }

    public String name() {
        return Op.name(op);
    }

    public Map<String, Object> header() {
        return header;
    }

    public byte[] payload() {
        return payload;
    }

    public boolean has(String key) {
        return header.get(key) != null;
    }

    public long getLong(String key) {
        Object v = header.get(key);
        return v instanceof Number ? ((Number) v).longValue() : 0L;
    }

    public int getInt(String key) {
        return (int) getLong(key);
    }

    public boolean getBool(String key) {
        return Boolean.TRUE.equals(header.get(key));
    }

    public String getString(String key) {
        Object v = header.get(key);
        return v instanceof String ? (String) v : "";
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMap(String key) {
        Object v = header.get(key);
        return v instanceof Map ? (Map<String, Object>) v : Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    public List<Object> getList(String key) {
        Object v = header.get(key);
        return v instanceof List ? (List<Object>) v : Collections.emptyList();
    }

    @Override
    public String toString() {
        return "Frame{" + name() + " " + header + ", payload " + payload.length + " bytes}";
    }
}
