package io.github.firemq.protocol;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ValueType;

/**
 * Converts between MessagePack and plain Java values.
 *
 * <p>Decoding produces {@code null}, {@link Boolean}, {@link Long} (or {@link BigInteger} for
 * unsigned values above {@link Long#MAX_VALUE}), {@link Float}, {@link Double}, {@link String},
 * {@code byte[]}, {@link List} and {@link Map} with string keys. Encoding accepts those types plus
 * the other boxed integer types, {@link BigDecimal} (as a double), {@link Instant} (as seconds since
 * the epoch, like an AMQP timestamp), enums (by name), arrays and any {@link Collection}.
 */
public final class MsgPack {
    private MsgPack() {}

    /** Encodes a value. */
    public static byte[] encode(Object value) {
        try (MessageBufferPacker p = MessagePack.newDefaultBufferPacker()) {
            pack(p, value);
            return p.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("encoding to memory failed", e);
        }
    }

    /** Decodes exactly one value. */
    public static Object decode(byte[] bytes) throws IOException {
        try (MessageUnpacker u = MessagePack.newDefaultUnpacker(bytes)) {
            Object v = unpack(u);
            if (u.hasNext()) {
                throw new IOException("trailing bytes after MessagePack value");
            }
            return v;
        }
    }

    public static void pack(MessagePacker p, Object v) throws IOException {
        if (v == null) {
            p.packNil();
        } else if (v instanceof Boolean) {
            p.packBoolean((Boolean) v);
        } else if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            p.packLong(((Number) v).longValue());
        } else if (v instanceof BigInteger) {
            p.packBigInteger((BigInteger) v);
        } else if (v instanceof Double) {
            p.packDouble((Double) v);
        } else if (v instanceof Float) {
            p.packFloat((Float) v);
        } else if (v instanceof BigDecimal) {
            p.packDouble(((BigDecimal) v).doubleValue());
        } else if (v instanceof CharSequence || v instanceof Character) {
            p.packString(v.toString());
        } else if (v instanceof byte[]) {
            byte[] b = (byte[]) v;
            p.packBinaryHeader(b.length);
            p.writePayload(b);
        } else if (v instanceof Map) {
            Map<?, ?> m = (Map<?, ?>) v;
            p.packMapHeader(m.size());
            for (Map.Entry<?, ?> e : m.entrySet()) {
                p.packString(String.valueOf(e.getKey()));
                pack(p, e.getValue());
            }
        } else if (v instanceof Collection) {
            Collection<?> c = (Collection<?>) v;
            p.packArrayHeader(c.size());
            for (Object o : c) {
                pack(p, o);
            }
        } else if (v instanceof Object[]) {
            Object[] a = (Object[]) v;
            p.packArrayHeader(a.length);
            for (Object o : a) {
                pack(p, o);
            }
        } else if (v instanceof Instant) {
            p.packLong(((Instant) v).getEpochSecond());
        } else if (v instanceof Enum) {
            p.packString(((Enum<?>) v).name());
        } else {
            throw new IllegalArgumentException(
                    "cannot encode a " + v.getClass().getName() + " as MessagePack");
        }
    }

    public static Object unpack(MessageUnpacker u) throws IOException {
        MessageFormat f = u.getNextFormat();
        ValueType t = f.getValueType();
        switch (t) {
            case NIL:
                u.unpackNil();
                return null;
            case BOOLEAN:
                return u.unpackBoolean();
            case INTEGER:
                if (f == MessageFormat.UINT64) {
                    BigInteger big = u.unpackBigInteger();
                    return big.bitLength() < 64 ? (Object) big.longValue() : big;
                }
                return u.unpackLong();
            case FLOAT:
                if (f == MessageFormat.FLOAT32) {
                    return u.unpackFloat();
                }
                return u.unpackDouble();
            case STRING:
                return u.unpackString();
            case BINARY:
                return u.readPayload(u.unpackBinaryHeader());
            case ARRAY: {
                int n = u.unpackArrayHeader();
                List<Object> list = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    list.add(unpack(u));
                }
                return list;
            }
            case MAP: {
                int n = u.unpackMapHeader();
                Map<String, Object> map = new LinkedHashMap<>(Math.max(4, n * 2));
                for (int i = 0; i < n; i++) {
                    Object k = unpack(u);
                    map.put(k instanceof String ? (String) k : String.valueOf(k), unpack(u));
                }
                return map;
            }
            case EXTENSION:
                return u.readPayload(u.unpackExtensionTypeHeader().getLength());
            default:
                throw new IOException("unsupported MessagePack type " + t);
        }
    }
}
