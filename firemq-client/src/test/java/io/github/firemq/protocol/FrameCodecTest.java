package io.github.firemq.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FrameCodecTest {
    private static DataInputStream stream(byte[]... frames) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] f : frames) {
            out.write(f);
        }
        return new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    }

    @Test
    void emptyHeaderEncodesToZeroBytes() throws IOException {
        byte[] ping = FrameCodec.encode(Op.PING, Map.of(), null);
        assertArrayEquals(new byte[] {0, 0, 0, 5, 3, 0, 0, 0, 0}, ping);
        assertTrue(FrameCodec.decodePrefixed(ping).header().isEmpty());
    }

    @Test
    void oversizedFramesAreRejected() throws IOException {
        byte[] big = FrameCodec.encode(Op.PUBLISH, Map.of(), new byte[100]);
        IOException e = assertThrows(IOException.class, () -> FrameCodec.read(stream(big), 50));
        assertTrue(e.getMessage().contains("exceeds"), e.getMessage());
    }

    @Test
    void headerLongerThanFrameIsMalformed() {
        assertThrows(IOException.class, () -> FrameCodec.decode(new byte[] {0x20, 0, 0, 0, 9, 1}));
        byte[] prefixed = {0, 0, 0, 6, 0x20, 0, 0, 0, 9, 1};
        assertThrows(IOException.class, () -> FrameCodec.read(stream(prefixed), 1024));
    }

    @Test
    void omittedFieldsTakeZeroValues() throws IOException {
        Frame f = FrameCodec.decodePrefixed(FrameCodec.encode(Op.SUBSCRIBE, Map.of("queue", "jobs"), null));
        assertEquals("jobs", f.getString("queue"));
        assertEquals(0, f.getLong("credit"));
        assertFalse(f.getBool("no_ack"));
        assertEquals("", f.getString("tag"));
        assertTrue(f.getMap("args").isEmpty());
    }

    @Test
    void unknownKeysAreKept() throws IOException {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("tag", 5);
        h.put("future_field", List.of(1, 2));
        Frame f = FrameCodec.decodePrefixed(FrameCodec.encode(Op.ACK, h, null));
        assertEquals(5, f.getLong("tag"));
        assertFalse(f.getBool("multiple"));
    }

    @Test
    void malformedHeaderLeavesTheStreamAligned() throws IOException {
        byte[] notAMap = FrameCodec.encode(Op.OK, MsgPack.encode(List.of(1, 2)), new byte[0]);
        byte[] ok = FrameCodec.encode(Op.OK, Map.of("rid", 7), null);
        DataInputStream in = stream(notAMap, ok);
        FrameCodec.MalformedHeaderException e =
                assertThrows(FrameCodec.MalformedHeaderException.class, () -> FrameCodec.read(in, 1024));
        assertEquals(Op.OK, e.op());
        assertEquals(7, FrameCodec.read(in, 1024).getLong("rid"));
    }

    @Test
    void payloadIsNotMessagePack() throws IOException {
        byte[] body = "{\"id\":42}".getBytes(StandardCharsets.UTF_8);
        Frame f = FrameCodec.decodePrefixed(FrameCodec.encode(Op.PUBLISH, Map.of("seq", 1), body));
        assertArrayEquals(body, f.payload());
    }

    @Test
    void valuesRoundTrip() throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nil", null);
        m.put("t", true);
        m.put("f", false);
        m.put("small", 7L);
        m.put("negative", -7L);
        m.put("max", Long.MAX_VALUE);
        m.put("min", Long.MIN_VALUE);
        m.put("double", 1.5d);
        m.put("float", 2.5f);
        m.put("str", "héllo");
        m.put("list", List.of(1L, "two", List.of()));
        m.put("map", Map.of("k", "v"));
        Map<?, ?> back = (Map<?, ?>) MsgPack.decode(MsgPack.encode(m));
        assertEquals(m, back);

        byte[] bin = {0, (byte) 0xff, 1};
        assertArrayEquals(bin, (byte[]) MsgPack.decode(MsgPack.encode(bin)));

        BigInteger huge = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
        assertEquals(huge, MsgPack.decode(MsgPack.encode(huge)));
    }

    @Test
    void otherJavaTypesEncode() throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("int", 3);
        m.put("short", (short) 4);
        m.put("array", new Object[] {"a", 1});
        m.put("instant", java.time.Instant.ofEpochSecond(1767225600));
        Map<?, ?> back = (Map<?, ?>) MsgPack.decode(MsgPack.encode(m));
        assertEquals(3L, back.get("int"));
        assertEquals(4L, back.get("short"));
        assertEquals(Arrays.asList("a", 1L), back.get("array"));
        assertEquals(1767225600L, back.get("instant"));
        assertThrows(IllegalArgumentException.class, () -> MsgPack.encode(new Object()));
    }

    @Test
    void positiveIntegersUseUnsignedFormats() {
        // 200 must be a uint8 (0xcc), as in the close vector, not an int16.
        assertArrayEquals(new byte[] {(byte) 0xcc, (byte) 0xc8}, MsgPack.encode(200));
    }
}
