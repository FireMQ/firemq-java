package io.github.firemq.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/** Decodes every frame in the spec's test vectors (spec/test-vectors/frames.json). */
class TestVectorsTest {
    private static final HexFormat HEX = HexFormat.of();

    record Vector(String name, int op, String opName, byte[] frame, Map<String, Object> header, byte[] payload) {}

    static List<Vector> vectors() throws IOException {
        try (InputStream in = TestVectorsTest.class.getResourceAsStream("/frames.json")) {
            JsonNode root = new ObjectMapper().readTree(in);
            List<Vector> out = new ArrayList<>();
            for (JsonNode v : root.get("vectors")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> header = (Map<String, Object>) toJava(v.get("header"));
                out.add(new Vector(v.get("name").asText(), v.get("op").asInt(), v.get("op_name").asText(),
                        HEX.parseHex(v.get("frame_hex").asText()), header, HEX.parseHex(v.get("payload_hex").asText())));
            }
            return out;
        }
    }

    /** JSON to the types MsgPack decodes to: integers as Long, objects as maps. */
    static Object toJava(JsonNode n) {
        if (n.isNull()) {
            return null;
        }
        if (n.isBoolean()) {
            return n.booleanValue();
        }
        if (n.isIntegralNumber()) {
            return n.longValue();
        }
        if (n.isNumber()) {
            return n.doubleValue();
        }
        if (n.isTextual()) {
            return n.textValue();
        }
        if (n.isArray()) {
            List<Object> l = new ArrayList<>();
            n.forEach(e -> l.add(toJava(e)));
            return l;
        }
        Map<String, Object> m = new LinkedHashMap<>();
        n.fields().forEachRemaining(e -> m.put(e.getKey(), toJava(e.getValue())));
        return m;
    }

    @TestFactory
    Stream<DynamicTest> everyVectorDecodes() throws IOException {
        return vectors().stream().map(v -> DynamicTest.dynamicTest(v.name(), () -> {
            Frame f = FrameCodec.decodePrefixed(v.frame());
            assertEquals(v.op(), f.op());
            assertEquals(v.opName(), f.name());
            assertEquals(v.header(), f.header());
            assertArrayEquals(v.payload(), f.payload());

            Frame streamed = FrameCodec.read(new DataInputStream(new ByteArrayInputStream(v.frame())), Integer.MAX_VALUE);
            assertEquals(v.header(), streamed.header());
            assertArrayEquals(v.payload(), streamed.payload());
        }));
    }

    @TestFactory
    Stream<DynamicTest> everyVectorRoundTrips() throws IOException {
        return vectors().stream().map(v -> DynamicTest.dynamicTest(v.name(), () -> {
            byte[] encoded = FrameCodec.encode(v.op(), v.header(), v.payload());
            Frame f = FrameCodec.decodePrefixed(encoded);
            assertEquals(v.op(), f.op());
            assertEquals(v.header(), f.header());
            assertArrayEquals(v.payload(), f.payload());

            byte[] ws = FrameCodec.encodeWs(v.op(), v.header(), v.payload());
            Frame fromWs = FrameCodec.decode(ws);
            assertEquals(v.header(), fromWs.header());
            assertArrayEquals(v.payload(), fromWs.payload());
        }));
    }

    @Test
    void vectorsCoverEveryOp() throws IOException {
        List<Vector> vs = vectors();
        for (int op : new int[] {Op.HELLO, Op.WELCOME, Op.PING, Op.PONG, Op.CLOSE, Op.OK, Op.ERROR, Op.FLOW,
                Op.DECLARE_EXCHANGE, Op.DELETE_EXCHANGE, Op.DECLARE_QUEUE, Op.DELETE_QUEUE, Op.PURGE_QUEUE,
                Op.BIND, Op.UNBIND, Op.PUBLISH, Op.CONFIRM, Op.RETURN, Op.SUBSCRIBE, Op.UNSUBSCRIBE, Op.CREDIT,
                Op.DELIVER, Op.CANCELLED, Op.ACK, Op.NACK, Op.GET, Op.GET_OK}) {
            assertTrue(vs.stream().anyMatch(v -> v.op() == op), "no vector for " + Op.name(op));
        }
    }

    @Test
    void framesSplitAcrossReadsDecodeInOrder() throws IOException {
        ByteArrayOutputStream all = new ByteArrayOutputStream();
        List<Vector> vs = vectors();
        for (Vector v : vs) {
            all.write(v.frame());
        }
        // One byte at a time, as a slow network would deliver it.
        InputStream trickle = new InputStream() {
            private final byte[] bytes = all.toByteArray();
            private int pos;

            @Override
            public int read() {
                return pos < bytes.length ? bytes[pos++] & 0xff : -1;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                if (pos >= bytes.length) {
                    return -1;
                }
                if (len == 0) {
                    return 0;
                }
                b[off] = bytes[pos++];
                return 1;
            }
        };
        DataInputStream in = new DataInputStream(trickle);
        for (Vector v : vs) {
            Frame f = FrameCodec.read(in, Integer.MAX_VALUE);
            assertEquals(v.op(), f.op(), v.name());
            assertEquals(v.header(), f.header(), v.name());
        }
        assertEquals(-1, in.read());
    }
}
