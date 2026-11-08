package io.github.firemq.protocol;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 * Encodes and decodes frames.
 *
 * <pre>
 * len: u32 | op: u8 | hlen: u32 | header (MessagePack map) | payload
 * </pre>
 *
 * {@code len} counts every byte after itself. On WebSocket the same frame is sent without
 * {@code len}.
 */
public final class FrameCodec {
    /** Bytes a TCP client sends before its first frame. */
    public static final byte[] MAGIC = "FMQ1".getBytes(StandardCharsets.US_ASCII);

    /** Largest frame accepted before {@code welcome}. */
    public static final int MAX_HELLO_FRAME = 65_536;

    /** Default largest frame after {@code welcome}. */
    public static final int DEFAULT_MAX_FRAME = 16 * 1024 * 1024;

    private static final byte[] EMPTY = new byte[0];

    private FrameCodec() {}

    /** Raised when a frame is well delimited but its header cannot be decoded. */
    public static final class MalformedHeaderException extends IOException {
        private final int op;

        MalformedHeaderException(int op, String message, Throwable cause) {
            super(message, cause);
            this.op = op;
        }

        public int op() {
            return op;
        }
    }

    /** Encodes a header map. An empty or null map encodes to zero bytes. */
    public static byte[] encodeHeader(Map<String, ?> header) {
        if (header == null || header.isEmpty()) {
            return EMPTY;
        }
        return MsgPack.encode(header);
    }

    /** Encodes a frame with its length prefix, as sent on TCP and TLS. */
    public static byte[] encode(int op, Map<String, ?> header, byte[] payload) {
        return encode(op, encodeHeader(header), payload);
    }

    /** Encodes a frame with its length prefix from an already encoded header. */
    public static byte[] encode(int op, byte[] header, byte[] payload) {
        if (payload == null) {
            payload = EMPTY;
        }
        int len = 1 + 4 + header.length + payload.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + len);
        buf.putInt(len);
        buf.put((byte) op);
        buf.putInt(header.length);
        buf.put(header);
        buf.put(payload);
        return buf.array();
    }

    /** Encodes a frame without the length prefix, as sent on WebSocket. */
    public static byte[] encodeWs(int op, Map<String, ?> header, byte[] payload) {
        byte[] full = encode(op, header, payload);
        byte[] out = new byte[full.length - 4];
        System.arraycopy(full, 4, out, 0, out.length);
        return out;
    }

    /** Size of the frame counted by the length prefix: op, header length, header and payload. */
    public static long frameLength(int headerLength, int payloadLength) {
        return 1L + 4L + headerLength + payloadLength;
    }

    /**
     * Decodes a frame without the length prefix (a WebSocket message, or the bytes after
     * {@code len} on TCP).
     */
    public static Frame decode(byte[] frame) throws IOException {
        if (frame.length < 5) {
            throw new IOException("malformed frame: shorter than 5 bytes");
        }
        ByteBuffer buf = ByteBuffer.wrap(frame);
        int op = buf.get() & 0xff;
        long hlen = buf.getInt() & 0xffffffffL;
        if (hlen > buf.remaining()) {
            throw new IOException("malformed frame: header length exceeds frame");
        }
        byte[] header = new byte[(int) hlen];
        buf.get(header);
        byte[] payload = new byte[buf.remaining()];
        buf.get(payload);
        return new Frame(op, decodeHeader(op, header), payload);
    }

    /** Decodes a length-prefixed frame from a byte array that holds exactly one frame. */
    public static Frame decodePrefixed(byte[] bytes) throws IOException {
        if (bytes.length < 4) {
            throw new IOException("malformed frame: shorter than its length prefix");
        }
        long len = ByteBuffer.wrap(bytes).getInt() & 0xffffffffL;
        if (len != bytes.length - 4L) {
            throw new IOException("length prefix " + len + " does not match " + (bytes.length - 4) + " bytes");
        }
        byte[] rest = new byte[bytes.length - 4];
        System.arraycopy(bytes, 4, rest, 0, rest.length);
        return decode(rest);
    }

    /**
     * Reads one length-prefixed frame from a stream.
     *
     * @param maxFrame the largest accepted value of the length prefix
     * @throws MalformedHeaderException when the frame was read completely but its header is not
     *     a MessagePack map; the stream stays aligned on the next frame
     * @throws IOException on I/O errors, end of stream, or frames that break the framing rules
     */
    public static Frame read(DataInputStream in, int maxFrame) throws IOException {
        long len = in.readInt() & 0xffffffffL;
        if (len > maxFrame) {
            throw new IOException("frame of " + len + " bytes exceeds the maximum of " + maxFrame + " bytes");
        }
        if (len < 5) {
            throw new IOException("malformed frame: shorter than 5 bytes");
        }
        int op = in.readUnsignedByte();
        long hlen = in.readInt() & 0xffffffffL;
        if (hlen > len - 5) {
            throw new IOException("malformed frame: header length exceeds frame");
        }
        byte[] header = new byte[(int) hlen];
        in.readFully(header);
        byte[] payload = new byte[(int) (len - 5 - hlen)];
        in.readFully(payload);
        return new Frame(op, decodeHeader(op, header), payload);
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> decodeHeader(int op, byte[] header) throws MalformedHeaderException {
        if (header.length == 0) {
            return Collections.emptyMap();
        }
        Object v;
        try {
            v = MsgPack.decode(header);
        } catch (IOException | RuntimeException e) {
            throw new MalformedHeaderException(op, "invalid header for " + Op.name(op) + ": " + e.getMessage(), e);
        }
        if (!(v instanceof Map)) {
            throw new MalformedHeaderException(op, "header for " + Op.name(op) + " is not a MessagePack map", null);
        }
        return (Map<String, Object>) v;
    }
}
