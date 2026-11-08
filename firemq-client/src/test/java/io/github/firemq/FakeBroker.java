package io.github.firemq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.firemq.protocol.Frame;
import io.github.firemq.protocol.FrameCodec;
import io.github.firemq.protocol.Op;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** A scripted broker on a local port, for testing the client without a real FireMQ. */
final class FakeBroker implements AutoCloseable {
    private final ServerSocket server;
    private Socket socket;
    private DataInputStream in;
    private OutputStream out;
    Frame hello;

    FakeBroker() throws IOException {
        server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
    }

    String url() {
        return "fire://guest:guest@127.0.0.1:" + server.getLocalPort();
    }

    /** Accepts one connection and reads the magic bytes and {@code hello}. */
    void accept() throws IOException {
        socket = server.accept();
        socket.setSoTimeout(5000);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = socket.getOutputStream();
        byte[] magic = new byte[4];
        in.readFully(magic);
        assertArrayEquals(FrameCodec.MAGIC, magic);
        hello = FrameCodec.read(in, FrameCodec.MAX_HELLO_FRAME);
        assertEquals(Op.HELLO, hello.op());
    }

    void welcome(int heartbeat) throws IOException {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("ver", 1);
        h.put("server", Map.of("name", "firemq", "version", "test"));
        h.put("conn_id", "test-conn");
        h.put("heartbeat", heartbeat);
        h.put("max_frame", 1 << 20);
        h.put("caps", List.of("confirms", "credit", "mandatory"));
        send(Op.WELCOME, h);
    }

    /** Starts a client connection in the background and completes the handshake. */
    Client connect() throws Exception {
        return connect(0);
    }

    Client connect(int heartbeat) throws Exception {
        CompletableFuture<Client> c = CompletableFuture.supplyAsync(() -> FireMQ.connect(url()));
        accept();
        welcome(heartbeat);
        return c.get(5, TimeUnit.SECONDS);
    }

    void send(int op, Map<String, ?> header) throws IOException {
        send(op, header, null);
    }

    void send(int op, Map<String, ?> header, byte[] payload) throws IOException {
        out.write(FrameCodec.encode(op, header, payload));
        out.flush();
    }

    /** Reads the next frame, skipping pings. */
    Frame read() throws IOException {
        while (true) {
            Frame f = FrameCodec.read(in, Integer.MAX_VALUE);
            if (f.op() != Op.PING) {
                return f;
            }
        }
    }

    Frame readRaw() throws IOException {
        return FrameCodec.read(in, Integer.MAX_VALUE);
    }

    Frame expect(int op) throws IOException {
        Frame f = read();
        assertEquals(Op.name(op), f.name(), "unexpected frame " + f);
        return f;
    }

    void ok(Frame request) throws IOException {
        send(Op.OK, Map.of("rid", request.getLong("rid")));
    }

    static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            socket.close();
        }
        server.close();
    }
}
