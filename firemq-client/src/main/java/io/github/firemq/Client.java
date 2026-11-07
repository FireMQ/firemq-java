package io.github.firemq;

import io.github.firemq.protocol.Frame;
import io.github.firemq.protocol.FrameCodec;
import io.github.firemq.protocol.Op;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A connection to FireMQ. Thread-safe: share one client between threads.
 *
 * <pre>{@code
 * try (Client client = FireMQ.connect("fire://guest:guest@localhost:5673")) {
 *     client.declareQueue("jobs", new QueueOptions());
 *     client.publish("", "jobs", "hello");          // returns when the broker confirms
 *     try (Subscription jobs = client.subscribe("jobs", new SubscribeOptions())) {
 *         for (Message msg : jobs) {
 *             System.out.println(msg.text());
 *             msg.ack();
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p>Defaults favour safety: queues and exchanges are durable, messages are persistent, and
 * {@code publish} waits for the broker's confirm.
 *
 * <p>Blocking methods throw {@link FireMQException} subclasses. The {@code *Async} methods return
 * futures that complete on a shared callback pool, never on the connection's reader thread, so
 * blocking in their callbacks is safe.
 */
public final class Client implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger("io.github.firemq");
    private static final byte[] EMPTY = new byte[0];
    private static final byte[] STOP = new byte[0];
    private static final byte[] PING = FrameCodec.encode(Op.PING, EMPTY, EMPTY);
    private static final AtomicInteger THREADS = new AtomicInteger();

    /**
     * Runs completions of the futures returned by the {@code *Async} methods. A fork-join pool
     * adds threads when callbacks block in {@code get()} or {@code join()}, so blocking callbacks
     * cannot starve it.
     */
    private static final ForkJoinPool CALLBACKS = new ForkJoinPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            pool -> {
                ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                t.setName("firemq-callback-" + t.getPoolIndex());
                t.setDaemon(true);
                return t;
            },
            null,
            true);

    private final ConnectOptions options;
    private final Socket socket;
    private final DataInputStream in;
    private final OutputStream out;
    private final ServerInfo server;
    private final int maxFrame;
    private final int heartbeat;

    private final LinkedBlockingQueue<byte[]> outbox = new LinkedBlockingQueue<>();
    private final AtomicInteger nextRid = new AtomicInteger(1);
    private final AtomicInteger nextSub = new AtomicInteger(1);
    private final Object publishLock = new Object();
    private long nextSeq = 1; // guarded by publishLock

    private final Object lock = new Object();
    private final Map<Integer, CompletableFuture<Frame>> requests = new HashMap<>(); // guarded by lock
    private final TreeMap<Long, CompletableFuture<Void>> confirms = new TreeMap<>(); // guarded by lock
    private final Map<Long, String[]> returned = new HashMap<>(); // guarded by lock
    private final ConcurrentHashMap<Integer, Subscription> subs = new ConcurrentHashMap<>();
    private volatile FireMQException closed;
    private volatile boolean closing;

    private final Object flowMonitor = new Object();
    private volatile boolean paused;

    private volatile long lastRecv = System.nanoTime();
    private volatile long lastSend = System.nanoTime();
    private final CountDownLatch closedLatch = new CountDownLatch(1);
    private final Thread reader;
    private final Thread writer;

    private Client(ConnectOptions options, Socket socket, DataInputStream in, OutputStream out, ServerInfo server) {
        this.options = options;
        this.socket = socket;
        this.in = in;
        this.out = out;
        this.server = server;
        this.maxFrame = Math.max(server.maxFrame(), FrameCodec.MAX_HELLO_FRAME);
        this.heartbeat = server.heartbeat();
        int n = THREADS.incrementAndGet();
        this.reader = new Thread(this::readLoop, "firemq-reader-" + n);
        this.writer = new Thread(this::writeLoop, "firemq-writer-" + n);
        reader.setDaemon(true);
        writer.setDaemon(true);
        reader.start();
        writer.start();
    }

    /** Connects with a URL such as {@code fire://user:pass@host:5673/vhost}. */
    public static Client connect(String url) {
        return connect(ConnectOptions.fromUrl(url));
    }

    /** Connects and performs the handshake. */
    public static Client connect(ConnectOptions options) {
        ConnectUrl url = options.url();
        if (url.transport() == ConnectUrl.Transport.WS || url.transport() == ConnectUrl.Transport.WSS) {
            throw new FireMQException("the Java client connects with fire:// or fires://; WebSocket is for browsers");
        }
        int timeout = (int) Math.min(Integer.MAX_VALUE, Math.max(1, options.connectTimeout().toMillis()));
        Socket socket = new Socket();
        try {
            try {
                socket.connect(new InetSocketAddress(url.host(), url.port()), timeout);
            } catch (SocketTimeoutException e) {
                throw new FireMQException("connecting to " + url.authority() + " timed out", e);
            } catch (IOException e) {
                throw new FireMQException("cannot connect to " + url.authority() + ": " + e.getMessage(), e);
            }
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            if (url.transport() == ConnectUrl.Transport.TLS) {
                socket = Tls.wrap(socket, options, timeout);
            }
            socket.setSoTimeout(timeout);
            OutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
            out.write(FrameCodec.MAGIC);
            out.write(FrameCodec.encode(Op.HELLO, hello(options), EMPTY));
            out.flush();
            Frame f;
            try {
                f = FrameCodec.read(in, FrameCodec.MAX_HELLO_FRAME);
            } catch (SocketTimeoutException e) {
                throw new FireMQException("handshake with " + url.authority() + " timed out", e);
            } catch (EOFException e) {
                throw new ConnectionClosedException("the broker closed the connection during the handshake", e);
            }
            if (f.op() == Op.ERROR) {
                throw new ServerException(f.getInt("code"), f.getString("msg"));
            }
            if (f.op() != Op.WELCOME) {
                throw new FireMQException("protocol error: expected welcome, got " + f.name());
            }
            socket.setSoTimeout(0);
            return new Client(options, socket, in, out, serverInfo(f));
        } catch (FireMQException e) {
            closeQuietly(socket);
            throw e;
        } catch (IOException e) {
            closeQuietly(socket);
            throw new FireMQException("handshake with " + url.authority() + " failed: " + e.getMessage(), e);
        } catch (RuntimeException e) {
            closeQuietly(socket);
            throw e;
        }
    }

    private static Map<String, Object> hello(ConnectOptions o) {
        ConnectUrl url = o.url();
        Map<String, Object> auth = new LinkedHashMap<>();
        auth.put("mech", "plain");
        auth.put("user", url.user());
        auth.put("pass", url.password());
        Map<String, Object> client = new LinkedHashMap<>();
        client.put("name", "firemq-java");
        client.put("version", FireMQ.VERSION);
        client.put("lang", "java");
        if (!o.connectionName().isEmpty()) {
            client.put("connection_name", o.connectionName());
        }
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("ver", 1);
        h.put("vhost", url.vhost());
        h.put("auth", auth);
        h.put("heartbeat", o.heartbeat());
        h.put("client", client);
        h.put("caps", List.of("confirms", "credit"));
        return h;
    }

    private static ServerInfo serverInfo(Frame f) {
        Map<String, Object> s = f.getMap("server");
        List<String> caps = new ArrayList<>();
        for (Object c : f.getList("caps")) {
            if (c instanceof String) {
                caps.add((String) c);
            }
        }
        long mf = f.getLong("max_frame");
        int maxFrame = mf <= 0 ? FrameCodec.DEFAULT_MAX_FRAME : (int) Math.min(Integer.MAX_VALUE, mf);
        return new ServerInfo(
                s.get("name") instanceof String ? (String) s.get("name") : "",
                s.get("version") instanceof String ? (String) s.get("version") : "",
                f.getString("conn_id"),
                f.getInt("heartbeat"),
                maxFrame,
                Collections.unmodifiableList(caps));
    }

    /** Server name, version and limits from the handshake. */
    public ServerInfo server() {
        return server;
    }

    /** The options this client connected with. */
    public ConnectOptions options() {
        return options;
    }

    /** True once the connection is closed, by either side or by a network failure. */
    public boolean isClosed() {
        return closed != null;
    }

    /** True while the broker has paused publishers because of a memory or disk alarm. */
    public boolean isPaused() {
        return paused;
    }

    // ---------------------------------------------------------------- topology

    /** Declares a durable queue. An empty name asks the broker to generate one. */
    public QueueInfo declareQueue(String name) {
        return declareQueue(name, new QueueOptions());
    }

    /** Declares a queue. An empty name asks the broker to generate one. */
    public QueueInfo declareQueue(String name, QueueOptions options) {
        return await(declareQueueInternal(name, options));
    }

    public CompletableFuture<QueueInfo> declareQueueAsync(String name, QueueOptions options) {
        return handOff(declareQueueInternal(name, options));
    }

    private CompletableFuture<QueueInfo> declareQueueInternal(String name, QueueOptions o) {
        String n = name == null ? "" : name;
        Map<String, Object> h = new LinkedHashMap<>();
        putIf(h, "name", n);
        putIf(h, "durable", o.durable());
        putIf(h, "exclusive", o.exclusive());
        putIf(h, "auto_delete", o.autoDelete());
        putIf(h, "passive", o.passive());
        putIf(h, "args", o.args());
        return callAsync(Op.DECLARE_QUEUE, h).thenApply(f -> new QueueInfo(
                f.has("name") ? f.getString("name") : n, f.getLong("messages"), f.getLong("consumers")));
    }

    /** Deletes a queue and returns how many messages it held. */
    public long deleteQueue(String name) {
        return deleteQueue(name, false, false);
    }

    /**
     * Deletes a queue and returns how many messages it held. Fails with 406 when
     * {@code ifUnused} is set and the queue has subscriptions, or {@code ifEmpty} is set and it
     * holds messages.
     */
    public long deleteQueue(String name, boolean ifUnused, boolean ifEmpty) {
        return await(deleteQueueInternal(name, ifUnused, ifEmpty));
    }

    public CompletableFuture<Long> deleteQueueAsync(String name, boolean ifUnused, boolean ifEmpty) {
        return handOff(deleteQueueInternal(name, ifUnused, ifEmpty));
    }

    private CompletableFuture<Long> deleteQueueInternal(String name, boolean ifUnused, boolean ifEmpty) {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("name", Objects.requireNonNull(name, "name"));
        putIf(h, "if_unused", ifUnused);
        putIf(h, "if_empty", ifEmpty);
        return callAsync(Op.DELETE_QUEUE, h).thenApply(f -> f.getLong("messages"));
    }

    /** Removes every ready message from a queue and returns how many were removed. */
    public long purgeQueue(String name) {
        return await(purgeQueueInternal(name));
    }

    public CompletableFuture<Long> purgeQueueAsync(String name) {
        return handOff(purgeQueueInternal(name));
    }

    private CompletableFuture<Long> purgeQueueInternal(String name) {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("name", Objects.requireNonNull(name, "name"));
        return callAsync(Op.PURGE_QUEUE, h).thenApply(f -> f.getLong("messages"));
    }

    /** Declares a durable exchange of type {@code direct}, {@code fanout}, {@code topic} or {@code headers}. */
    public void declareExchange(String name, String type) {
        declareExchange(name, type, new ExchangeOptions());
    }

    /** Declares an exchange of type {@code direct}, {@code fanout}, {@code topic} or {@code headers}. */
    public void declareExchange(String name, String type, ExchangeOptions options) {
        await(declareExchangeInternal(name, type, options));
    }

    public CompletableFuture<Void> declareExchangeAsync(String name, String type, ExchangeOptions options) {
        return handOff(declareExchangeInternal(name, type, options));
    }

    private CompletableFuture<Void> declareExchangeInternal(String name, String type, ExchangeOptions o) {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("name", Objects.requireNonNull(name, "name"));
        h.put("type", Objects.requireNonNull(type, "type"));
        putIf(h, "durable", o.durable());
        putIf(h, "auto_delete", o.autoDelete());
        putIf(h, "internal", o.internal());
        putIf(h, "passive", o.passive());
        putIf(h, "args", o.args());
        return callAsync(Op.DECLARE_EXCHANGE, h).thenApply(f -> null);
    }

    public void deleteExchange(String name) {
        deleteExchange(name, false);
    }

    /** Deletes an exchange. With {@code ifUnused}, fails with 406 when it has bindings. */
    public void deleteExchange(String name, boolean ifUnused) {
        await(deleteExchangeInternal(name, ifUnused));
    }

    public CompletableFuture<Void> deleteExchangeAsync(String name, boolean ifUnused) {
        return handOff(deleteExchangeInternal(name, ifUnused));
    }

    private CompletableFuture<Void> deleteExchangeInternal(String name, boolean ifUnused) {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("name", Objects.requireNonNull(name, "name"));
        putIf(h, "if_unused", ifUnused);
        return callAsync(Op.DELETE_EXCHANGE, h).thenApply(f -> null);
    }

    /** Binds {@code queue} to {@code exchange} with a routing key or pattern. */
    public void bindQueue(String queue, String exchange, String key) {
        bindQueue(queue, exchange, key, null);
    }

    /** Binds with arguments, for example {@code x-match} for headers exchanges. */
    public void bindQueue(String queue, String exchange, String key, Map<String, ?> args) {
        await(bind(Op.BIND, exchange, queue, null, key, args));
    }

    public CompletableFuture<Void> bindQueueAsync(String queue, String exchange, String key, Map<String, ?> args) {
        return handOff(bind(Op.BIND, exchange, queue, null, key, args));
    }

    public void unbindQueue(String queue, String exchange, String key) {
        unbindQueue(queue, exchange, key, null);
    }

    public void unbindQueue(String queue, String exchange, String key, Map<String, ?> args) {
        await(bind(Op.UNBIND, exchange, queue, null, key, args));
    }

    public CompletableFuture<Void> unbindQueueAsync(String queue, String exchange, String key, Map<String, ?> args) {
        return handOff(bind(Op.UNBIND, exchange, queue, null, key, args));
    }

    /** Binds exchange {@code destination} to exchange {@code source}. */
    public void bindExchange(String destination, String source, String key) {
        bindExchange(destination, source, key, null);
    }

    public void bindExchange(String destination, String source, String key, Map<String, ?> args) {
        await(bind(Op.BIND, source, null, destination, key, args));
    }

    public CompletableFuture<Void> bindExchangeAsync(String destination, String source, String key, Map<String, ?> args) {
        return handOff(bind(Op.BIND, source, null, destination, key, args));
    }

    public void unbindExchange(String destination, String source, String key) {
        unbindExchange(destination, source, key, null);
    }

    public void unbindExchange(String destination, String source, String key, Map<String, ?> args) {
        await(bind(Op.UNBIND, source, null, destination, key, args));
    }

    public CompletableFuture<Void> unbindExchangeAsync(String destination, String source, String key, Map<String, ?> args) {
        return handOff(bind(Op.UNBIND, source, null, destination, key, args));
    }

    private CompletableFuture<Void> bind(int op, String exchange, String queue, String toExchange, String key, Map<String, ?> args) {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("exchange", Objects.requireNonNull(exchange, "exchange"));
        putIf(h, "queue", queue);
        putIf(h, "to_exchange", toExchange);
        putIf(h, "key", key);
        putIf(h, "args", args);
        return callAsync(op, h).thenApply(f -> null);
    }

    // ---------------------------------------------------------------- publishing

    /**
     * Publishes a persistent message and waits for the broker's confirm. Use exchange {@code ""}
     * to send straight to the queue named by {@code key}.
     */
    public void publish(String exchange, String key, byte[] body) {
        publish(exchange, key, body, new PublishOptions());
    }

    /** Publishes a UTF-8 text message and waits for the broker's confirm. */
    public void publish(String exchange, String key, String body) {
        publish(exchange, key, utf8(body), new PublishOptions());
    }

    public void publish(String exchange, String key, String body, PublishOptions options) {
        publish(exchange, key, utf8(body), options);
    }

    /**
     * Publishes a message. With {@link PublishOptions#confirm()} (the default) this waits for the
     * broker's confirm and throws {@link NackedException} when the broker rejects the message and
     * {@link UnroutableException} when a mandatory message reached no queue. While the broker has
     * paused publishers, this waits until it resumes them.
     */
    public void publish(String exchange, String key, byte[] body, PublishOptions options) {
        awaitFlow();
        await(publishInternal(exchange, key, body, options));
    }

    /** Publishes without blocking. The future completes when the broker confirms. */
    public CompletableFuture<Void> publishAsync(String exchange, String key, byte[] body) {
        return publishAsync(exchange, key, body, new PublishOptions());
    }

    public CompletableFuture<Void> publishAsync(String exchange, String key, String body) {
        return publishAsync(exchange, key, utf8(body), new PublishOptions());
    }

    public CompletableFuture<Void> publishAsync(String exchange, String key, String body, PublishOptions options) {
        return publishAsync(exchange, key, utf8(body), options);
    }

    /**
     * Publishes without blocking. The future completes when the broker confirms, or right away
     * when {@link PublishOptions#confirm()} is off. It fails with {@link NackedException},
     * {@link UnroutableException} or {@link ConnectionClosedException}.
     */
    public CompletableFuture<Void> publishAsync(String exchange, String key, byte[] body, PublishOptions options) {
        return handOff(publishInternal(exchange, key, body, options));
    }

    private CompletableFuture<Void> publishInternal(String exchange, String key, byte[] body, PublishOptions o) {
        Objects.requireNonNull(o, "options");
        byte[] payload = body == null ? EMPTY : body;
        Map<String, Object> h = new LinkedHashMap<>();
        putIf(h, "exchange", exchange);
        putIf(h, "key", key);
        putIf(h, "mandatory", o.mandatory());
        putIf(h, "persistent", o.persistent());
        putIf(h, "props", o.properties().toMap());
        putIf(h, "headers", o.headers());
        synchronized (publishLock) {
            long seq = o.confirm() ? nextSeq : 0;
            if (seq > 0) {
                h.put("seq", seq);
            }
            byte[] header = FrameCodec.encodeHeader(h);
            long len = FrameCodec.frameLength(header.length, payload.length);
            if (len > maxFrame) {
                throw new FireMQException("message too large: the frame would be " + len
                        + " bytes and the broker accepts at most " + maxFrame);
            }
            byte[] frame = FrameCodec.encode(Op.PUBLISH, header, payload);
            CompletableFuture<Void> confirmed = new CompletableFuture<>();
            synchronized (lock) {
                if (closed != null) {
                    return CompletableFuture.failedFuture(closed);
                }
                if (seq > 0) {
                    confirms.put(seq, confirmed);
                }
            }
            outbox.add(frame);
            if (seq == 0) {
                return CompletableFuture.completedFuture(null);
            }
            nextSeq++;
            return confirmed;
        }
    }

    private void awaitFlow() {
        if (!paused) {
            return;
        }
        synchronized (flowMonitor) {
            while (paused && closed == null) {
                try {
                    flowMonitor.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new FireMQException("interrupted while the broker paused publishers", e);
                }
            }
        }
    }

    private void setPaused(boolean pause) {
        synchronized (flowMonitor) {
            paused = pause;
            flowMonitor.notifyAll();
        }
        if (pause) {
            LOG.log(System.Logger.Level.WARNING, "the broker paused publishers (memory or disk alarm)");
        }
    }

    // ---------------------------------------------------------------- consuming

    /** Starts consuming from a queue with default options (prefetch 100, manual acks). */
    public Subscription subscribe(String queue) {
        return subscribe(queue, new SubscribeOptions());
    }

    /** Starts consuming from a queue. Take messages from the returned subscription. */
    public Subscription subscribe(String queue, SubscribeOptions options) {
        return await(subscribeInternal(queue, options));
    }

    /**
     * Starts consuming from a queue and calls {@code handler} for each message on a dedicated
     * thread, one message at a time. The handler acknowledges messages itself (unless
     * {@code noAck}). When the handler throws, the error is logged and the message is nacked with
     * requeue. Cancel or close the returned subscription to stop.
     */
    public Subscription subscribe(String queue, SubscribeOptions options, Consumer<Message> handler) {
        Objects.requireNonNull(handler, "handler");
        Subscription s = subscribe(queue, options);
        Thread t = new Thread(() -> {
            Message m;
            while ((m = s.next()) != null) {
                try {
                    handler.accept(m);
                } catch (Throwable e) {
                    LOG.log(System.Logger.Level.WARNING,
                            "message handler for queue '" + queue + "' failed; requeueing the message", e);
                    try {
                        m.nack(true);
                    } catch (FireMQException ignored) {
                        // The connection is gone; the broker requeues the message.
                    }
                }
            }
        }, "firemq-consumer-" + s.id());
        t.setDaemon(true);
        t.start();
        return s;
    }

    public CompletableFuture<Subscription> subscribeAsync(String queue, SubscribeOptions options) {
        return handOff(subscribeInternal(queue, options));
    }

    private CompletableFuture<Subscription> subscribeInternal(String queue, SubscribeOptions o) {
        Objects.requireNonNull(queue, "queue");
        int id = nextSub.getAndIncrement();
        int window = Math.max(1, o.prefetch());
        Subscription s = new Subscription(this, id, queue, window, o.noAck());
        subs.put(id, s);
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("sub", id);
        h.put("queue", queue);
        h.put("credit", window);
        putIf(h, "no_ack", o.noAck());
        putIf(h, "exclusive", o.exclusive());
        putIf(h, "tag", o.tag());
        putIf(h, "args", o.args());
        return callAsync(Op.SUBSCRIBE, h).handle((f, e) -> {
            if (e != null) {
                subs.remove(id);
                s.end();
                throw unwrap(e);
            }
            return s;
        });
    }

    void removeSubscription(int id) {
        subs.remove(id);
    }

    /**
     * Takes one message from a queue, or returns {@code null} when it is empty. Unless
     * {@code noAck}, acknowledge the message with {@link Message#ack()}.
     */
    public Message get(String queue, boolean noAck) {
        return await(getInternal(queue, noAck));
    }

    public CompletableFuture<Message> getAsync(String queue, boolean noAck) {
        return handOff(getInternal(queue, noAck));
    }

    private CompletableFuture<Message> getInternal(String queue, boolean noAck) {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("queue", Objects.requireNonNull(queue, "queue"));
        putIf(h, "no_ack", noAck);
        return callAsync(Op.GET, h).thenApply(f -> f.op() == Op.GET_OK ? message(f, noAck ? null : this, null) : null);
    }

    private static Message message(Frame f, Client acker, Credit credit) {
        return new Message(f.payload(), f.getString("exchange"), f.getString("key"),
                MessageProperties.fromMap(f.getMap("props")), f.getMap("headers"), f.getBool("redelivered"),
                f.getLong("tag"), acker, credit);
    }

    // ---------------------------------------------------------------- closing

    /**
     * Closes the connection gracefully: sends {@code close} and waits up to two seconds for the
     * broker's reply. Pending requests and confirms fail with {@link ConnectionClosedException}.
     * Safe to call more than once.
     */
    @Override
    public void close() {
        if (closed == null) {
            closing = true;
            Map<String, Object> h = new LinkedHashMap<>();
            h.put("code", 200);
            h.put("msg", "client closed");
            sendQuietly(Op.CLOSE, h);
            try {
                closedLatch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            shutdown(new ConnectionClosedException("the client closed the connection"));
        }
        if (Thread.currentThread() != writer) {
            try {
                writer.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        closeQuietly(socket);
    }

    // ---------------------------------------------------------------- internals

    /** Sends a request and returns the future of its reply. Completes on the reader thread. */
    CompletableFuture<Frame> callAsync(int op, Map<String, Object> header) {
        int rid = nextRid.getAndIncrement();
        header.put("rid", rid);
        byte[] frame = FrameCodec.encode(op, header, EMPTY);
        CompletableFuture<Frame> reply = new CompletableFuture<>();
        synchronized (lock) {
            if (closed != null) {
                return CompletableFuture.failedFuture(closed);
            }
            requests.put(rid, reply);
        }
        outbox.add(frame);
        return reply;
    }

    void send(int op, Map<String, Object> header, byte[] payload) {
        FireMQException c = closed;
        if (c != null) {
            throw c;
        }
        outbox.add(FrameCodec.encode(op, header, payload == null ? EMPTY : payload));
    }

    void sendQuietly(int op, Map<String, Object> header) {
        try {
            send(op, header, null);
        } catch (FireMQException ignored) {
            // The connection is closed; nothing to send.
        }
    }

    private void writeLoop() {
        long tick = TimeUnit.SECONDS.toNanos(Math.max(1, heartbeat / 2));
        long dead = TimeUnit.SECONDS.toNanos(2L * heartbeat);
        long pollMillis = Math.min(1000, TimeUnit.NANOSECONDS.toMillis(tick));
        try {
            while (true) {
                byte[] frame = heartbeat > 0 ? outbox.poll(pollMillis, TimeUnit.MILLISECONDS) : outbox.take();
                if (frame != null) {
                    boolean stop = false;
                    for (byte[] f = frame; f != null; f = outbox.poll()) {
                        if (f == STOP) {
                            stop = true;
                            break;
                        }
                        out.write(f);
                    }
                    out.flush();
                    lastSend = System.nanoTime();
                    if (stop) {
                        return;
                    }
                }
                if (heartbeat > 0) {
                    long now = System.nanoTime();
                    if (now - lastSend >= tick) {
                        out.write(PING);
                        out.flush();
                        lastSend = now;
                    }
                    if (now - lastRecv > dead) {
                        LOG.log(System.Logger.Level.WARNING, "the broker missed heartbeats; closing the connection");
                        shutdown(new ConnectionClosedException("the broker missed heartbeats"));
                        return;
                    }
                }
            }
        } catch (IOException e) {
            shutdown(new ConnectionClosedException("connection lost: " + e.getMessage(), e));
        } catch (InterruptedException e) {
            shutdown(new ConnectionClosedException("the writer thread was interrupted", e));
        } finally {
            closeQuietly(socket);
        }
    }

    private void readLoop() {
        FireMQException reason;
        while (true) {
            Frame f;
            try {
                f = FrameCodec.read(in, maxFrame);
            } catch (FrameCodec.MalformedHeaderException e) {
                lastRecv = System.nanoTime();
                LOG.log(System.Logger.Level.WARNING, "ignoring frame: " + e.getMessage());
                continue;
            } catch (EOFException e) {
                reason = new ConnectionClosedException(
                        closing ? "the client closed the connection" : "the broker closed the connection", e);
                break;
            } catch (IOException e) {
                reason = closing
                        ? new ConnectionClosedException("the client closed the connection", e)
                        : new ConnectionClosedException("connection lost: " + e.getMessage(), e);
                break;
            }
            lastRecv = System.nanoTime();
            FireMQException end = null;
            try {
                end = dispatch(f);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "failed to handle a " + f.name() + " frame", e);
            }
            if (end != null) {
                reason = end;
                break;
            }
        }
        shutdown(reason);
    }

    /** Handles one frame from the broker. Returns a reason when the connection is over. */
    private FireMQException dispatch(Frame f) {
        switch (f.op()) {
            case Op.OK:
            case Op.GET_OK: {
                CompletableFuture<Frame> r;
                synchronized (lock) {
                    r = requests.remove(f.getInt("rid"));
                }
                if (r != null) {
                    r.complete(f);
                }
                return null;
            }
            case Op.ERROR: {
                int code = f.getInt("code");
                String msg = f.getString("msg");
                boolean fatal = f.getBool("fatal");
                CompletableFuture<Frame> r = null;
                if (f.has("rid")) {
                    synchronized (lock) {
                        r = requests.remove(f.getInt("rid"));
                    }
                }
                if (r != null) {
                    r.completeExceptionally(new ServerException(code, msg));
                } else if (!fatal) {
                    LOG.log(System.Logger.Level.WARNING, "the broker reported an error: " + msg + " (code " + code + ")");
                }
                if (fatal) {
                    return new ConnectionClosedException(msg + " (code " + code + ")", code);
                }
                return null;
            }
            case Op.CONFIRM:
                onConfirm(f);
                return null;
            case Op.RETURN: {
                long seq = f.getLong("seq");
                if (seq > 0) {
                    synchronized (lock) {
                        returned.put(seq, new String[] {f.getString("exchange"), f.getString("key")});
                    }
                }
                return null;
            }
            case Op.DELIVER: {
                Subscription s = subs.get(f.getInt("sub"));
                if (s != null) {
                    s.deliver(message(f, s.noAck() ? null : this, s.credit()));
                }
                return null;
            }
            case Op.CANCELLED: {
                Subscription s = subs.remove(f.getInt("sub"));
                if (s != null) {
                    LOG.log(System.Logger.Level.INFO, "the broker cancelled the subscription to '" + s.queue() + "': "
                            + f.getString("msg") + " (code " + f.getInt("code") + ")");
                    s.end();
                }
                return null;
            }
            case Op.PING:
                sendQuietly(Op.PONG, null);
                return null;
            case Op.PONG:
                return null;
            case Op.FLOW:
                setPaused(f.getBool("pause"));
                return null;
            case Op.CLOSE: {
                if (closing) {
                    return new ConnectionClosedException("the client closed the connection");
                }
                int code = f.getInt("code");
                String msg = f.getString("msg");
                Map<String, Object> h = new LinkedHashMap<>();
                h.put("code", 200);
                sendQuietly(Op.CLOSE, h);
                return new ConnectionClosedException(
                        (msg.isEmpty() ? "closed by the broker" : msg) + " (code " + code + ")", code);
            }
            default:
                LOG.log(System.Logger.Level.DEBUG, "ignoring unexpected " + f.name() + " frame");
                return null;
        }
    }

    private void onConfirm(Frame f) {
        List<Runnable> done = new ArrayList<>();
        synchronized (lock) {
            if (f.getBool("nack")) {
                if (f.has("seq")) {
                    long seq = f.getLong("seq");
                    returned.remove(seq);
                    CompletableFuture<Void> c = confirms.remove(seq);
                    if (c != null) {
                        NackedException e = new NackedException(f.getInt("code"), f.getString("msg"));
                        done.add(() -> c.completeExceptionally(e));
                    }
                }
            } else if (f.has("upto")) {
                NavigableMap<Long, CompletableFuture<Void>> head = confirms.headMap(f.getLong("upto"), true);
                for (Map.Entry<Long, CompletableFuture<Void>> e : head.entrySet()) {
                    String[] r = returned.remove(e.getKey());
                    CompletableFuture<Void> c = e.getValue();
                    if (r != null) {
                        UnroutableException ex = new UnroutableException(r[0], r[1]);
                        done.add(() -> c.completeExceptionally(ex));
                    } else {
                        done.add(() -> c.complete(null));
                    }
                }
                head.clear();
            }
        }
        for (Runnable r : done) {
            r.run();
        }
    }

    private void shutdown(FireMQException reason) {
        List<CompletableFuture<Frame>> reqs;
        List<CompletableFuture<Void>> confs;
        synchronized (lock) {
            if (closed != null) {
                return;
            }
            closed = reason;
            reqs = new ArrayList<>(requests.values());
            requests.clear();
            confs = new ArrayList<>(confirms.values());
            confirms.clear();
            returned.clear();
        }
        List<Subscription> ss = new ArrayList<>(subs.values());
        subs.clear();
        outbox.add(STOP);
        synchronized (flowMonitor) {
            flowMonitor.notifyAll();
        }
        closedLatch.countDown();
        for (CompletableFuture<Frame> r : reqs) {
            r.completeExceptionally(reason);
        }
        for (CompletableFuture<Void> c : confs) {
            c.completeExceptionally(reason);
        }
        for (Subscription s : ss) {
            s.end();
        }
    }

    private static void putIf(Map<String, Object> h, String key, boolean value) {
        if (value) {
            h.put(key, true);
        }
    }

    private static void putIf(Map<String, Object> h, String key, String value) {
        if (value != null && !value.isEmpty()) {
            h.put(key, value);
        }
    }

    private static void putIf(Map<String, Object> h, String key, Map<String, ?> value) {
        if (value != null && !value.isEmpty()) {
            h.put(key, value);
        }
    }

    private static byte[] utf8(String s) {
        return s == null ? EMPTY : s.getBytes(StandardCharsets.UTF_8);
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
            // Nothing left to do.
        }
    }

    /** Returns a future that completes like {@code f}, but runs dependent callbacks off the reader thread. */
    static <T> CompletableFuture<T> handOff(CompletableFuture<T> f) {
        if (f.isDone()) {
            return f;
        }
        return f.whenCompleteAsync((v, e) -> {}, CALLBACKS);
    }

    /** Waits for a future and rethrows its failure as a {@link FireMQException}. */
    static <T> T await(CompletableFuture<T> f) {
        try {
            return f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FireMQException("interrupted while waiting for the broker", e);
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
    }

    static FireMQException unwrap(Throwable t) {
        while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
            t = t.getCause();
        }
        if (t instanceof FireMQException) {
            return (FireMQException) t;
        }
        return new FireMQException(String.valueOf(t.getMessage()), t);
    }

    @Override
    public String toString() {
        return "Client{" + options.url() + ", " + (isClosed() ? "closed" : "open") + "}";
    }
}
