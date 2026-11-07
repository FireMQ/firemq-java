package io.github.firemq;

import io.github.firemq.protocol.Op;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Messages from a queue, in delivery order.
 *
 * <p>Take messages with {@link #next()}, {@link #next(Duration)}, {@link #nextAsync()} or by
 * iterating. The subscription ends when it is cancelled, when the broker cancels it (for example
 * because the queue was deleted) or when the connection closes; after that, buffered messages are
 * still returned, then {@code next} returns {@code null} and iteration stops.
 *
 * <p>Closing the subscription cancels it. Unacknowledged messages can still be acknowledged.
 */
public final class Subscription implements AutoCloseable, Iterable<Message> {
    private final Client client;
    private final int id;
    private final String queue;
    private final boolean noAck;
    private final Credit credit;

    private final ArrayDeque<Message> buffer = new ArrayDeque<>();
    private final ArrayDeque<CompletableFuture<Message>> waiters = new ArrayDeque<>();
    private boolean ended;
    private final AtomicBoolean cancelled = new AtomicBoolean();

    Subscription(Client client, int id, String queue, int window, boolean noAck) {
        this.client = client;
        this.id = id;
        this.queue = queue;
        this.noAck = noAck;
        this.credit = new Credit(client, id, window);
    }

    /** The subscription id on the connection. */
    public int id() {
        return id;
    }

    public String queue() {
        return queue;
    }

    public boolean noAck() {
        return noAck;
    }

    Credit credit() {
        return credit;
    }

    /** Called by the reader thread for each delivery. */
    void deliver(Message m) {
        while (true) {
            CompletableFuture<Message> w;
            synchronized (this) {
                if (ended) {
                    return;
                }
                w = waiters.poll();
                if (w == null) {
                    buffer.add(m);
                    notifyAll();
                    return;
                }
            }
            if (w.complete(m)) {
                taken(m);
                return;
            }
        }
    }

    /** Marks the subscription as ended; waiting readers get {@code null} once the buffer is empty. */
    void end() {
        List<CompletableFuture<Message>> ws;
        synchronized (this) {
            if (ended) {
                return;
            }
            ended = true;
            credit.stop();
            ws = new ArrayList<>(waiters);
            waiters.clear();
            notifyAll();
        }
        for (CompletableFuture<Message> w : ws) {
            w.complete(null);
        }
    }

    private void taken(Message m) {
        if (noAck) {
            credit.release(1);
        }
    }

    /** Waits for the next message. Returns {@code null} once the subscription has ended. */
    public Message next() {
        Message m;
        synchronized (this) {
            while (buffer.isEmpty() && !ended) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new FireMQException("interrupted while waiting for a message", e);
                }
            }
            m = buffer.poll();
        }
        if (m != null) {
            taken(m);
        }
        return m;
    }

    /**
     * Waits up to {@code timeout} for the next message. Returns {@code null} on timeout and once
     * the subscription has ended; {@link #isActive()} tells the two apart.
     */
    public Message next(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        Message m;
        synchronized (this) {
            while (buffer.isEmpty() && !ended) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return null;
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(this, remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new FireMQException("interrupted while waiting for a message", e);
                }
            }
            m = buffer.poll();
        }
        if (m != null) {
            taken(m);
        }
        return m;
    }

    /**
     * The next message, or {@code null} once the subscription has ended. Cancelling the returned
     * future gives up the wait without losing a message.
     *
     * <p>When no message is buffered, the future completes on the client's reader thread. Do not
     * block in callbacks attached with {@code thenApply} or {@code thenAccept}; use the
     * {@code *Async} variants instead.
     */
    public CompletableFuture<Message> nextAsync() {
        Message m;
        synchronized (this) {
            m = buffer.poll();
            if (m == null) {
                if (ended) {
                    return CompletableFuture.completedFuture(null);
                }
                CompletableFuture<Message> w = new CompletableFuture<>();
                waiters.add(w);
                return w;
            }
        }
        taken(m);
        return CompletableFuture.completedFuture(m);
    }

    /** True until the subscription is cancelled, cancelled by the broker, or the connection closes. */
    public synchronized boolean isActive() {
        return !ended;
    }

    /** Messages received and not yet taken. */
    public synchronized int buffered() {
        return buffer.size();
    }

    /**
     * Stops the subscription and waits for the broker to confirm. The broker sends no more
     * messages; buffered ones can still be taken.
     */
    public void cancel() {
        Client.await(cancelAsync());
    }

    /** Stops the subscription without waiting. */
    public CompletableFuture<Void> cancelAsync() {
        if (!cancelled.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        client.removeSubscription(id);
        end();
        if (client.isClosed()) {
            return CompletableFuture.completedFuture(null);
        }
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("sub", id);
        return client.callAsync(Op.UNSUBSCRIBE, h).handle((f, e) -> {
            if (e != null && !client.isClosed()) {
                throw Client.unwrap(e);
            }
            return null;
        });
    }

    /** Cancels the subscription. Does nothing when the connection is already closed. */
    @Override
    public void close() {
        try {
            cancel();
        } catch (ConnectionClosedException e) {
            // The broker forgets the subscription with the connection.
        }
    }

    /**
     * Iterates over messages until the subscription ends. Each call to {@code hasNext} may block.
     */
    @Override
    public Iterator<Message> iterator() {
        return new Iterator<>() {
            private Message pending;
            private boolean done;

            @Override
            public boolean hasNext() {
                if (pending != null) {
                    return true;
                }
                if (done) {
                    return false;
                }
                pending = Subscription.this.next();
                if (pending == null) {
                    done = true;
                }
                return pending != null;
            }

            @Override
            public Message next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("the subscription has ended");
                }
                Message m = pending;
                pending = null;
                return m;
            }
        };
    }

    @Override
    public String toString() {
        return "Subscription{id=" + id + ", queue='" + queue + "', active=" + isActive() + "}";
    }
}
