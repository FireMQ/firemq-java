package io.github.firemq.examples;

import io.github.firemq.Client;
import io.github.firemq.FireMQ;
import io.github.firemq.SubscribeOptions;
import io.github.firemq.Subscription;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Distributes tasks among two workers. Each dot in a task is a second of work, scaled down here to
 * 100 ms. With prefetch 1 a busy worker gets no new task until it acknowledges the current one.
 */
public final class WorkQueues {
    public static void main(String[] args) throws InterruptedException {
        List<String> tasks = List.of("First task.", "Second task..", "Third task...", "Fourth task....",
                "Fifth task.....", "Sixth task.");
        try (Client client = FireMQ.connect(Examples.url())) {
            client.declareQueue("task_queue.java");
            client.purgeQueue("task_queue.java");

            CountDownLatch done = new CountDownLatch(tasks.size());
            Subscription[] workers = new Subscription[2];
            for (int i = 0; i < workers.length; i++) {
                String worker = "worker " + (i + 1);
                workers[i] = client.subscribe("task_queue.java", new SubscribeOptions().prefetch(1), msg -> {
                    String task = msg.text();
                    System.out.println(" [" + worker + "] Received '" + task + "'");
                    sleep(100L * task.chars().filter(c -> c == '.').count());
                    System.out.println(" [" + worker + "] Done");
                    msg.ack();
                    done.countDown();
                });
            }

            for (String task : tasks) {
                client.publish("", "task_queue.java", task); // persistent by default
                System.out.println(" [x] Sent '" + task + "'");
            }

            if (!done.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException(done.getCount() + " tasks were not processed");
            }
            for (Subscription w : workers) {
                w.cancel();
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
