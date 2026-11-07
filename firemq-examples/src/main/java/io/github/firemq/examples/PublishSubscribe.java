package io.github.firemq.examples;

import io.github.firemq.Client;
import io.github.firemq.ExchangeOptions;
import io.github.firemq.FireMQ;
import io.github.firemq.QueueOptions;
import io.github.firemq.Subscription;
import java.util.List;

/** Broadcasts log lines through a fanout exchange to every subscriber's private queue. */
public final class PublishSubscribe {
    public static void main(String[] args) {
        List<String> logs = List.of("info: service started", "warning: disk at 80%", "error: payment failed");
        try (Client client = FireMQ.connect(Examples.url())) {
            client.declareExchange("logs.java", "fanout", new ExchangeOptions());

            Subscription[] subscribers = new Subscription[2];
            for (int i = 0; i < subscribers.length; i++) {
                String queue = client.declareQueue("", QueueOptions.temporary()).name();
                client.bindQueue(queue, "logs.java", "");
                subscribers[i] = client.subscribe(queue);
            }

            for (String line : logs) {
                client.publish("logs.java", "", line);
                System.out.println(" [x] Sent '" + line + "'");
            }

            for (int i = 0; i < subscribers.length; i++) {
                List<String> got = Examples.take(subscribers[i], logs.size());
                System.out.println(" [subscriber " + (i + 1) + "] Received " + got);
                subscribers[i].cancel();
            }
        }
    }
}
