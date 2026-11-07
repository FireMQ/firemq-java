package io.github.firemq.examples;

import io.github.firemq.Client;
import io.github.firemq.ExchangeOptions;
import io.github.firemq.FireMQ;
import io.github.firemq.QueueOptions;
import io.github.firemq.Subscription;
import java.util.List;

/**
 * Routes by pattern through a topic exchange. {@code *} matches one word and {@code #} matches
 * zero or more.
 */
public final class Topics {
    public static void main(String[] args) {
        try (Client client = FireMQ.connect(Examples.url())) {
            client.declareExchange("topic_logs.java", "topic", new ExchangeOptions());

            String critical = client.declareQueue("", QueueOptions.temporary()).name();
            client.bindQueue(critical, "topic_logs.java", "*.critical");
            String kernel = client.declareQueue("", QueueOptions.temporary()).name();
            client.bindQueue(kernel, "topic_logs.java", "kern.#");
            Subscription criticalSub = client.subscribe(critical);
            Subscription kernelSub = client.subscribe(kernel);

            for (String key : List.of("kern.critical", "kern.info", "app.critical", "app.info")) {
                client.publish("topic_logs.java", key, "a message about " + key);
                System.out.println(" [x] Sent '" + key + "'");
            }

            // *.critical gets kern.critical and app.critical; kern.# gets kern.critical and kern.info.
            System.out.println(" [*.critical] Received " + Examples.take(criticalSub, 2));
            System.out.println(" [kern.#] Received " + Examples.take(kernelSub, 2));
            criticalSub.cancel();
            kernelSub.cancel();
        }
    }
}
