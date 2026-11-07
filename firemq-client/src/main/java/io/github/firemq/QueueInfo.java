package io.github.firemq;

/**
 * What {@link Client#declareQueue(String, QueueOptions)} reports.
 *
 * @param name the queue's name; the generated one for server-named queues
 * @param messages messages ready in the queue
 * @param consumers subscriptions on the queue
 */
public record QueueInfo(String name, long messages, long consumers) {}
