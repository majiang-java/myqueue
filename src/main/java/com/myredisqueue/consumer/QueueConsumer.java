package com.myredisqueue.consumer;

public interface QueueConsumer<T> {
    void process();
}
