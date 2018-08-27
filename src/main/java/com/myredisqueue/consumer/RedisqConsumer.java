package com.myredisqueue.consumer;

import com.myredisqueue.Document;
import com.myredisqueue.MyQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.util.function.Consumer;

public class RedisqConsumer<T extends Document> implements QueueConsumer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisqConsumer.class);

    private Consumer<T> consumer;
    private Pool<Jedis> jedisPool;
    private MyQueueImpl<T> tRedisq;

    public RedisqConsumer(Consumer<T> consumer, Pool<Jedis> jedisPool, MyQueueImpl<T> tRedisq) {
        this.consumer = consumer;
        this.jedisPool = jedisPool;
        this.tRedisq = tRedisq;
    }


    @Override
    public void process() {

    }
}
