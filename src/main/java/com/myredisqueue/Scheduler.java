package com.myredisqueue;

import com.github.rholder.retry.*;
import com.myredisqueue.consumer.QueueConsumer;
import com.myredisqueue.consumer.RedisqConsumer;
import com.myredisqueue.exceptions.RedisqException;
import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import com.sun.xml.internal.ws.encoding.soap.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Scheduler<T extends Document> {

    private static final Names NAMES = new Names();
    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);

    private static final Retryer<Integer> CLOSE_RETRLER = RetryerBuilder.<Integer>newBuilder()
                .retryIfResult(r -> !(r != null && r == 0))
                .withWaitStrategy(WaitStrategies.fixedWait(100,TimeUnit.MILLISECONDS))
                .withStopStrategy(StopStrategies.stopAfterDelay(10, TimeUnit.SECONDS))
                .build();

    private final long threadDelay;
    private ScheduledExecutorService threadPool;
    private QueueConsumer<T> subscription;
    private Pool<Jedis> jedisPool;
    private Mapper<T> mapper;
    private AtomicInteger runningThreads = new AtomicInteger(0);

    private Scheduler(long threadDelay, ScheduledExecutorService threadPool, QueueConsumer<T> subscription,
                      Pool<Jedis> jedisPool, Mapper<T> mapper){
        this.threadDelay = threadDelay;
        this.threadPool = threadPool;
        this.subscription = subscription;
        this.jedisPool = jedisPool;
        this.mapper = mapper;
    }


    static <T extends Document> Scheduler<T> of(
            long threadDelay, ScheduledExecutorService threadPool, RedisqConsumer<T> subscription,
            Pool<Jedis> jedisPool, Mapper<T> mapper
    ) {
        return new Scheduler<>(threadDelay, threadPool, subscription, jedisPool, mapper);
    }

    void execute(T document) {
        String serialized;
        try{
            serialized = mapper.serialize(document);
        }catch (SerializationException e){
            throw new RedisqException("Could not serialize element " + document.getIdAsString(), e);
        }

        String key = NAMES.delayedKeyFromId(document.getIdAsString());

        try(Jedis jedis = jedisPool.getResource()) {
            jedis.set(key, serialized);
        }

        threadPool.schedule(() ->{
            runningThreads.incrementAndGet();
            try {
                processFromRedis(key);
            } finally{
                runningThreads.decrementAndGet();
            }
        }, threadDelay, TimeUnit.SECONDS);

    }

    //processor redis
    private void processFromRedis(String key) {
        String serialied;
        try(Jedis jedis = jedisPool.getResource()){
            serialied = jedis.get(key);
        }
        T document;
        try{
            document = mapper.deserialize(serialied);
        }catch (DeserializationException e){
            throw new RedisqException("Could not deserialize element " + serialied, e);
        }

        subscription.process();
    }

    void close() throws ExecutionException, InterruptedException{
        try{
            CLOSE_RETRLER.call(runningThreads::get);
        } catch (RetryException e){
            LOG.warn("Closing while some threads are still running");
        }
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.MINUTES);
    }
}
