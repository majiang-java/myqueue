package com.myredisqueue;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.myredisqueue.consumer.RedisqConsumer;
import com.myredisqueue.exceptions.RedisqException;
import com.sun.xml.internal.ws.encoding.soap.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.util.Pool;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.codahale.metrics.MetricRegistry.name;

public class MyQueueImpl<T extends Document> implements MyQueue<T> {
    static final Mapper<StateInfo> stateMapper = new Mapper<>(StateInfo.class);
    private static final Logger LOG = LoggerFactory.getLogger(MyQueueImpl.class);
    private static final int DEFAULT_SUBSCRIPTION_WAIT_TIMEOUT_SECONDS = 30;
    private static final int MARGIN_MS = 60_000;
    private final String queueName;
    private final String inFlightQueueName;
    private final String name;
    private final Duration timeout;
    private final Mapper<TimedWrap<T>> mapper;
    private final Names names;
    private final int lockTime;
    private final Pool<Jedis> jedisPool;
    private final Scheduler<T> scheduler;
    private int ttlStateInfo;
    private final AtomicBoolean working = new AtomicBoolean(false);
    private Duration discardTime;

    private final MetricRegistry metricRegistry;

    private Future<?> mainLoop;
    private Future<?> inFlightLoop;

    private Timer restoreBlockedTime;
    private Timer idleTimer;

    private Timer pushTimer;
    private Timer executeWaitTimer;
    private Meter serializationErrors;

    private ExecutorService mainThreadPool;

    public MyQueueImpl(String name, Duration timeout, Duration ttlStateInfo, Duration lockTime,
                  Duration discardTime, Consumer<T> consumer, Class<T> klass, Pool<Jedis> jedisPool,
                  ScheduledExecutorService threadPool, long threadDelay, MetricRegistry metricRegistry) {
        Preconditions.checkState(ttlStateInfo.minus(lockTime).toMillis() > MARGIN_MS, "The ttl for" +
                " a status has to be higher than the time a document is locked for by "+MARGIN_MS + "ms");

        this.name = name;
        this.timeout = timeout;
        this.ttlStateInfo = (int) ttlStateInfo.getSeconds();
        this.lockTime = (int) lockTime.getSeconds();
        this.discardTime = discardTime;
        this.metricRegistry = metricRegistry;
        RedisqConsumer<T> subscription = new RedisqConsumer<>(consumer,jedisPool,this);
        Mapper<T> documentMapper = new Mapper<>(klass);
        this.scheduler = Scheduler.of(threadDelay, threadPool, subscription, jedisPool, documentMapper);
        this.names = new Names();
        this.queueName = names.queueNameFor(name);

        this.inFlightQueueName = names.inFlightQueueNameFor(name);
        this.jedisPool = jedisPool;
        this.mapper = new Mapper<>(new ObjectMapper().getTypeFactory()
                .constructParametricType(TimedWrap.class, klass));
        this.mainThreadPool = Executors.newFixedThreadPool(2,  new ThreadFactoryBuilder().setNameFormat("redisq-ai.grakn.redisq.consumer-%s").build());
        this.pushTimer = metricRegistry.timer(name(this.getClass(), "push"));
        this.idleTimer = metricRegistry.timer(name(this.getClass(), "idle"));
        metricRegistry.register(name(this.getClass(), "queue", "size"),
                new CachedGauge<Long>(15,TimeUnit.SECONDS) {

                    @Override
                    protected Long loadValue() {
                        try(Jedis jedis = jedisPool.getResource()) {
                            return jedis.llen(queueName);
                        }
                    }
                });
        this.restoreBlockedTime = metricRegistry.timer(name(this.getClass(), "restore_blocked"));
        this.executeWaitTimer = metricRegistry.timer(name(this.getClass(), "execute_wait"));
        this.serializationErrors = metricRegistry.meter(name(this.getClass(), "serialization_errors"));
    }





        @Override
    public void push(T document) {
        long timestampNs = System.currentTimeMillis();
        String serialized;
        String stateSerialized;
        try{
            serialized = mapper.serialize(new TimedWrap<>(document,timestampNs));
            stateSerialized = stateMapper.serialize(new StateInfo(State.NEW, timestampNs, ""));
        }catch (SerializationException e){
            serializationErrors.mark();
            throw new RedisqException("Could not serialize element " + document.getIdAsString(), e);
        }
         LOG.debug("Jedis active: {}, idle: {}", jedisPool.getNumActive(), jedisPool.getNumIdle());
        try ( Jedis jedis = jedisPool.getResource();Timer.Context ignore = pushTimer.time()){
            Transaction transaction = jedis.multi();
            String id = document.getIdAsString();
            String lockId = names.lockKeyFromId(id);
            transaction.setex(lockId, lockTime, "locked");
            transaction.lpush(queueName, id);
            transaction.setex(names.contentKeyFromId(id), ttlStateInfo, serialized);
            transaction.setex(names.stateKeyFromId(id), ttlStateInfo, stateSerialized);
        }

    }

    @Override
    public void pushAndWait(T document, long waitTimeout, TimeUnit waitTimeoutUnit) {

    }
}
