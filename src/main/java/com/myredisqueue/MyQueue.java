package com.myredisqueue;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public interface MyQueue<T> {


    /**
     * Put a document in the queue
     * @param document  Document to be pushed to the queue. It must be serialisable.
     */
    void push(T document);

    /**
     * Same as push but it waits for the state of the document to be DONE i.e. the ai.grakn.redisq.consumer successfully completed
     * working on it.
     *
     * @param document          Document to be pushed to the queue. It must be serialisable.
     * @param waitTimeout       Timeout for the wait. A WaitException is thrown when expired
     * @param waitTimeoutUnit   Unit for the timeout
     */
    void pushAndWait(T document, long waitTimeout, TimeUnit waitTimeoutUnit) ;


}
