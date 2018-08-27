package com.myredisqueue.exceptions;

public class RedisqException extends RuntimeException {
    public RedisqException(String s, Exception e) {
        super(s, e);
    }
}
