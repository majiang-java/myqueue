package com.myredisqueue;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TimedWrap<T> {

    @JsonProperty
    private T element;

    @JsonProperty
    private long timestampMs;

    public TimedWrap(T elemment, long timestampMs){
        this.element = element;
        this.timestampMs = timestampMs;
    }

    public T getElement() {
        return element;
    }

    public long getTimestampMs() {
        return timestampMs;
    }
}
