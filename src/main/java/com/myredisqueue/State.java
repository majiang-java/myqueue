package com.myredisqueue;

public enum State {
    NEW,PROCESSING, FIELD,DONE;
    public boolean inFinal(){
        return this.equals(DONE) || this.equals(FIELD);
    }
}
