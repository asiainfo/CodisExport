package com.asiainfo.codis.event;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by peng on 16/9/6.
 */
public abstract class EventQueue<T> {


    protected BlockingQueue<T> events = new LinkedBlockingQueue();

    public abstract boolean consumeEvent();

    public abstract boolean produceEvent(T events);


    public int getEventsCounts() {
        return events.size();
    }
}
