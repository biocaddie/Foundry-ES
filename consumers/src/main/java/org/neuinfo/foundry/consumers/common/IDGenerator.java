package org.neuinfo.foundry.consumers.common;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by bozyurt on 7/20/15.
 */
public class IDGenerator {
    private AtomicInteger counter = new AtomicInteger(0);
    private static IDGenerator instance;

    public synchronized static IDGenerator getInstance() {
        if (instance == null) {
            instance = new IDGenerator();
        }
        return instance;
    }

    private IDGenerator() {}

    public int getNextId() {
        return counter.addAndGet(1);
    }

}
