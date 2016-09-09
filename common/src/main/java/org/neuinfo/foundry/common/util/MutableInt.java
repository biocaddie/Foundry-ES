package org.neuinfo.foundry.common.util;

/**
 * Created by bozyurt on 11/1/15.
 */
public class MutableInt {
    private int value;

    public MutableInt(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void incr() {
        value++;
    }
}
