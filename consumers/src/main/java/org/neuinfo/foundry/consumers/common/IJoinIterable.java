package org.neuinfo.foundry.consumers.common;

/**
 * Created by bozyurt on 2/17/17.
 */
public interface IJoinIterable<T> {
    public void reset(String refValue) throws Exception;
    public boolean hasNext();
    public T next();
    public T peek();
    public String getJoinValue();
    public String getAlias();
}
