package org.neuinfo.foundry.consumers.common;

/**
 * Created by bozyurt on 7/20/15.
 */
public interface ConsumerProcessListener {

    public void register(String consumerId, String consumerName);
    public void documentProcessed(String consumerId, String consumerName, String docId);
}
