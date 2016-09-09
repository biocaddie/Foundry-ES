package org.neuinfo.foundry.consumers.coordinator;

import javax.jms.JMSException;
import javax.jms.MessageListener;

/**
 * Created by bozyurt on 9/25/14.
 */
public interface IConsumer {

    public String getId();
    public String getName();

    public void startup(String configFile) throws Exception;

    public void shutdown() throws Exception;

    public void stop();

    public void handleMessages(MessageListener listener) throws JMSException;

    public String getSuccessMessageQueueName();

    public void setSuccessMessageQueueName(String successMessageQueueName);

    public String getFailureMessageQueueName();

    public void setFailureMessageQueueName(String failureMessageQueueName);

    public void setParam(String name, String value);

    public String getParam(String name);

    public void setCollectionName(String name);

    public String getCollectionName();

    /**
     * @param inStatus the status this consumer can respond to
     */
    public void setInStatus(String inStatus);

    public String getInStatus();

    /**
     * @param outStatus the status this consumer set the document wrapper on successful operation
     */
    public void setOutStatus(String outStatus);

    public String getOutStatus();


}
