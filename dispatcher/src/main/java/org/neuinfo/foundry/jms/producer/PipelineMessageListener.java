package org.neuinfo.foundry.jms.producer;

import org.apache.log4j.Logger;
import org.neuinfo.foundry.common.Constants;
import org.neuinfo.foundry.common.config.Configuration;

/**
 * Created by bozyurt on 7/9/15.
 */
class PipelineMessageListener {
    private Configuration configuration;
    private Thread consumerThread;
    private PipelineMessageConsumer consumer;
    static Logger logger = Logger.getLogger(PipelineMessageListener.class);

    public PipelineMessageListener(Configuration configuration) {
        this.configuration = configuration;
    }

    public void startup() {
        try {
            this.consumer = new PipelineMessageConsumer(configuration, Constants.PIPELINE_MSG_QUEUE);
            consumer.startup();
            consumerThread = new Thread(consumer);
            consumerThread.setDaemon(true);

            consumerThread.start();

        } catch (Throwable t) {
            logger.warn("Fail to start PipelineMessageConsumer ", t);
        }
    }

    public void shutdown() {
         logger.info("shutting down PipelineMessageConsumer");
        try {
           this.consumer.setFinished(true);

        } catch (Throwable t) {
            logger.error("Fail to close PipelineMessageConsumer:" + t.getMessage());
        }

    }
}
