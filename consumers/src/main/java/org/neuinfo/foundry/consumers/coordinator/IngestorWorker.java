package org.neuinfo.foundry.consumers.coordinator;

import java.util.concurrent.Callable;

/**
 * Created by bozyurt on 10/31/14.
 */
public class IngestorWorker implements Callable<Void> {
    IConsumer consumer;
    String configFile;

    public IngestorWorker(IConsumer consumer, String configFile) {
        this.consumer = consumer;
        this.configFile = configFile;
    }

    @Override
    public Void call() throws Exception {
        try {
            consumer.startup(configFile);
            consumer.handleMessages(null);
        } finally {
            consumer.shutdown();
        }
        return null;
    }
}
