package org.neuinfo.foundry.consumers.coordinator;

import javax.jms.MessageListener;
import java.util.concurrent.Callable;

/**
 * Created by bozyurt on 9/30/14.
 */
public class ConsumerWorker implements Callable<Void> {
    IConsumer consumer;
    MessageListener listener;
    String configFile;
    volatile boolean finished = false;

    public ConsumerWorker(IConsumer consumer, MessageListener listener, String configFile) {
        this.consumer = consumer;
        this.listener = listener;
        this.configFile = configFile;
    }

    @Override
    public Void call() throws Exception {
        try {
            consumer.startup(this.configFile);

            consumer.handleMessages(listener);
            while (!finished) {
                synchronized (this) {
                    try {
                        this.wait();
                    } catch (InterruptedException ie) {
                        // ignore
                    }
                }
            }
        } finally {
            consumer.shutdown();
        }
        return null;
    }

    public boolean isFinished() {
        return finished;
    }

    public synchronized void setFinished(boolean finished) {
        this.finished = finished;
        notifyAll();
    }
}
