package org.neuinfo.foundry.jms.common;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.neuinfo.foundry.common.config.Configuration;
import org.neuinfo.foundry.common.jms.ReplyProcessor;

import javax.jms.Connection;
import javax.jms.JMSException;

/**
 * Created by bozyurt on 1/25/16.
 */
public class SourceProcessStatusReplier {
    private Configuration configuration;
    private Thread thread;
    private Worker worker;
    static Logger logger = Logger.getLogger(SourceProcessStatusReplier.class);

    public SourceProcessStatusReplier(Configuration configuration) {
        this.configuration = configuration;
    }

    public void startup() {
        try {
            worker = new Worker(configuration);
            thread = new Thread(worker);
            thread.setDaemon(true);
            thread.start();
        } catch (Throwable t) {
            logger.warn("Fail to start SourceProcessStatusReplier ", t);
        }
    }
    public void shutdown() {
        logger.info("shutting down SourceProcessStatusReplier...");
        try {
            worker.setFinished(true);
        } catch(Throwable t) {
            logger.error("Failed to close SourceProcessStatusReplier:" + t.getMessage());
        }
    }

    public static class Worker implements Runnable {
        private ReplyProcessor processor;
        private Connection con;
        private volatile boolean finished = false;

        public Worker(Configuration configuration) throws JMSException {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(configuration.getBrokerURL());
            this.con = factory.createConnection();
            con.start();
            this.processor = ReplyProcessor.newReplyProcessor(con, "proc.status.in", new SourceProcessStatusReplyHandler());
        }

        synchronized void setFinished(boolean finished) {
            this.finished = finished;
            notifyAll();
        }

        void shutdown() {
            if (con != null) {
                try {
                    con.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void run() {
            try {
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
                shutdown();
            }

        }
    }
}
