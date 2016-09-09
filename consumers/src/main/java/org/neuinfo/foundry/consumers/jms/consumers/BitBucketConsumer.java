package org.neuinfo.foundry.consumers.jms.consumers;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;

/**
 * Consumes messages from a given queue useful for development/testing
 * Created by bozyurt on 5/28/14.
 */
public class BitBucketConsumer extends JMSConsumerSupport implements MessageListener {
    public BitBucketConsumer(String queueName) {
        super(queueName);
    }

    @Override
    public void onMessage(Message message) {
         try {
             ObjectMessage om = (ObjectMessage) message;
             String payload = (String) om.getObject();
             System.out.println("consumed message with payload:" + payload);
         } catch(Exception x) {
             x.printStackTrace();
         }
        // no op
    }

    public static void usage() {
        System.out.println("BitBucketConsumer <queue.name>");
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            usage();
        }
        String queueName = args[0];
        String configFile = "consumers-cfg.xml";
        BitBucketConsumer consumer = new BitBucketConsumer(queueName);
        try {
            consumer.startup(configFile);
            consumer.handleMessages(consumer);

            System.out.print("Press a key to exit:");
            System.in.read();

        } finally {
            consumer.shutdown();
        }
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }
}
