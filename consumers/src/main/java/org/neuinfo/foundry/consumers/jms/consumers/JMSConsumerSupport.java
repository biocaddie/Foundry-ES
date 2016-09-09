package org.neuinfo.foundry.consumers.jms.consumers;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Created by bozyurt on 5/8/14.
 */
public abstract class JMSConsumerSupport extends ConsumerSupport {
    //   private static String brokerURL = "tcp://localhost:61616";
    protected transient ConnectionFactory factory;
    protected transient Connection con;


    public JMSConsumerSupport(String queueName) {
        super(queueName);
    }

    @Override
    public void startup(String configFile) throws Exception {
        super.startup(configFile);
        this.factory = new ActiveMQConnectionFactory(config.getBrokerURL());
        this.con = factory.createConnection();
        con.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        try {
            if (this.con != null) {
                con.close();
            }
        } catch (JMSException x) {
            x.printStackTrace();
        }
    }

    public void handleMessages(MessageListener listener) throws JMSException {
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(queueName);
        MessageConsumer messageConsumer = session.createConsumer(destination);
        messageConsumer.setMessageListener(listener);
    }

}
