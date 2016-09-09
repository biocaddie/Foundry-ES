package org.neuinfo.foundry.consumers.jms.consumers;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.neuinfo.foundry.consumers.coordinator.ICompositeConsumer;
import org.neuinfo.foundry.consumers.plugin.IPlugin;

import javax.jms.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by bozyurt on 12/8/15.
 */
public abstract class CompositeConsumerSupport extends ConsumerSupport implements ICompositeConsumer {
    protected List<IPlugin> plugins = new LinkedList<IPlugin>();
    protected transient ConnectionFactory factory;
    protected transient Connection con;

    public CompositeConsumerSupport(String queueName) {
        super(queueName);
    }

    @Override
    public void addPlugin(IPlugin plugin) {
        plugins.add(plugin);
    }

    @Override
    public List<IPlugin> getPlugins() {
        return plugins;
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
