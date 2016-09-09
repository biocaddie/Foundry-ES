package org.neuinfo.foundry.jms.producer;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.json.JSONObject;
import org.neuinfo.foundry.common.Constants;
import org.neuinfo.foundry.common.model.CCOperationStats;
import org.neuinfo.foundry.common.model.ConsumerOperationStats;
import org.neuinfo.foundry.common.config.ConfigLoader;
import org.neuinfo.foundry.common.config.Configuration;

import javax.jms.*;

/**
 * Created by bozyurt on 7/20/15.
 */
public class FoundryTopService implements MessageListener, Runnable {
    private Configuration config;
    private transient ConnectionFactory factory;
    private transient Connection con;
    protected transient Session session;
    volatile boolean finished = false;
    private CCOperationStats currentCcos;

    public void startup(String configFile) throws Exception {
        if (configFile == null) {
            this.config = ConfigLoader.load("dispatcher-cfg.xml");
        } else {
            this.config = ConfigLoader.load(configFile);
        }
        this.factory = new ActiveMQConnectionFactory(config.getBrokerURL());
        this.con = factory.createConnection();
        con.start();
        session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        handleMessages(this);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                setFinished(true);
            }
        });
    }

    public void handleMessages(MessageListener listener) throws JMSException {
        Destination destination = this.session.createQueue(Constants.OPSTATS_MSG_QUEUE);
        MessageConsumer messageConsumer = this.session.createConsumer(destination);
        messageConsumer.setMessageListener(listener);
    }

    public synchronized void setFinished(boolean finished) {
        this.finished = finished;
        notifyAll();
    }

    void shutdown() {
        try {
            if (con != null) {
                con.close();
            }
        } catch (JMSException x) {
            x.printStackTrace();
        }
    }

    @Override
    public void onMessage(Message message) {
        try {
            ObjectMessage om = (ObjectMessage) message;
            String payload = (String) om.getObject();
            JSONObject json = new JSONObject(payload);
            CCOperationStats ccos = CCOperationStats.fromJSON(json);
            if (this.currentCcos == null) {
                showStats(ccos);
                this.currentCcos = ccos;
            } else {
                if (!this.currentCcos.getLastUpdated().equals(ccos.getLastUpdated())) {
                    showStats(ccos);
                    this.currentCcos = ccos;
                }
            }
        } catch (Exception x) {
            //TODO proper error handling
            x.printStackTrace();
        }
    }

    void showStats(CCOperationStats ccos) {
        System.out.println(ccos.getConsumerCoordinatorId() + "\t last update:" + ccos.getLastUpdated());
        for (ConsumerOperationStats cos : ccos.getCosList()) {
            System.out.println(String.format("\t%s:%s\t%d",
                    cos.getConsumerId(), cos.getConsumerName(), cos.getNumDocsProcessed()));
        }
        System.out.println();
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
