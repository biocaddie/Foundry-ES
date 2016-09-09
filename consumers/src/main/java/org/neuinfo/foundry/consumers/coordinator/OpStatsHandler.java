package org.neuinfo.foundry.consumers.coordinator;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.json.JSONObject;
import org.neuinfo.foundry.common.Constants;
import org.neuinfo.foundry.common.model.CCOperationStats;
import org.neuinfo.foundry.common.model.ConsumerOperationStats;
import org.neuinfo.foundry.consumers.common.ConsumerProcessListener;

import javax.jms.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bozyurt on 7/20/15.
 */
public class OpStatsHandler implements Runnable, ConsumerProcessListener {
    String ccosId;
    ConcurrentHashMap<String, ConsumerOperationStats> cosMap = new ConcurrentHashMap<String, ConsumerOperationStats>();
    long pollTime = 5000l; // 5 secs
    private static OpStatsHandler instance;
    private StatsPublisher publisher;
    private volatile boolean finished = false;

    private OpStatsHandler(String brokerURL) throws JMSException {
        String hostname = null;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        StringBuilder sb = new StringBuilder(80);
        UUID uuid = UUID.randomUUID();
        if (hostname != null) {
            sb.append(hostname).append(':').append(uuid.toString());
        } else {
            sb.append(':').append(uuid.toString());
        }
        this.ccosId = sb.toString();
        this.publisher = new StatsPublisher(brokerURL, pollTime);
    }

    public synchronized static OpStatsHandler getInstance(String brokerURL) throws JMSException {
        if (instance == null) {
            instance = new OpStatsHandler(brokerURL);
        }
        return instance;
    }

    public synchronized static OpStatsHandler getInstance() {
        if (instance == null) {
            throw new RuntimeException("OpStatsHandler is not properly initialized!");
        }
        return instance;
    }

    public synchronized void shutdown() {
        System.out.println("Shutting down OpStatsHandler...");
        if (publisher != null) {
            publisher.close();
            publisher = null;
        }
    }

    synchronized void setFinished(boolean finished) {
        this.finished = finished;
        notifyAll();
    }

    @Override
    public void run() {
        while (!finished) {
            long start = System.currentTimeMillis();
            long timeLeft = pollTime;
            do {
                synchronized (this) {
                    try {
                        this.wait(timeLeft);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                timeLeft = pollTime - System.currentTimeMillis() + start;
            } while (timeLeft > 0);
            // send stats
            try {
                sendStats();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        shutdown();
    }

    private void sendStats() throws JMSException {
        if (publisher == null) {
            return;
        }
        CCOperationStats ccos = new CCOperationStats(this.ccosId);
        Date now = new Date(System.currentTimeMillis());
        synchronized (this) {
            for (ConsumerOperationStats cos : cosMap.values()) {
                ccos.addConsumerOpStats(cos);
                cos.setLastUpdated(now);
            }
        }
        publisher.sendMessage(ccos.toJSON());
    }

    @Override
    public void register(String consumerId, String consumerName) {
        String key = prepKey(consumerId, consumerName);
        cosMap.putIfAbsent(key, new ConsumerOperationStats(consumerId, consumerName));
    }


    @Override
    public void documentProcessed(String consumerId, String consumerName, String docId) {
        String key = prepKey(consumerId, consumerName);
        ConsumerOperationStats cos = cosMap.get(key);
        if (cos != null) {
            cos.incr();
        }
    }

    private static String prepKey(String consumerId, String consumerName) {
        StringBuilder sb = new StringBuilder();
        sb.append(consumerName).append(':').append(consumerId);
        return sb.toString();
    }

    public static class StatsPublisher {
        private transient Connection con;
        private transient Session session;
        private transient MessageProducer producer;

        public StatsPublisher(String brokerURL, long time2Live) throws JMSException {
            ConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
            this.con = factory.createConnection();
            session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            this.producer = session.createProducer(null);
            this.producer.setTimeToLive(time2Live);
        }

        public void sendMessage(JSONObject json) throws JMSException {
            Destination destination = session.createQueue(Constants.OPSTATS_MSG_QUEUE);
            Message message = session.createObjectMessage(json.toString());
            this.producer.send(destination, message);
        }

        public void close() {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (JMSException x) {
                x.printStackTrace();
            }
        }
    }
}
