package org.neuinfo.foundry.jms.producer;

import com.mongodb.DBObject;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.config.QueueInfo;

import javax.jms.*;

/**
 * Created by bozyurt on 7/9/15.
 */
public class PipelineMessagePublisher {
    private transient Connection con;
    private final static Logger logger = Logger.getLogger(PipelineMessagePublisher.class);

    public PipelineMessagePublisher(String brokerURL) throws JMSException {
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        this.con = factory.createConnection();
    }

    public void sendMessage(String objectId, DBObject docDBO, QueueInfo destQI, String status)
            throws JMSException, JSONException {
        Session session = null;
        try {
            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(null);
            Destination destination = session.createQueue(destQI.getName());
            JSONObject json = new JSONObject();
            json.put("oid", objectId);
            json.put("status", status);
            addAdditionalFieldsIfAny(destQI, docDBO, json);
            if (logger.isInfoEnabled()) {
                logger.info("sending JMS message with payload:" + json.toString(2) + " to destination "
                        + destQI.getName());
            }
            Message message = session.createObjectMessage(json.toString());
            producer.send(destination, message);
        } finally {
            if (session != null) {
                session.close();
            }
        }
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

    void addAdditionalFieldsIfAny(QueueInfo destQI, DBObject data, JSONObject json) {
        if (destQI.hasHeaderFields()) {
            for (String fieldName : destQI.getHeaderFieldSet()) {
                Object o = data.get(fieldName);

                if (o != null) {
                    json.put(fieldName, o);
                } else {
                    if (fieldName.indexOf('.') != -1) {
                        String[] toks = fieldName.split("\\.");
                        o = JSONUtils.findNested(data, fieldName);
                        if (o != null) {
                            json.put(toks[toks.length - 1], o);
                        }
                    }
                }
            }
        }
    }


}
