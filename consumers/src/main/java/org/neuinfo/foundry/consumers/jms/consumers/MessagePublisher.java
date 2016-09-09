package org.neuinfo.foundry.consumers.jms.consumers;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.neuinfo.foundry.common.Constants;

import javax.jms.*;
import javax.jms.Connection;
import java.sql.*;

/**
 * Created by bozyurt on 7/9/15.
 */
public class MessagePublisher {
    private transient Connection con;
    private ConnectionFactory factory;

    private final static Logger logger = Logger.getLogger(MessagePublisher.class);

    public MessagePublisher(String brokerURL) throws JMSException {
        this.factory = new ActiveMQConnectionFactory(brokerURL);
    }

    public void sendMessage(String objectId, String status) throws JMSException, JSONException {
        Session session = null;
        try {
            session = createSession();
            MessageProducer producer = session.createProducer(null);
            Destination destination = session.createQueue(Constants.PIPELINE_MSG_QUEUE);
            JSONObject json = new JSONObject();
            json.put("oid", objectId);
            json.put("status", status);
            String jsonStr = json.toString();
            Message message = session.createObjectMessage(jsonStr);
            logger.info("msg to dispatcher:" + jsonStr);
            producer.send(destination, message);
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    Session createSession() throws JMSException {
        if (this.con == null) {
            this.con = factory.createConnection();
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return session;
        } else {
            try {
                Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
                return session;
            } catch (JMSException e) {
                logger.warn("createSession", e);
                try {
                    con.close();
                } catch (Exception x) {
                    logger.warn("createSession", x);
                }
                this.con = factory.createConnection();
                Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
                return session;
            }
        }
    }


    public void close() {
        logger.info("closing MessagePublisher...");
        try {
            if (con != null) {
                con.close();
            }
        } catch (JMSException x) {
            x.printStackTrace();
        }
    }
}
