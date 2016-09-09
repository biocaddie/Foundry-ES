package org.neuinfo.foundry.common.jms;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import javax.jms.*;

/**
 * Created by bozyurt on 1/25/16.
 */
public class RequestProcessor {
    protected Session session;
    protected Destination replyQueue;
    protected MessageProducer requestProducer;
    protected MessageConsumer replyConsumer;
    protected boolean verbose;
    final static Logger logger = Logger.getLogger(RequestProcessor.class);

    protected RequestProcessor(boolean verbose) {
        this.verbose = verbose;
    }

    public static RequestProcessor newRequestProcessor(Connection con, String requestQueueName, String replyQueueName,
                                                       boolean verbose) throws JMSException {
        RequestProcessor rp = new RequestProcessor(verbose);
        rp.initialize(con, requestQueueName, replyQueueName);
        return rp;
    }

    protected void initialize(Connection con, String requestQueueName, String replyQueueName) throws JMSException {
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination requestQueue = session.createQueue(requestQueueName);
        replyQueue = session.createQueue(replyQueueName);
        requestProducer = session.createProducer(requestQueue);
        replyConsumer = session.createConsumer(replyQueue);
    }

    public void send(JSONObject message) throws JMSException {
        ObjectMessage requestMessage = session.createObjectMessage(message.toString());
        requestMessage.setJMSReplyTo(replyQueue);
        requestProducer.send(requestMessage);
        if (verbose) {
            logger.info("\tTime:       " + System.currentTimeMillis() + " ms");
            logger.info("\tMessage ID: " + requestMessage.getJMSMessageID());
            logger.info("\tCorrel. ID: " + requestMessage.getJMSCorrelationID());
            logger.info("\tReply to:   " + requestMessage.getJMSReplyTo());
        }
    }

    public JSONObject receiveSync() throws JMSException {
        Message message = replyConsumer.receive(3000l); // timeout in 3 secs
        if (message instanceof ObjectMessage) {
            ObjectMessage om = (ObjectMessage) message;
            String jsonStr = (String) om.getObject();
            if (verbose) {
                logger.info("Received reply ");
                logger.info("\tTime:       " + System.currentTimeMillis() + " ms");
                logger.info("\tMessage ID: " + om.getJMSMessageID());
                logger.info("\tCorrel. ID: " + om.getJMSCorrelationID());
                logger.info("\tReply to:   " + om.getJMSReplyTo());
                logger.info("\tContents:   " + om.getObject());
            }
            return new JSONObject(jsonStr);
        }
        return null;
    }

}
