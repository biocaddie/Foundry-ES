package org.neuinfo.foundry.common.jms;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import javax.jms.*;

/**
 * Created by bozyurt on 1/25/16.
 */
public class ReplyProcessor implements MessageListener {
    protected Session session;
    protected IReplyHandler handler;
    private final static Logger logger = Logger.getLogger(ReplyProcessor.class);

    protected ReplyProcessor(IReplyHandler handler) {
        this.handler = handler;
    }

    public static ReplyProcessor newReplyProcessor(Connection con, String requestQueueName, IReplyHandler handler) throws JMSException {
        ReplyProcessor rp = new ReplyProcessor(handler);
        rp.initialize(con, requestQueueName);
        return rp;
    }

    protected void initialize(Connection con, String requestQueueName) throws JMSException {
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination requestQueue = session.createQueue(requestQueueName);
        MessageConsumer requestConsumer = session.createConsumer(requestQueue);
        requestConsumer.setMessageListener(this);
    }

    @Override
    public void onMessage(Message message) {
        try {
            if ((message instanceof ObjectMessage) && message.getJMSReplyTo() != null) {
                ObjectMessage om = (ObjectMessage) message;
                String jsonStr = (String) om.getObject();
                JSONObject requestJSON = new JSONObject(jsonStr);
                JSONObject responseJSON = handler.handle(requestJSON);
                Destination replyQueue = om.getJMSReplyTo();
                MessageProducer replyProducer = session.createProducer(replyQueue);
                ObjectMessage responseMsg = session.createObjectMessage(responseJSON);
                responseMsg.setJMSCorrelationID(om.getJMSMessageID());
                replyProducer.send(responseMsg);
            }

        } catch (JMSException e) {
            logger.error(e);
        }
    }
}
