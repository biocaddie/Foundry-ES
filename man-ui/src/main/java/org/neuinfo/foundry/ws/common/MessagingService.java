package org.neuinfo.foundry.ws.common;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.config.ConfigLoader;
import org.neuinfo.foundry.common.config.Configuration;
import org.neuinfo.foundry.common.jms.RequestProcessor;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.model.SourceProcessStatusInfo;

import javax.jms.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by bozyurt on 1/25/16.
 */
public class MessagingService {
    private Configuration config;
    private Connection con = null;
    private static MessagingService instance = null;
    private final static Logger logger = Logger.getLogger(MessagingService.class);

    private MessagingService() throws Exception {
        this.config = ConfigLoader.load("man-ui-cfg.xml");
        ConnectionFactory factory = new ActiveMQConnectionFactory(config.getBrokerURL());
        con = factory.createConnection();
    }


    public synchronized static MessagingService getInstance() throws Exception {
        if (instance == null) {
            instance = new MessagingService();
        }
        return instance;
    }

    public void shutdown() {
        if (con != null) {
            try {
                con.close();
            } catch (JMSException e) {
                logger.error(e);
            }
        }
    }

    public List<SourceProcessStatusInfo> getCurrentProcessStatusList4AllSources() {
        try {
            RequestProcessor requestProcessor = RequestProcessor.newRequestProcessor(con, "proc.status.in", "proc.status.out", true);
            JSONObject requestJSON = new JSONObject();
            requestJSON.put("command", "getAllSPSIs");
            requestProcessor.send(requestJSON);
            JSONObject resultJSON = requestProcessor.receiveSync();
            if (resultJSON != null && resultJSON.getString("status").equals("ok")) {
                JSONArray results = resultJSON.getJSONArray("results");
                List<SourceProcessStatusInfo> spsiList = new ArrayList<SourceProcessStatusInfo>(results.length());
                for (int i = 0; i < results.length(); i++) {
                    JSONObject js = results.getJSONObject(i);
                    spsiList.add(SourceProcessStatusInfo.fromJSON(js));
                }
                return spsiList;
            }
        } catch (Throwable t) {
            logger.error(t);
        }
        return Collections.EMPTY_LIST;
    }

    public void sendIngestMessage(Source source) {
        // TODO
    }
    /**
     * @param messageBody the payload to send
     * @param queue2Send  e.g. foundry.consumer.head
     * @throws JMSException
     */
    public void sendMessage(JSONObject messageBody, String queue2Send) throws JMSException {
        Session session = null;
        try {
            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(null);
            Destination destination = session.createQueue(queue2Send);
            if (logger.isInfoEnabled()) {
                logger.info("sending user JMS message with payload:" + messageBody.toString(2) +
                        " to queue:" + queue2Send);
            }
            Message message = session.createObjectMessage(messageBody.toString());
            producer.send(destination, message);
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
}
