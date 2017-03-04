package org.neuinfo.foundry.jms.producer;

import com.mongodb.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.neuinfo.foundry.common.config.*;
import org.neuinfo.foundry.common.util.MongoUtils;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.jms.common.*;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 7/9/15.
 */
public class PipelineMessageConsumer implements Runnable, MessageListener {
    private Configuration configuration;
    MongoClient mongoClient;
    String queueName;
    private PipelineMessagePublisher publisher;
    private transient ConnectionFactory factory;
    private transient Connection con;
    protected transient Session session;
    protected SourceProcessStatusManager spsMan;
    volatile boolean finished = false;
    private final static Logger logger = Logger.getLogger(PipelineMessageConsumer.class);

    public PipelineMessageConsumer(Configuration configuration, String queueName) {
        this.configuration = configuration;
        this.queueName = queueName;

    }

    public void startup() throws Exception {
        List<ServerAddress> mongoServers = new ArrayList<ServerAddress>(configuration.getServers().size());
        for (ServerInfo si : configuration.getServers()) {
            mongoServers.add(new ServerAddress(si.getHost(), si.getPort()));
        }
        this.mongoClient = MongoUtils.createMongoClient(mongoServers);

        this.factory = new ActiveMQConnectionFactory(configuration.getBrokerURL());
        this.con = factory.createConnection();
        con.start();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        this.publisher = new PipelineMessagePublisher(configuration.getBrokerURL());
        // NOT USED IBO
        //this.spsMan = new SourceProcessStatusManager(this.mongoClient, configuration.getMongoDBName());
        handleMessages(this);
    }

    public void shutdown() {
        System.out.println("shutting down the PipelineMessageConsumer...");
        if (spsMan != null) {
            System.out.println("shutting down SourceProcessStatusManager...");
            spsMan.shutdown();
        }
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (publisher != null) {
            publisher.close();
        }
        System.out.println("shutdown complete.");
    }


    void handle(String objectId, String status, String collectionName4Record) {
        DB db = mongoClient.getDB(configuration.getMongoDBName());
        String theCollection = configuration.getCollectionName();
        if (!Utils.isEmpty(collectionName4Record)) {
            theCollection = collectionName4Record;
        }
        DBCollection collection = db.getCollection(theCollection);
        BasicDBObject query = new BasicDBObject(Constants.MONGODB_ID_FIELD, new ObjectId(objectId));
        DBObject theDoc = collection.findOne(query);
        if (theDoc != null) {
            Map<String, String> paramsMap = new HashMap<String, String>();
            boolean ok = false;
            // keep count of processed and erred records for pipeline monitoring
            if (spsMan != null && (status.equals("finished") || status.equals("error"))) {
                try {
                    spsMan.updateStatus(status, theDoc);
                } catch (Throwable t) {
                    // log but ignore errors
                    t.printStackTrace();
                }
            }
            if (!status.equals("finished")) {
                paramsMap.put("processing.status", status);
                for (Workflow wf : configuration.getWorkflows()) {
                    List<Route> routes = wf.getRoutes();
                    for (Route route : routes) {
                        if (route.getCondition().isSatisfied(paramsMap)) {
                            final List<QueueInfo> queueNames = route.getQueueNames();
                            for (QueueInfo qi : queueNames) {
                                try {
                                    publisher.sendMessage(objectId, theDoc, qi, status);
                                    ok = true;
                                } catch (Exception e) {
                                    //TODO proper error handling
                                    logger.error("handle", e);
                                    e.printStackTrace();
                                    ok = false;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void handleIngestionStart(String sourceID, String dataSource) {
        DB db = mongoClient.getDB(configuration.getMongoDBName());
        DBCollection collection = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject("sourceID", sourceID)
                .append("dataSource", dataSource);
        DBObject source = collection.findOne(query);
        if (source != null) {
            spsMan.resetSource(sourceID, dataSource);
        }
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


    public synchronized void setFinished(boolean finished) {
        this.finished = finished;
        notifyAll();
    }

    @Override
    public void onMessage(Message message) {
        try {
            ObjectMessage om = (ObjectMessage) message;
            String payload = (String) om.getObject();

            JSONObject json = new JSONObject(payload);
            if (json.has("ingestionStarted")) {
                String sourceID = json.getString("sourceID");
                String dataSource = json.getString("dataSource");
                handleIngestionStart(sourceID, dataSource);
                return;
            }
            String status = json.getString("status");
            String objectId = json.getString("oid");
            String collectionName = null;
            if (json.has("collectionName")) {
                collectionName = json.getString("collectionName");
            }
            if (logger.isInfoEnabled()) {
                if (collectionName == null) {
                    logger.info(String.format("status:%s objectId:%s%n", status, objectId));
                } else {
                    logger.info(String.format("status:%s objectId:%s collection:%s%n", status, objectId, collectionName));
                }
            }
            handle(objectId, status, collectionName);
        } catch (Exception x) {
            logger.error("onMessage", x);
            //TODO proper error handling
            x.printStackTrace();
        }
    }


    public void handleMessages(MessageListener listener) throws JMSException {
        Destination destination = this.session.createQueue(queueName);
        MessageConsumer messageConsumer = this.session.createConsumer(destination);
        messageConsumer.setMessageListener(listener);
    }
}
