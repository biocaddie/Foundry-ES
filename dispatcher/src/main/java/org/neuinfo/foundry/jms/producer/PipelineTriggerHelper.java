package org.neuinfo.foundry.jms.producer;

import com.mongodb.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.neuinfo.foundry.common.config.*;
import org.neuinfo.foundry.common.ingestion.DocProcessingStatsService;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.util.*;
import org.neuinfo.foundry.jms.common.Constants;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.*;
import java.net.InetAddress;
import java.util.*;

/**
 * Created by bozyurt on 6/15/15.
 */
public class PipelineTriggerHelper {
    private transient Connection con;
    private String queueName;
    private Configuration config;
    private String dbName;
    private MongoClient mongoClient;
    private DocumentIngestionService docService;
    private final static Logger logger = Logger.getLogger(PipelineTriggerHelper.class);

    public PipelineTriggerHelper(String queueName) {
        this.queueName = queueName;
    }

    public void startup(String configFile) throws Exception {
        if (configFile == null) {
            this.config = ConfigLoader.load("dispatcher-cfg.xml");
        } else {
            this.config = ConfigLoader.load(configFile);
        }

        this.dbName = config.getMongoDBName();
        List<ServerAddress> servers = new ArrayList<ServerAddress>(config.getMongoServers().size());
        for (ServerInfo si : config.getMongoServers()) {
            InetAddress inetAddress = InetAddress.getByName(si.getHost());
            servers.add(new ServerAddress(inetAddress, si.getPort()));
        }

        mongoClient = MongoUtils.createMongoClient(servers);
        ConnectionFactory factory = new ActiveMQConnectionFactory(config.getBrokerURL());
        this.con = factory.createConnection();

        docService = new DocumentIngestionService();
        docService.start(this.config);
        // docService.initialize(this.dbName, mongoClient);
    }


    public void shutdown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        try {
            if (con != null) {
                logger.info("closing JMS connection");
                con.close();
            }
        } catch (JMSException x) {
            logger.error("shutdown", x);
            x.printStackTrace();
        }
    }


    public void showWS() {
        for(Workflow wf: this.config.getWorkflows()) {
            System.out.println(wf.toString());
        }
    }

    public Source findSource(String sourceID) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", sourceID);
        // FIXME finding only by NIF ID currently
        return MongoUtils.getSource(query, sources);
    }

    public List<Source> findSources() {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        List<Source> srcList = new LinkedList<Source>();
        DBCursor cursor = sources.find();
        try {
            while (cursor.hasNext()) {
                DBObject dbo = cursor.next();
                Source source = Source.fromDBObject(dbo);
                srcList.add(source);
            }

        } finally {
            cursor.close();
        }
        return srcList;
    }

    public void sendMessage(JSONObject messageBody) throws JMSException {
        sendMessage(messageBody, this.queueName);
    }

    private void sendMessage(JSONObject messageBody, String queue2Send) throws JMSException {
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

    public void index2ElasticSearchBulk(Source source, String status2Match, String indexPath,
                                        String serverURL, String apiKey) throws Exception {
        Map<String, String> docId2JsonDocStrMap = new HashMap<String, String>();
        DB db = mongoClient.getDB(dbName);
        DBCollection records = db.getCollection(this.getCollectionName());
        BasicDBObject query = new BasicDBObject("Processing.status", status2Match)
                .append("SourceInfo.SourceID", source.getResourceID());
        DBCursor cursor = records.find(query);
        int count = 0;
        try {
            while (cursor.hasNext()) {
                DBObject dbo = cursor.next();
                BasicDBObject procDBO = (BasicDBObject) dbo.get("Processing");
                ObjectId oid = (ObjectId) dbo.get(Constants.MONGODB_ID_FIELD);
                //String docId = procDBO.getString("docId");
                String docId = oid.toString();
                BasicDBObject data = (BasicDBObject) dbo.get("Data");
                BasicDBObject transformedRecDBO = (BasicDBObject) data.get("transformedRec");
                if (transformedRecDBO != null) {
                    JSONObject js = JSONUtils.toJSON(transformedRecDBO, true);
                    String jsonDocStr = js.toString();
                    count++;
                    docId2JsonDocStrMap.put(docId, jsonDocStr);
                    if ((count % 100) == 0) {
                        ElasticSearchUtils.sendBatch2ElasticSearch(docId2JsonDocStrMap, indexPath, serverURL, apiKey);
                        docId2JsonDocStrMap.clear();
                    }
                } else {
                    System.err.println("No transformedREc for docId:" + docId);
                }
            }
            if (!docId2JsonDocStrMap.isEmpty()) {
                ElasticSearchUtils.sendBatch2ElasticSearch(docId2JsonDocStrMap, indexPath, serverURL, apiKey);
            }
        } finally {
            cursor.close();
        }

    }

    public void index2ElasticSearch(Source source, String status2Match,
                                    String indexPath, String serverURL, String apiKey) throws Exception {
        DB db = mongoClient.getDB(dbName);
        DBCollection records = db.getCollection(this.getCollectionName());
        BasicDBObject query = new BasicDBObject("Processing.status", status2Match)
                .append("SourceInfo.SourceID", source.getResourceID());
        DBCursor cursor = records.find(query);
        try {
            while (cursor.hasNext()) {
                DBObject dbo = cursor.next();
                BasicDBObject procDBO = (BasicDBObject) dbo.get("Processing");
                String docId = procDBO.getString("docId");
                BasicDBObject data = (BasicDBObject) dbo.get("Data");
                BasicDBObject transformedRecDBO = (BasicDBObject) data.get("transformedRec");
                if (transformedRecDBO != null) {
                    JSONObject js = JSONUtils.toJSON(transformedRecDBO, true);
                    String jsonDocStr = js.toString();
                    ElasticSearchUtils.send2ElasticSearch(jsonDocStr, docId, indexPath, serverURL, apiKey);
                } else {
                    System.err.println("No transformedREc for docId:" + docId);
                }
            }
        } finally {
            cursor.close();
        }
    }

    public void triggerPipeline(Source source, String status2Match, String queue2Send, String newStatus,
                                String newOutStatus) throws Exception {
        DB db = mongoClient.getDB(dbName);
        DBCollection records = db.getCollection(this.getCollectionName());
        BasicDBObject query = new BasicDBObject("Processing.status", status2Match)
                .append("SourceInfo.SourceID", source.getResourceID());
        DBCursor cursor = records.find(query, new BasicDBObject("Processing.status", 1));
        if (newStatus == null) {
            try {
                while (cursor.hasNext()) {
                    DBObject dbo = cursor.next();
                    String oid = dbo.get("_id").toString();
                    JSONObject mb = prepareMessageBody(oid, status2Match, newOutStatus);
                    sendMessage(mb, queue2Send);
                }

            } finally {
                cursor.close();
            }
        } else {
            List<String> matchingOIdList = new LinkedList<String>();
            try {
                while (cursor.hasNext()) {
                    DBObject dbo = cursor.next();
                    String oid = dbo.get("_id").toString();
                    matchingOIdList.add(oid);
                }
            } finally {
                cursor.close();
            }
            logger.info("updating status of " + matchingOIdList.size() + " records to " + newStatus);
            Thread.sleep(1000);
            for (String oidStr : matchingOIdList) {
                ObjectId oid = new ObjectId(oidStr);
                BasicDBObject update = new BasicDBObject();
                update.append("$set", new BasicDBObject("Processing.status", newStatus));
                query = new BasicDBObject("_id", oid);
                records.update(query, update, false, false, WriteConcern.SAFE);
            }
            for (String oidStr : matchingOIdList) {
                JSONObject mb = prepareMessageBody(oidStr, newStatus, newOutStatus);
                sendMessage(mb, queue2Send);
            }
        }
    }

    public JSONObject prepareMessageBody(String oid, String status, String newOutStatus) {
        JSONObject json = new JSONObject();
        json.put("oid", oid);
        json.put("status", status);
        if (newOutStatus != null) {
            json.put("outStatus", newOutStatus);
        }
        return json;
    }

    public JSONObject prepareMessageBody(String cmd, Source source) throws Exception {
        return MessagingUtils.prepareMessageBody(cmd, source, this.config.getWorkflowMappings());
    }

    DocumentIngestionService getDocService() {
        return this.docService;
    }


    public List<DocProcessingStatsService.SourceStats> getProcessingStats() {
        DocProcessingStatsService dpss = new DocProcessingStatsService();
        dpss.setMongoClient(this.mongoClient);
        dpss.setDbName(this.dbName);
        return dpss.getDocCountsPerStatusPerSource2(getCollectionName());
    }

    public String getCollectionName() {
        return config.getCollectionName();
    }


}//;
