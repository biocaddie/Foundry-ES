package org.neuinfo.foundry.consumers.jms.consumers;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.ingestion.GridFSService;
import org.neuinfo.foundry.common.model.SourceProgressInfo;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.consumers.common.Constants;
import org.neuinfo.foundry.consumers.common.ConsumerProcessListener;
import org.neuinfo.foundry.consumers.common.IDGenerator;
import org.neuinfo.foundry.consumers.common.ServiceFactory;
import org.neuinfo.foundry.consumers.plugin.IPlugin;
import org.neuinfo.foundry.consumers.plugin.Pluggable;
import org.neuinfo.foundry.consumers.plugin.Result;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import java.util.Date;

/**
 * Created by bozyurt on 10/27/14.
 */
public class JavaPluginConsumer extends JMSConsumerSupport implements MessageListener, Pluggable {
    private IPlugin plugin;
    GridFSService gridFSService;
    DocumentIngestionService dis;
    MessagePublisher messagePublisher;
    private String id;
    private String name;
    private ConsumerProcessListener cpListener;
    private final static Logger logger = Logger.getLogger(JavaPluginConsumer.class);

    public JavaPluginConsumer(String queueName) {
        super(queueName);
        gridFSService = new GridFSService();
    }

    public void setCpListener(ConsumerProcessListener cpListener) {
        this.cpListener = cpListener;
    }

    @Override
    public String getId() {
        if (id == null) {
            id = String.valueOf(IDGenerator.getInstance().getNextId());
        }
        return id;
    }

    @Override
    public String getName() {
        if (name == null && plugin != null) {
            name = plugin.getPluginName();
        }
        return name;
    }

    @Override
    public void startup(String configFile) throws Exception {
        super.startup(configFile);
        gridFSService.start(this.config);
        messagePublisher = new MessagePublisher(this.config.getBrokerURL());
        this.dis = ServiceFactory.getInstance(configFile).createDocumentIngestionService();
    }

    @Override
    public void shutdown() {
        if (gridFSService != null) {
            gridFSService.shutdown();
        }
        if (messagePublisher != null) {
            messagePublisher.close();
        }
        if (dis != null) {
            dis.shutdown();
        }
        super.shutdown();
    }

    void handle(String objectId, String specifiedOutStatus, String collectionName4Record) throws Exception {
        getPlugin().setGridFSService(this.gridFSService);
        getPlugin().setDocumentIngestionService(this.dis);
        DB db = mongoClient.getDB(super.mongoDbName);
        String colName = collectionName4Record != null ? collectionName4Record : getCollectionName();
        DBCollection collection = db.getCollection(colName);
        BasicDBObject query = new BasicDBObject(Constants.MONGODB_ID_FIELD, new ObjectId(objectId));
        DBObject theDoc = collection.findOne(query);
        if (theDoc != null) {
            DBObject pi = (DBObject) theDoc.get("Processing");
            if (pi != null) {
                if (logger.isInfoEnabled()) {
                    logger.info("pi:" + pi);
                }
                String status = (String) pi.get("status");
                BasicDBObject origDoc = (BasicDBObject) theDoc.get("OriginalDoc");
                if (origDoc != null && status != null && status.equals(getInStatus())) {
                    BasicDBObject sourceInfo = (BasicDBObject) theDoc.get("SourceInfo");
                    String sourceID = sourceInfo.getString("SourceID");
                    String dataSource = sourceInfo.getString("DataSource");
                    try {
                        logger.info("using plugin:" + getPlugin().getPluginName());
                        Result result = getPlugin().handle(theDoc);
                        String outStatus = specifiedOutStatus == null ? getOutStatus() : specifiedOutStatus;
                        if (result.getStatus() == Result.Status.OK_WITH_CHANGE) {
                            //String s = theDoc.toString();
                            //logger.info("after 1:" + s.length());
                            theDoc = result.getDocWrapper();
                            //s = theDoc.toString();
                            // logger.info("after 2:" + s.length());
                            pi = (DBObject) theDoc.get("Processing");
                            pi.put("status", outStatus);
                            logger.info("updating document");
                            if (logger.isDebugEnabled()) {
                                logger.info(theDoc.toString());
                            }
                            // System.out.println(JSONUtils.toJSON((BasicDBObject) theDoc,true).toString(2));
                            collection.update(query, theDoc);
                            logger.info("updated document");
                            updateProgressRecord(sourceID, dataSource, outStatus, db);
                            messagePublisher.sendMessage(objectId, outStatus, colName);
                        } else if (result.getStatus() == Result.Status.OK_WITHOUT_CHANGE) {
                            pi.put("status", outStatus);
                            logger.info("updating status to " + outStatus);
                            collection.update(query, theDoc);
                            updateProgressRecord(sourceID, dataSource, outStatus, db);
                            messagePublisher.sendMessage(objectId, outStatus, colName);
                        } else {
                            pi.put("status", "error");
                            logger.info("updating status to error");
                            collection.update(query, theDoc);
                            updateProgressRecord(sourceID, dataSource, "error", db);
                            messagePublisher.sendMessage(objectId, "error", colName);
                        }
                    } catch (Throwable t) {
                        logger.error("handle", t);
                        t.printStackTrace();
                        if (pi != null) {
                            pi.put("status", "error");
                            logger.info("updating");
                            collection.update(query, theDoc);
                            updateProgressRecord(sourceID, dataSource, "error", db);
                            messagePublisher.sendMessage(objectId, "error", colName);
                        }
                    }
                }
            }
        } else {
            logger.warn("Cannot find object with id:" + objectId);
        }
    }

    public void updateProgressRecord(String sourceID, String dataSource, String outStatus, DB db) {
        DBCollection collection = db.getCollection(org.neuinfo.foundry.common.Constants.SOURCE_PROG_COLLECTION);
        BasicDBObject query = new BasicDBObject("sourceID", sourceID)
                .append("dataSource", dataSource);
        BasicDBObject existingDBO = (BasicDBObject) collection.findOne(query);
        if (existingDBO != null) {
            BasicDBObject query2 = new BasicDBObject("_id", existingDBO.getObjectId("_id"));
            String fieldName = null;
            if (outStatus.indexOf('.') != -1) {
                fieldName = outStatus.replaceAll("\\.", "_") + "_Count";
            } else if (outStatus.equals("finished")) {
                fieldName = "finishedCount";
            } else if (outStatus.equals("error")) {
                fieldName = "errorCount";
            } else {
                throw new RuntimeException("Unknown status type:" + outStatus);
            }
            Object ingestionEndDate = existingDBO.get("ingestionEndDate");
            boolean finished = false;
            if (ingestionEndDate != null) {
                int newCount = existingDBO.getInt("newCount");
                int updatedCount = existingDBO.getInt("updatedCount");
                int finishedCount = existingDBO.getInt("finishedCount");
                int errorCount = existingDBO.getInt("errorCount");
                int totalCount = finishedCount + errorCount + 1;
                if (totalCount >= (newCount + updatedCount)) {
                    finished = true;
                }
            }

            BasicDBObject incValue = new BasicDBObject(fieldName, 1);
            if (!finished) {
                collection.update(query2, new BasicDBObject("$inc", incValue));
            } else {
                BasicDBObject setValues = new BasicDBObject("processingStatus", SourceProgressInfo.FINISHED)
                        .append("endDate", SourceProgressInfo.formatDate(new Date()));
                collection.update(query2, new BasicDBObject("$inc", incValue).append("$set", setValues));
            }
        }
    }

    @Override
    public void onMessage(Message message) {
        try {
            ObjectMessage om = (ObjectMessage) message;
            String payload = (String) om.getObject();
            if (logger.isInfoEnabled()) {
                logger.info("payload:" + payload);
            }
            JSONObject json = new JSONObject(payload);
            String status = json.getString("status");
            String objectId = json.getString("oid");
            String outStatus = null;
            if (json.has("outStatus")) {
                outStatus = json.getString("outStatus");
            }
            String collectionName4Record = null;
            if (json.has("collectionName")) {
                collectionName4Record = json.getString("collectionName");
            }
            if (logger.isInfoEnabled()) {
                if (outStatus != null) {
                    logger.info(String.format("status:%s objectId:%s outStatus:%s%n", status, objectId, outStatus));
                } else {
                    logger.info(String.format("status:%s objectId:%s%n", status, objectId));
                }
            }
            handle(objectId, outStatus, collectionName4Record);
        } catch (Exception x) {
            logger.error("onMessage", x);
            //TODO proper error handling
            x.printStackTrace();
        }
    }

    @Override
    public void setPlugin(IPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public IPlugin getPlugin() {
        return this.plugin;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(plugin.getPluginName());
        return sb.toString();
    }
}
